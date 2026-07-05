import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import IO, Any, Optional

from app_logger._executor import _internal_log_record_executor
from app_logger.interfaces import AsyncAbstractLogger
from app_logger.log_level import LogLevel

_CST = timezone(timedelta(hours=8))            # 中国标准时区 UTC+8
_DEFAULT_MAX_BYTES = 50 * 1024 * 1024          # 默认单文件最大 50MB
_DEFAULT_QUEUE_MAX_SIZE = 10000                 # 默认队列最大容量


class AsyncFileLogger(AsyncAbstractLogger):
    """
    异步文件日志记录器，基于队列缓冲实现，支持按日期和文件大小自动切割

    日志写入流程：调用方 -> asyncio.Queue -> 后台消费者协程 -> 线程池 -> 文件IO
    调用方仅将格式化后的消息放入队列即返回，文件IO由消费者协程通过线程池异步完成
    """

    def __init__(
        self,
        logger_name: str,
        file_path: str | Path,
        executor: ThreadPoolExecutor = _internal_log_record_executor,
        level: LogLevel = LogLevel.INFO,
        encoding: str = "utf-8",
        max_bytes: int = _DEFAULT_MAX_BYTES,
        queue_max_size: int = _DEFAULT_QUEUE_MAX_SIZE,
        queue_blocking: bool = False,
    ):
        """
        :param logger_name: 日志记录器名称
        :param file_path: 日志文件路径
        :param executor: 外部共享线程池，所有 AsyncFileLogger 实例共用
        :param level: 最低日志等级
        :param encoding: 文件编码
        :param max_bytes: 单个日志文件最大字节数，超过后自动切割
        :param queue_max_size: 日志队列最大容量
        :param queue_blocking: 队列满时是否阻塞等待，False 则丢弃新消息
        """
        super().__init__(logger_name, level)
        self._file_path = Path(file_path)
        self._encoding = encoding
        self._max_bytes = max_bytes
        self._file_handle: Optional[IO[str]] = None
        self._current_date: str = ""            # 当前写入文件对应的日期，用于日期切割判断
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_max_size)
        self._queue_blocking = queue_blocking   # 队列满时是否阻塞等待
        self._consumer_task: Optional[asyncio.Task] = None
        self._closed = False                    # 标记 logger 是否已关闭
        self._executor = executor               # 外部共享线程池
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def start(self) -> None:
        """启动后台消费者协程，缓存事件循环引用，必须在日志写入前调用"""
        self._loop = asyncio.get_running_loop()
        if self._consumer_task is None:
            self._consumer_task = asyncio.create_task(self._consume_loop())

    async def _run_in_executor(self, func, *args):
        """通过外部共享线程池执行同步函数"""
        loop = self._loop
        assert loop is not None, "logger 未启动，请先调用 start()"
        return await loop.run_in_executor(self._executor, func, *args)

    async def _consume_loop(self) -> None:
        """后台消费者循环，从队列取出消息并写入文件"""
        while not self._closed or not self._queue.empty():
            try:
                formatted = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            await self._write_to_file(formatted)
            self._queue.task_done()

    async def _write_to_file(self, formatted: str) -> None:
        """将单条格式化消息写入文件，包含切割检查和文件IO"""
        await self._check_and_rotate()
        if self._file_handle is None:
            self._file_handle = await self._run_in_executor(
                partial(open, self._file_path, "a", encoding=self._encoding)
            )
        fh = self._file_handle
        assert fh is not None
        await self._run_in_executor(fh.write, formatted)
        await self._run_in_executor(fh.flush)

    def _date_str(self) -> str:
        """获取当前 CST 日期字符串，格式 YYYY-MM-DD"""
        return datetime.now(_CST).strftime("%Y-%m-%d")

    def _rotated_file_path(self, index: int) -> Path:
        """
        生成切割后的归档文件路径
        格式：{stem}_{日期}_{序号}{suffix}，例如 app_2026-07-04_1.log
        """
        stem = self._file_path.stem
        suffix = self._file_path.suffix
        parent = self._file_path.parent
        return parent / f"{stem}_{self._current_date}_{index}{suffix}"

    async def _close_handle(self) -> None:
        """关闭当前文件句柄"""
        if self._file_handle is not None:
            await self._run_in_executor(self._file_handle.close)
            self._file_handle = None

    async def _rotate(self) -> None:
        """
        执行日志切割：关闭当前文件，将其重命名为归档文件名
        归档序号自动递增，避免覆盖已有归档
        """
        await self._close_handle()
        index = 1
        while self._rotated_file_path(index).exists():
            index += 1
        rotated_path = self._rotated_file_path(index)
        await self._run_in_executor(self._file_path.rename, rotated_path)

    async def _check_and_rotate(self) -> None:
        """
        检查并执行日志切割
        条件一：当前日期与上次写入日期不同（日期切割）
        条件二：当前文件大小超过 max_bytes（大小切割）
        """
        today = self._date_str()
        if self._current_date != today:
            await self._close_handle()
            self._current_date = today
        if self._file_path.exists():
            file_size = await self._run_in_executor(self._file_path.stat)
            if file_size.st_size >= self._max_bytes:
                await self._rotate()

    async def close(self) -> None:
        """
        关闭日志记录器：等待队列中剩余消息消费完毕，停止消费者协程，关闭文件句柄
        注意：不会关闭外部共享线程池，由调用方统一管理生命周期
        """
        self._closed = True
        if self._consumer_task is not None:
            await self._consumer_task
            self._consumer_task = None
        await self._close_handle()

    def _format_message(self, level: LogLevel, message: str, file_name: str, method_name: str) -> str:
        """
        格式化日志消息
        格式：等级 CST时间(毫秒) 毫秒级unix时间戳 文件名:方法名 日志文本
        示例：INFO 2026-07-04 18:31:04.643 1783161064643 dao.py:query error direct
        """
        now = datetime.now(_CST)
        cst_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        unix_ms = int(now.timestamp() * 1000)
        source = f"{file_name}:{method_name}" if file_name and method_name else file_name or method_name
        return f"{level.name} {cst_time} {unix_ms} {source} {message}\n"

    async def _log(self, level: LogLevel, message: str, file_name: str, method_name: str, *args: Any, **kwargs: Any) -> None:
        """将格式化后的日志消息放入队列，由后台消费者异步写入文件"""
        formatted = self._format_message(level, message, file_name, method_name)
        if self._queue_blocking:
            await self._queue.put(formatted)
        else:
            try:
                self._queue.put_nowait(formatted)
            except asyncio.QueueFull:
                pass
