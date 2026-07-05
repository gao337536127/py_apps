import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import aiosqlite

from app_logger.interfaces import AsyncAbstractLogger
from app_logger.log_level import LogLevel

_CST = timezone(timedelta(hours=8))            # 中国标准时区 UTC+8
_DEFAULT_QUEUE_MAX_SIZE = 10000                 # 默认队列最大容量

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS log_record (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    level       TEXT    NOT NULL,
    cst_time    TEXT    NOT NULL,
    unix_ms    INTEGER NOT NULL,
    file_name   TEXT    NOT NULL DEFAULT '',
    method_name TEXT    NOT NULL DEFAULT '',
    message     TEXT    NOT NULL
)
"""

_INSERT_SQL = """
INSERT INTO log_record (level, cst_time, unix_ms, file_name, method_name, message)
VALUES (?, ?, ?, ?, ?, ?)
"""


class AsyncSqliteLogger(AsyncAbstractLogger):
    """
    异步 SQLite 日志记录器，基于 aiosqlite 和队列缓冲实现

    日志写入流程：调用方 -> asyncio.Queue -> 后台消费者协程 -> aiosqlite -> SQLite IO
    调用方仅将日志记录放入队列即返回，SQLite IO 由消费者协程通过 aiosqlite 异步完成
    """

    def __init__(
        self,
        logger_name: str,
        db_path: str | Path,
        level: LogLevel = LogLevel.INFO,
        queue_max_size: int = _DEFAULT_QUEUE_MAX_SIZE,
        queue_blocking: bool = False,
    ):
        """
        :param logger_name: 日志记录器名称
        :param db_path: SQLite 数据库文件路径
        :param level: 最低日志等级
        :param queue_max_size: 日志队列最大容量
        :param queue_blocking: 队列满时是否阻塞等待，False 则丢弃新消息
        """
        super().__init__(logger_name, level)
        self._db_path = Path(db_path)
        self._conn: Optional[aiosqlite.Connection] = None
        self._queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=queue_max_size)
        self._queue_blocking = queue_blocking   # 队列满时是否阻塞等待
        self._consumer_task: Optional[asyncio.Task] = None
        self._closed = False                    # 标记 logger 是否已关闭

    async def start(self) -> None:
        """初始化数据库连接和表结构，启动后台消费者协程，必须在日志写入前调用"""
        self._conn = await aiosqlite.connect(str(self._db_path))
        conn = self._conn
        assert conn is not None
        await conn.execute(_CREATE_TABLE_SQL)
        await conn.commit()
        if self._consumer_task is None:
            self._consumer_task = asyncio.create_task(self._consume_loop())

    async def _consume_loop(self) -> None:
        """后台消费者循环，从队列取出日志记录并写入数据库"""
        while not self._closed or not self._queue.empty():
            try:
                record = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            await self._write_to_db(record)
            self._queue.task_done()

    async def _write_to_db(self, record: tuple) -> None:
        """将单条日志记录写入数据库"""
        conn = self._conn
        assert conn is not None
        await conn.execute(_INSERT_SQL, record)
        await conn.commit()

    async def close(self) -> None:
        """
        关闭日志记录器：等待队列中剩余消息消费完毕，停止消费者协程，关闭数据库连接
        """
        self._closed = True
        if self._consumer_task is not None:
            await self._consumer_task
            self._consumer_task = None
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _build_record(self, level: LogLevel, message: str, file_name: str, method_name: str) -> tuple:
        """
        构建日志记录元组
        字段：level, cst_time, unix_ms, file_name, method_name, message
        """
        now = datetime.now(_CST)
        cst_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        unix_ms = int(now.timestamp() * 1000)
        return (level.name, cst_time, unix_ms, file_name, method_name, message)

    async def _log(self, level: LogLevel, message: str, file_name: str, method_name: str, *args: Any, **kwargs: Any) -> None:
        """将日志记录放入队列，由后台消费者异步写入数据库"""
        record = self._build_record(level, message, file_name, method_name)
        if self._queue_blocking:
            await self._queue.put(record)
        else:
            try:
                self._queue.put_nowait(record)
            except asyncio.QueueFull:
                pass
