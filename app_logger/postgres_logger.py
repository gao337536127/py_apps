import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import asyncpg

from app_logger.interfaces import AsyncAbstractLogger
from app_logger.log_level import LogLevel

_CST = timezone(timedelta(hours=8))            # 中国标准时区 UTC+8
_DEFAULT_QUEUE_MAX_SIZE = 10000                 # 默认队列最大容量

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS log_record (
    id          SERIAL PRIMARY KEY,
    sys_name    TEXT    NOT NULL,
    level       TEXT    NOT NULL,
    cst_time    TEXT    NOT NULL,
    unix_ms    BIGINT  NOT NULL,
    file_name   TEXT    NOT NULL DEFAULT '',
    method_name TEXT    NOT NULL DEFAULT '',
    message     TEXT    NOT NULL
)
"""

_CREATE_INDEX_SQLS = [
    "CREATE INDEX IF NOT EXISTS idx_log_record_sys_name ON log_record (sys_name)",
    "CREATE INDEX IF NOT EXISTS idx_log_record_level ON log_record (level)",
    "CREATE INDEX IF NOT EXISTS idx_log_record_cst_time ON log_record (cst_time)",
    "CREATE INDEX IF NOT EXISTS idx_log_record_unix_ms ON log_record (unix_ms)",
]

_INSERT_SQL = """
INSERT INTO log_record (sys_name, level, cst_time, unix_ms, file_name, method_name, message)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"""


class AsyncPostgresLogger(AsyncAbstractLogger):
    """
    异步 PostgreSQL 日志记录器，基于 asyncpg 和队列缓冲实现

    日志写入流程：调用方 -> asyncio.Queue -> 后台消费者协程 -> asyncpg -> PostgreSQL IO
    调用方仅将日志记录放入队列即返回，PostgreSQL IO 由消费者协程通过 asyncpg 异步完成
    """

    def __init__(
        self,
        logger_name: str,
        dsn: str,
        sys_name: str,
        level: LogLevel = LogLevel.INFO,
        queue_max_size: int = _DEFAULT_QUEUE_MAX_SIZE,
        queue_blocking: bool = False,
    ):
        """
        :param logger_name: 日志记录器名称
        :param dsn: PostgreSQL 连接字符串，例如 postgres://user:pass@host:port/dbname
        :param sys_name: 系统名称，标识日志来源系统
        :param level: 最低日志等级
        :param queue_max_size: 日志队列最大容量
        :param queue_blocking: 队列满时是否阻塞等待，False 则丢弃新消息
        """
        super().__init__(logger_name, level)
        self._dsn = dsn
        self._sys_name = sys_name
        self._conn: Optional[asyncpg.Connection] = None
        self._queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=queue_max_size)
        self._queue_blocking = queue_blocking   # 队列满时是否阻塞等待
        self._consumer_task: Optional[asyncio.Task] = None
        self._closed = False                    # 标记 logger 是否已关闭

    async def start(self) -> None:
        """初始化数据库连接和表结构，启动后台消费者协程，必须在日志写入前调用"""
        self._conn = await asyncpg.connect(self._dsn)
        conn = self._conn
        assert conn is not None
        await conn.execute(_CREATE_TABLE_SQL)
        for index_sql in _CREATE_INDEX_SQLS:
            await conn.execute(index_sql)
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
        await conn.execute(_INSERT_SQL, *record)

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
        字段：sys_name, level, cst_time, unix_ms, file_name, method_name, message
        """
        now = datetime.now(_CST)
        cst_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        unix_ms = int(now.timestamp() * 1000)
        return (self._sys_name, level.name, cst_time, unix_ms, file_name, method_name, message)

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
