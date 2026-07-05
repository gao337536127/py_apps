from abc import ABC, abstractmethod
from typing import Any

from app_logger.log_level import LogLevel


class AsyncAbstractLogger(ABC):
    """异步日志记录器抽象基类，定义日志记录的通用接口和等级过滤逻辑"""

    def __init__(self, logger_name: str, level: LogLevel = LogLevel.INFO):
        """
        :param logger_name: 日志记录器名称，用于标识不同日志来源
        :param level: 最低日志等级，低于此等级的日志将被忽略
        """
        self._logger_name = logger_name
        self._level = level

    @property
    def logger_name(self) -> str:
        """日志记录器名称"""
        return self._logger_name

    @property
    def level(self) -> LogLevel:
        """当前最低日志等级"""
        return self._level

    @level.setter
    def level(self, value: LogLevel) -> None:
        """设置最低日志等级"""
        self._level = value

    def _should_log(self, message_level: LogLevel) -> bool:
        """判断消息等级是否达到记录阈值"""
        return message_level >= self._level

    @abstractmethod
    async def _log(self, level: LogLevel, message: str, file_name: str, method_name: str, *args: Any, **kwargs: Any) -> None:
        """子类实现的实际日志写入逻辑"""
        ...

    async def debug(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """记录 DEBUG 等级日志"""
        if self._should_log(LogLevel.DEBUG):
            await self._log(LogLevel.DEBUG, message, file_name, method_name, *args, **kwargs)

    async def info(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """记录 INFO 等级日志"""
        if self._should_log(LogLevel.INFO):
            await self._log(LogLevel.INFO, message, file_name, method_name, *args, **kwargs)

    async def warning(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """记录 WARNING 等级日志"""
        if self._should_log(LogLevel.WARNING):
            await self._log(LogLevel.WARNING, message, file_name, method_name, *args, **kwargs)

    async def error(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """记录 ERROR 等级日志"""
        if self._should_log(LogLevel.ERROR):
            await self._log(LogLevel.ERROR, message, file_name, method_name, *args, **kwargs)

    async def critical(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """记录 CRITICAL 等级日志"""
        if self._should_log(LogLevel.CRITICAL):
            await self._log(LogLevel.CRITICAL, message, file_name, method_name, *args, **kwargs)
