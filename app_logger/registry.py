import asyncio
from typing import Any, Dict, List, Optional

from app_logger.interfaces import AsyncAbstractLogger
from app_logger.log_level import LogLevel


class AsyncLoggerRegistry:
    """异步日志注册中心，管理多个日志记录器并统一分发日志消息"""

    def __init__(self, default_level: LogLevel = LogLevel.INFO):
        """
        :param default_level: 注册中心默认日志等级，新注册的 logger 不指定等级时使用
        """
        self._loggers: Dict[str, AsyncAbstractLogger] = {}
        self._default_level: LogLevel = default_level

    def register(self, logger: AsyncAbstractLogger) -> None:
        """注册一个日志记录器，名称重复时抛出 ValueError"""
        if logger.logger_name in self._loggers:
            raise ValueError(f"logger '{logger.logger_name}' already registered")
        self._loggers[logger.logger_name] = logger

    def unregister(self, logger_name: str) -> None:
        """注销指定名称的日志记录器，不存在时抛出 KeyError"""
        if logger_name not in self._loggers:
            raise KeyError(f"logger '{logger_name}' not found")
        del self._loggers[logger_name]

    def get(self, logger_name: str) -> Optional[AsyncAbstractLogger]:
        """按名称获取日志记录器，不存在返回 None"""
        return self._loggers.get(logger_name)

    def get_all(self) -> List[AsyncAbstractLogger]:
        """获取所有已注册的日志记录器"""
        return list(self._loggers.values())

    @property
    def default_level(self) -> LogLevel:
        """注册中心默认日志等级"""
        return self._default_level

    @default_level.setter
    def default_level(self, value: LogLevel) -> None:
        """设置注册中心默认日志等级"""
        self._default_level = value

    def set_level(self, level: LogLevel, logger_name: Optional[str] = None) -> None:
        """
        设置日志等级
        :param level: 目标日志等级
        :param logger_name: 指定 logger 名称则仅修改该 logger，为 None 则全局修改
        """
        if logger_name is not None:
            logger = self._loggers.get(logger_name)
            if logger is None:
                raise KeyError(f"logger '{logger_name}' not found")
            logger.level = level
        else:
            self._default_level = level
            for logger in self._loggers.values():
                logger.level = level

    async def _dispatch(self, log_method_name: str, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """将日志消息并发分发到所有已注册的 logger"""
        tasks = []
        for logger in self._loggers.values():
            method = getattr(logger, log_method_name, None)
            if method is not None:
                tasks.append(method(message, file_name, method_name, *args, **kwargs))
        if tasks:
            await asyncio.gather(*tasks)

    async def debug(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """向所有 logger 分发 DEBUG 等级日志"""
        await self._dispatch("debug", message, file_name, method_name, *args, **kwargs)

    async def info(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """向所有 logger 分发 INFO 等级日志"""
        await self._dispatch("info", message, file_name, method_name, *args, **kwargs)

    async def warning(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """向所有 logger 分发 WARNING 等级日志"""
        await self._dispatch("warning", message, file_name, method_name, *args, **kwargs)

    async def error(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """向所有 logger 分发 ERROR 等级日志"""
        await self._dispatch("error", message, file_name, method_name, *args, **kwargs)

    async def critical(self, message: str, file_name: str = "", method_name: str = "", *args: Any, **kwargs: Any) -> None:
        """向所有 logger 分发 CRITICAL 等级日志"""
        await self._dispatch("critical", message, file_name, method_name, *args, **kwargs)
