from enum import IntEnum


class LogLevel(IntEnum):
    """日志等级枚举，数值越大等级越高"""

    DEBUG = 10      # 调试信息，仅开发阶段使用
    INFO = 20       # 常规运行信息
    WARNING = 30    # 警告信息，不影响运行但需关注
    ERROR = 40      # 错误信息，功能异常但程序仍可运行
    CRITICAL = 50   # 严重错误，可能导致程序崩溃
