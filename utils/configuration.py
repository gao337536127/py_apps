from psutil import cpu_count

MAX_LOG_RECORD_WORKS = (cpu_count() or 1) * 2
