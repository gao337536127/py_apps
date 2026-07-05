from concurrent.futures import ThreadPoolExecutor

from psutil import cpu_count

from utils.configuration import MAX_LOG_RECORD_WORKS

_internal_log_record_executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=MAX_LOG_RECORD_WORKS)
