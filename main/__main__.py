import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import numpy as np
from psutil import cpu_count

thread_executor: (ThreadPoolExecutor | None) = None
process_executor: (ProcessPoolExecutor | None) = None


def _compute_matrix_product(matrix_a: np.ndarray) -> np.ndarray:
    return matrix_a @ matrix_a.T


async def big_matrix_compute_test(row: int, col: int) -> np.ndarray:
    loop = asyncio.get_running_loop()
    matrix_a = np.random.rand(row, col)
    result = await loop.run_in_executor(thread_executor, _compute_matrix_product, matrix_a)
    return result


async def main():
    print('this is test'.upper())
    result = await big_matrix_compute_test(10000, 10000)
    print(result)


if __name__ == "__main__":
    thread_executor = ThreadPoolExecutor(max_workers=10)
    process_executor = ProcessPoolExecutor(max_workers=(cpu_count() or 1) * 2)
    asyncio.run(main())
