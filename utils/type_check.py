import functools
import inspect


def type_check(func):
    """
    装饰器：在函数执行前检查传入参数的类型是否与函数注解一致。

    Args:
        func (Callable): 被装饰的目标函数。

    Returns:
        Callable: 包装后的函数，在调用时会进行参数类型校验。

    Raises:
        TypeError: 如果传入参数的实际类型与期望的注解类型不匹配。
    """
    # 获取被装饰函数的签名信息
    sig = inspect.signature(func)
    # 获取函数的类型注解字典
    annotations = func.__annotations__

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # 将传入的位置参数和关键字参数绑定到函数签名的对应参数上
        bound = sig.bind(*args, **kwargs)
        # 应用参数的默认值，确保未显式传参的参数也能被检查
        bound.apply_defaults()
        # 遍历所有已绑定的参数名和参数值
        for name, value in bound.arguments.items():
            # 如果该参数存在类型注解
            if name in annotations:
                # 获取期望的参数类型
                expected = annotations[name]
                # 检查实际传入的值是否为期望类型的实例
                if not isinstance(value, expected):
                    # 若类型不匹配，抛出 TypeError 异常
                    raise TypeError(
                        f"参数 '{name}' 期望类型 {expected.__name__}，实际类型 {type(value).__name__}"
                    )
        # 类型检查通过，执行原函数并返回结果
        return func(*args, **kwargs)

    # 返回包装后的函数
    return wrapper
