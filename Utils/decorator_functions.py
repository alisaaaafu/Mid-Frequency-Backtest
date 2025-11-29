# encoding=utf-8
"""
Author: Wamnzhen Fu
Date: 7-8-2020
修改: 移除线程实现，改为单线程模式
"""
# 不再使用线程
# from threading import Thread

def thread(func):
    """
    为了保持兼容性保留的装饰器，但不再启动线程
    在单线程模式下，直接调用被装饰的函数
    """
    def wrapper(*args):
        # 不再启动线程，直接调用函数
        return func(*args)
    return wrapper
