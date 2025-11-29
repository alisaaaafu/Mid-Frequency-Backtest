# encoding: utf-8
"""
class Engine, multithreading, queue
Author: Wamnzhen Fu
Date: 7-6-2020
修改: 单线程模式，移除所有线程实现
"""
import os
import time
import datetime
# 不再使用线程
# from threading import Thread
from queue import Queue, Empty, PriorityQueue
from Utils.Event import TIMER_EVENT, Event_Type
# import redis
import json
from Utils.decorator_functions import thread
import re
import itertools  # 添加itertools用于生成序列号

# 全局回测当前时间变量，字符串格式
CURRENT_BACKTEST_TIME = ""

class Event_Engine(object):
    """
    事件引擎,对于每种type的event注册函数
    单线程模式：不再使用线程和队列，而是在发送事件时直接处理事件
    """

    def __init__(self, interval: int = 1):
        self.__interval = interval  # timer的间隔
        self.__active = False
        # 不再使用队列，保留事件历史记录
        self.event_history = []
        self.__handlers = dict()
        self.__data_handler = dict()
        self.__general_handlers = []

    # 移除计时器线程方法，不再需要
    def _runTimer(self):
        """
        计时器(单线程模式下不再使用)
        """
        pass

    # 移除主线程方法，不再需要，但保留_process方法处理事件
    def _process(self, event):
        """
        处理event
        :param event:
        :return:
        """
        # 更新当前回测时间(如果事件有时间戳)
        global CURRENT_BACKTEST_TIME
        if hasattr(event, 'data') and hasattr(event.data, 'timestamp') and event.data.timestamp:
            CURRENT_BACKTEST_TIME = event.data.timestamp
        
        # 处理事件
        if event.type in self.__handlers:
            for handler in self.__handlers[event.type]:
                handler(event)
        
        if self.__general_handlers:
            for handler in self.__general_handlers:
                handler(event)

    def start(self):
        """
        启动事件引擎，单线程模式下不再启动线程
        """
        self.__active = True
    
    def stop(self):
        """
        停止事件引擎
        """
        self.__active = False
        
    def register(self, type, handler):
        """
        注册回调函数
        """
        try:
            handler_list = self.__handlers[type]
        except KeyError:
            handler_list = []

        if handler not in handler_list:
            handler_list.append(handler)

        self.__handlers[type] = handler_list

    def unregister(self, type, handler):
        """
        注销回调函数
        """
        try:
            handler_list = self.__handlers[type]

            if handler in handler_list:
                handler_list.remove(handler)

            if not handler_list:
                del self.__handlers[type]

        except KeyError:
            pass

    def register_general_handler(self, handler):
        """
        注册通用回调函数
        """
        if handler not in self.__general_handlers:
            self.__general_handlers.append(handler)

    def unregister_general_handler(self, handler):
        """
        注销通用回调函数
        """
        if handler in self.__general_handlers:
            self.__general_handlers.remove(handler)

    def send_event(self, event):
        """
        发送事件，单线程模式下直接处理事件，不再放入队列
        """
        if not self.__active:
            return
            
        # 将事件添加到历史记录
        self.event_history.append(event)
        
        # 直接处理事件
        self._process(event)


if __name__ == "__main__":
    class A:
        def __init__(self):
            pass

        @staticmethod
        def func(event):
            print("处理事件.....")


    # 测试单线程模式
    ee = Event_Engine()
    type_ = TIMER_EVENT.type
    ee.register(type_, A.func)
    ee.start()
    ee.send_event(TIMER_EVENT)
    # 测试通过
