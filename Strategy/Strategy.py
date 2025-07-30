# encoding=utf-8
"""
Author: Richard
Date: 7-10-2020
"""
from abc import ABCMeta
from Utils.Constant import *
from Event_Engine import Event_Engine
from Utils.Event import *



class StrategyBase(object):
    """
    策略抽象基类
    """
    __metaclass__ = ABCMeta


class StrategyTemplate(StrategyBase):
    """
    策略模板类1
    """

    def __init__(self):
        self.inited = False

        self.symbol = None

        self.last_price = dict()
        self.realized_pnls = dict()
        self.unrealized_pnls = dict()
        self.occupied_margins = dict()

    def init(self, engine):
        self.inited = True
        self.engine = engine

    def onStart(self):
        """
        启动策略(必须由用户继承实现)
        """
        raise NotImplementedError("function onStart() is not implemented")

    def onStop(self):
        """
        停止策略(必须由用户继承实现)
        """
        raise NotImplementedError("function onStop() is not implemented")

    def onBar(self, bar):
        """
        收到行情bar推送(必须由用户继承实现)
        """
        raise NotImplementedError("function onBar() is not implemented")

    def onOrder(self, orderback):
        """
        收到order回执推送(必须由用户继承实现)
        """
        raise NotImplementedError("function onOrder() is not implemented")

    def onPosition(self, position):
        """
        仓位信息管理(必须由用户继承实现)
        """
        raise NotImplementedError("function onPosition() is not implemented")

    def onAccount(self, account):
        """
        仓位信息管理(必须由用户继承实现)
        """
        raise NotImplementedError("function onAccount() is not implemented")

    def buy(self, symbol, price, volume_in_contract, type, bar):
        """
        买开
        """
        self.send_order(symbol, price, volume_in_contract, action=OrderAction.Buy, type=type, offset=OrderOffset.Open, bar=bar)

    def sell(self, symbol, price, volume_in_contract, type, bar):
        """
        卖平
        """
        self.send_order(symbol, price, volume_in_contract, action=OrderAction.Sell, type=type, offset=OrderOffset.Close, bar=bar)

    def short(self, symbol, price, volume_in_contract, type, bar):
        """
        卖开
        """
        self.send_order(symbol, price, volume_in_contract, action=OrderAction.Sell, type=type, offset=OrderOffset.Open, bar=bar)

    def cover(self, symbol, price, volume_in_contract, type, bar):
        """
        买平
        """
        self.send_order(symbol, price, volume_in_contract, action=OrderAction.Buy, type=type, offset=OrderOffset.Close, bar=bar)

    def bid(self, symbol, price, volume_in_contract, type, pos_short=0):
        """
        bid方向开单(cover/buy),
        需要输入当前空头仓位,优先发cover
        """
        cover_vol = min(volume_in_contract, pos_short)
        buy_vol = volume_in_contract - cover_vol

        if cover_vol > 0:
            self.cover(symbol, price, volume_in_contract, type=type)
        if buy_vol > 0:
            self.buy(symbol, price, volume_in_contract, type=type)
        return pos_short - cover_vol

    def ask(self, symbol, price, volume_in_contract, type, pos_long=0):
        """
        ask方向开单(sell/short),
        需要输入当前多头仓位,优先发sell
        """
        sell_vol = min(volume_in_contract, pos_long)
        short_vol = volume_in_contract - sell_vol

        if sell_vol > 0:
            self.sell(symbol, price, sell_vol, type=type)
        if short_vol > 0:
            self.short(symbol, price, volume_in_contract, type=type)

        return pos_long - sell_vol

    def send_order(self, symbol, price, volume_in_contract, action, type, offset, bar):
        """
        向event engine发送发单事件
        action: buy/sell/cancel
        type: limit/market
        """
        if self.inited:  # 策略已经初始化完成
            response = self.engine.sendOrder(symbol, price, volume_in_contract, action, type, offset=offset, bar=bar)
            return response

    def cancel_order(self, symbol, order_id):
        """
        根据id取消相应的订单
        """
        self.engine.cancelOrder(symbol, order_id)

    def cancel_all_orders(self, symbol):
        """
        撤掉所有的订单
        """
        self.engine.cancelAll(symbol)

    def write_log(self, msg, level):
        """
        输出日志
        """
        self.engine.write_log(msg, level)

    def ExecutionLargeOrder(self, symbol, type, price, volume, direction, offset):
        """
        优化的大额订单下单算法,按需求可以自己实现
        """
        pass
