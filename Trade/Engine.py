# encoding:utf-8
"""
Author:Richard
7-9-2020
Modified: 2025 (removed multithreading for deterministic backtest)
"""
from abc import ABCMeta, abstractmethod
from Event_Engine import Event_Engine
import logging

from Utils.Event import *
from Data.DataHandlers import MongoDBHandler
from Utils.Constant import *
from Utils.DataStructure import POSITION, ACCOUNT
from Utils.util import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pymongo
from io import BytesIO
from datetime import datetime
import time
import os
import matplotlib
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.gridspec import GridSpec
from matplotlib.dates import DateFormatter
import pyarrow.parquet as pq

matplotlib.pyplot.switch_backend('Agg')


class EngineBase:
    """
    基类
    """
    __metaclass__ = ABCMeta

    def __init__(self, ee: Event_Engine, engine_name: str):
        self.event_manager = ee
        self.name = engine_name

    @abstractmethod
    def close(self, event):
        raise NotImplementedError("function close() must be implemented by user")

    @abstractmethod
    def addStrategy(self, strategy):
        raise NotImplementedError("function addStrategy() must be implemented by user")


class PositionEngine(EngineBase):
    def __init__(self, ee: Event_Engine, config, cfg, **kwargs):
        super(PositionEngine, self).__init__(ee, "position")
        """
        基于币本位的计算方式,如果需要USDT本位,则需要另行实现position和account的计算方法
        """
        self.config = config
        self.cfg = cfg
        self.kwargs = kwargs
        self.__position_DB = self.config['DB']["POSITION_DB"]
        self.__position_COL_List = self.config['DB']["POSITION_COL"]  # 不同品种的long short position

        self.__account_DB = self.config["DB"]['ACCOUNT_DB']
        self.__account_COL = dict()

        self.strategy = None
        self.trading_symbols = self.config['TradingSymbols']
        self.account = dict()  # each symbol has its corresponding sub-account
        self.position = dict()
        self.save_account = dict()
        self.save_position = dict()
        self.last_order_id = None
        self.back_id = None
        self.last_price = dict()

        self.Connect_MONGO()
        self.init()

    def init(self):
        """
        初始化
        """
        for symbol in self.trading_symbols:
            self.save_position[symbol] = {'long': [], 'short': []}
            self.save_account[symbol] = []

            self.position[symbol] = {}
            self.position[symbol]['long'] = POSITION(symbol=symbol)
            # available是volume减去冻结的部分
            self.save_position_info(self.position[symbol]['long'], 'long', 'init')

            self.position[symbol]['short'] = POSITION(symbol=symbol)
            self.save_position_info(self.position[symbol]['short'], 'short', 'init')
            self.last_price[symbol] = None

            # init account_collection
            self.__account_COL[symbol] = self.config["DB"]['ACCOUNT_COL'][symbol]
            # init account
            self.account[symbol] = ACCOUNT(symbol=symbol, init_balance=float(self.config['init_account']), margin_available=float(self.config['init_account']))
            self.save_account_info(self.account[symbol])

    def register_event(self):
        """
        注册函数
        """
        self.event_manager.register(Event_Type.EVENT_STOP, self.save_data)
        self.event_manager.register(Event_Type.EVENT_ORDERBACK, self.update_position)
        self.event_manager.register(Event_Type.EVENT_STOP, self.close)

    def Connect_MONGO(self):
        """
        链接数据库
        """
        self.mongo_service = MongoDBHandler(self.config)
        self.mongo_service.Connect_DB()

    def addStrategy(self, strategy):
        self.strategy = strategy
        self.strategy_name = self.config['strategy_name']

    def update_position(self, event):
        """
        收到order的回执,更新position的信息
        fee/pnl calculated in BTC, trade unit need to be stated in config document
        """
        global orderBack
        if event.type == Event_Type.EVENT_ORDERBACK:
            
            orderBack = event.data
            price = orderBack.last_price
            symbol = orderBack.symbol
            exchange, signal, contract_type = symbol.split('_')
            
            ### orderBack.fee in trade_unit
            trade_fee = orderBack.fee

            if orderBack.offset == OrderOffset.Open and orderBack.direction == OrderAction.Buy:
                # 买开,更新多头仓位信息
                # 不需要更新pos frozen,更新acc frozen
                if orderBack.status == OrderStatus.AllTraded:
                    self.position[symbol]['long'].cur_price = price

                    self.position[symbol]['long'].margin_frozen += orderBack.volume - orderBack.trade_volume

                    self.position[symbol]['long'].direction = PositionDirection.Long
                    self.position[symbol]['long'].timestamp = orderBack.timestamp
                    self.position[symbol]['long'].trade_volume += orderBack.trade_volume

                    self.position[symbol]['long'].avg_price = cal_avg_price(exchange, signal, contract_type, self.config['Trade_Unit'], 
                                                                            orderBack.traded_avg_price, orderBack.trade_volume_in_contract,
                                                                            self.position[symbol]['long'].contracts, self.position[symbol]['long'].avg_price)
                    
                    # trade unit
                    self.position[symbol]['long'].volume += orderBack.trade_volume

                    # contract
                    self.position[symbol]['long'].available += orderBack.trade_volume_in_contract

                    # contracts
                    self.position[symbol]['long'].contracts += orderBack.trade_volume_in_contract

                    # trade unit
                    self.position[symbol]['long'].profit_unreal = cal_position_pnl(exchange, signal,
                                                                                   contract_type, self.config['Trade_Unit'], price,
                                                                                   self.position[symbol][
                                                                                       'long'].avg_price,
                                                                                   self.position[symbol][
                                                                                       'long'].contracts)

                    #  trade unit
                    self.position[symbol]['long'].tmp_unreal_pnl = self.position[symbol]['long'].profit_unreal

                    # 开仓realized profit是负手续费
                    self.position[symbol]['long'].profit_real = -trade_fee
                    self.position[symbol]['long'].tmp_real_pnl = -trade_fee
                    self.position[symbol]['long'].hedge_pnl = -trade_fee
                    self.position[symbol]['long'].position_pnl = 0
                    self.position[symbol]['long'].funding_pnl = 0

                    self.position[symbol]['long'].profit_total = self.position[symbol]['long'].profit_real + \
                                                                 self.position[symbol]['long'].profit_unreal

                    self.position[symbol]['long'].total_pnl = self.position[symbol]['long'].position_pnl + \
                                                              self.position[symbol]['long'].hedge_pnl + \
                                                              self.position[symbol]['long'].funding_pnl

                else:
                    self.write_log("order back status error", logging.ERROR)

                # 储存position
                self.save_position_info(self.position[symbol]['long'], type='long', source='order')

                # 用long,short position更新account
                self.update_account(self.position[symbol])
                # account_update = COMMON_ACCOUNTUPDATE_EVENT(self.position[symbol])
                # self.event_manager.send_event(account_update)

            elif orderBack.offset == OrderOffset.Open and orderBack.direction == OrderAction.Sell:
                # 卖开,更新空头仓位信息
                # 不需要更新pos frozen,更新acc frozen
                if orderBack.status == OrderStatus.AllTraded:
                    # 第一次撮合order
                    self.position[symbol]['short'].cur_price = price
                    self.position[symbol]['short'].margin_frozen += orderBack.volume - orderBack.trade_volume

                    self.position[symbol]['short'].direction = PositionDirection.Short
                    self.position[symbol]['short'].timestamp = orderBack.timestamp
                    self.position[symbol]['short'].trade_volume += orderBack.trade_volume

                    self.position[symbol]['short'].avg_price = cal_avg_price(exchange, signal, contract_type, self.config['Trade_Unit'], 
                                                                            orderBack.traded_avg_price, orderBack.trade_volume_in_contract,
                                                                            self.position[symbol]['short'].contracts, self.position[symbol]['short'].avg_price)
                    
                    ### in trade unit
                    self.position[symbol]['short'].volume += orderBack.trade_volume

                    ### in contract
                    self.position[symbol]['short'].available += orderBack.trade_volume_in_contract

                    ### in contract
                    self.position[symbol]['short'].contracts += orderBack.trade_volume_in_contract
                    
                    ### in trade unit
                    self.position[symbol]['short'].profit_unreal = -cal_position_pnl(exchange, signal,
                                                                                     contract_type, self.config['Trade_Unit'], price,
                                                                                     self.position[symbol][
                                                                                         'short'].avg_price,
                                                                                     self.position[symbol][
                                                                                         'short'].contracts)

                    self.position[symbol]['short'].tmp_unreal_pnl = self.position[symbol]['short'].profit_unreal

                    # 开仓的realized profit是负的手续费
                    self.position[symbol]['short'].profit_real -= trade_fee
                    self.position[symbol]['short'].tmp_real_pnl -= trade_fee
                    self.position[symbol]['short'].hedge_pnl = -trade_fee
                    self.position[symbol]['short'].position_pnl = 0
                    self.position[symbol]['short'].funding_pnl = 0

                    self.position[symbol]['short'].profit_total = self.position[symbol]['short'].profit_real + \
                                                                  self.position[symbol]['short'].profit_unreal

                    self.position[symbol]['short'].total_pnl = self.position[symbol]['short'].position_pnl + \
                                                               self.position[symbol]['short'].hedge_pnl + \
                                                               self.position[symbol]['short'].funding_pnl

                else:
                    self.write_log("order back status error", logging.ERROR)

                # 储存position
                self.save_position_info(self.position[symbol]['short'], type='short', source='order')

                # 用long short position更新account
                self.update_account(self.position[symbol])
                # account_update = COMMON_ACCOUNTUPDATE_EVENT(self.position[symbol])
                # self.event_manager.send_event(account_update)

            elif orderBack.offset == OrderOffset.Close and orderBack.direction == OrderAction.Buy:
                if orderBack.status == OrderStatus.AllTraded:
                    # 买平,更新空头仓位
                    self.position[symbol]['short'].cur_price = price
                    self.position[symbol]['short'].timestamp = orderBack.timestamp
                    self.position[symbol]['short'].trade_volume += orderBack.trade_volume

                    # 持仓均价不需要进行更新
                    # frozen更新,根据订单回执是否是第一次成交
                    # in contract   
                    self.position[symbol]['short'].frozen += orderBack.volume_in_contract - orderBack.trade_volume_in_contract

                    # in contract
                    self.position[symbol]['short'].contracts -= orderBack.trade_volume_in_contract

                    # in trade_unit
                    self.position[symbol]['short'].volume = cal_value_in_trade_unit(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                       self.position[symbol]['short'].contracts, 
                                                       self.position[symbol]['short'].avg_price)  # convert contract to trade unit

                    # in contract
                    self.position[symbol]['short'].available = self.position[symbol]['short'].contracts - \
                                                               self.position[symbol]['short'].frozen

                    # 平仓盈利之外还有手续费, in trade unit
                    self.position[symbol]['short'].profit_real -= trade_fee
                    self.position[symbol]['short'].profit_real += -cal_profit_real(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                                                   orderBack.traded_avg_price,
                                                                                   self.position[symbol]['short'].avg_price,
                                                                                   orderBack.trade_volume_in_contract)

                    self.position[symbol]['short'].tmp_real_pnl = self.position[symbol]['short'].profit_real

                    ### in trade unit
                    self.position[symbol]['short'].profit_unreal = -cal_position_pnl(exchange, signal,
                                                                                     contract_type, self.config['Trade_Unit'], price,
                                                                                     self.position[symbol][
                                                                                         'short'].avg_price,
                                                                                     self.position[symbol][
                                                                                         'short'].contracts)

                    self.position[symbol]['short'].hedge_pnl = -trade_fee
                    self.position[symbol]['short'].position_pnl = 0
                    self.position[symbol]['short'].funding_pnl = 0

                    self.position[symbol]['short'].tmp_unreal_pnl = self.position[symbol]['short'].profit_unreal

                    self.position[symbol]['short'].profit_total = self.position[symbol]['short'].profit_real + \
                                                                  self.position[symbol]['short'].profit_unreal

                    self.position[symbol]['short'].total_pnl = self.position[symbol]['short'].hedge_pnl + \
                                                               self.position[symbol]['short'].position_pnl + \
                                                               self.position[symbol]['short'].funding_pnl

                else:
                    self.write_log("order back error", logging.ERROR)

                # 储存position
                self.save_position_info(self.position[symbol]['short'], type='short', source='order')

                # 用long short position更新account
                self.update_account(self.position[symbol])
                # account_update = COMMON_ACCOUNTUPDATE_EVENT(self.position[symbol])
                # self.event_manager.send_event(account_update)

                # 检查空头头寸是否为0
                if self.position[symbol]['short'].volume > 0:
                    pass
                elif self.position[symbol]['short'].volume == 0:
                    self.position[symbol]['short'].direction = PositionDirection.Net
                    self.position[symbol]['short'].avg_price = 0
                    self.position[symbol]['short'].tmp_real_pnl = 0
                    self.position[symbol]['short'].tmp_unreal_pnl = 0

            elif orderBack.offset == OrderOffset.Close and orderBack.direction == OrderAction.Sell:
                if orderBack.status == OrderStatus.AllTraded:
                    # 卖平,更新多头仓位
                    self.position[symbol]['long'].cur_price = price
                    self.position[symbol]['long'].timestamp = orderBack.timestamp
                    self.position[symbol]['long'].trade_volume += orderBack.trade_volume

                    # 持仓均价不需要更新
                    # frozen更新,根据订单绘制是否是第一次成交
                    # in contract
                    self.position[symbol]['long'].frozen += orderBack.volume_in_contract - orderBack.trade_volume_in_contract

                    self.position[symbol]['long'].contracts -= orderBack.trade_volume_in_contract 

                    self.position[symbol]['long'].volume = cal_value_in_trade_unit(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                       self.position[symbol]['long'].contracts, 
                                                       self.position[symbol]['long'].avg_price)  # convert contract to trade unit

                    self.position[symbol]['long'].available = self.position[symbol]['long'].contracts - \
                                                              self.position[symbol]['long'].frozen

                    # 平仓盈利之外还有手续费
                    self.position[symbol]['long'].profit_real -= trade_fee
                    self.position[symbol]['long'].profit_real += cal_profit_real(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                                                 orderBack.traded_avg_price,
                                                                                 self.position[symbol]['long'].avg_price,
                                                                                 orderBack.trade_volume_in_contract)

                    self.position[symbol]['long'].tmp_real_pnl = self.position[symbol]['long'].profit_real

                    self.position[symbol]['long'].profit_unreal = cal_position_pnl(exchange, signal,
                                                                                   contract_type, self.config['Trade_Unit'], price,
                                                                                   self.position[symbol][
                                                                                       'long'].avg_price,
                                                                                   self.position[symbol][
                                                                                       'long'].contracts)

                    self.position[symbol]['long'].hedge_pnl = -trade_fee
                    self.position[symbol]['long'].position_pnl = 0
                    self.position[symbol]['long'].funding_pnl = 0

                    self.position[symbol]['long'].tmp_unreal_pnl = self.position[symbol]['long'].profit_unreal

                    self.position[symbol]['long'].profit_total = self.position[symbol]['long'].profit_real + \
                                                                 self.position[symbol]['long'].profit_unreal

                    self.position[symbol]['long'].total_pnl = self.position[symbol]['long'].hedge_pnl + \
                                                              self.position[symbol]['long'].position_pnl + \
                                                              self.position[symbol]['long'].funding_pnl

                else:
                    self.write_log("order back error", logging.ERROR)

                # 储存position
                self.save_position_info(self.position[symbol]['long'], type='long', source='order')

                # 用long short position更新account
                self.update_account(self.position[symbol])
                # account_update = COMMON_ACCOUNTUPDATE_EVENT(self.position[symbol])
                # self.event_manager.send_event(account_update)

                # 检查多头头寸是否是0
                if self.position[symbol]['long'].volume > 0:
                    pass
                elif self.position[symbol]['long'].volume == 0:
                    self.position[symbol]['long'].direction = PositionDirection.Net
                    self.position[symbol]['long'].avg_price = 0
                    self.position[symbol]['long'].tmp_unreal_pnl = 0
                    self.position[symbol]['long'].tmp_real_pnl = 0

        else:
            self.write_log("not order back", logging.WARNING)

        # print(orderBack, self.position)
        if self.strategy is not None:
            self.strategy.onPosition(self.position)

            if self.last_order_id == self.back_id and self.back_id is not None:
                self.strategy.pos_update = 1

    def update_pnl(self, bar):
        """
        更新position的未实现盈亏, 杠杆率等信息, 由 Exchange Engine的 Feed 触发
        """
        
        price = bar.close
        symbol = bar.symbol
        exchange, signal, contract_type = symbol.split('_')

        # long
        self.position[symbol]['long'].cur_price = price
        # self.position[symbol]['long'].volume = self.position[symbol]['long'].volume
        # self.position[symbol]['long'].contracts = self.position[symbol]['long'].contracts
        # self.position[symbol]['long'].available = self.position[symbol]['long'].available
        self.position[symbol]['long'].trade_volume = 0
        self.position[symbol]['long'].hedge_pnl = 0
        self.position[symbol]['long'].funding_pnl = 0

        if self.last_price[symbol] and self.position[symbol]['long'].contracts != 0:
            self.position[symbol]['long'].position_pnl = cal_position_pnl(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                                          price, self.last_price[symbol],
                                                                          self.position[symbol]['long'].contracts)

        if self.position[symbol]['long'].avg_price:
            self.position[symbol]['long'].profit_unreal = cal_position_pnl(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                                           price, self.position[symbol]['long'].avg_price,
                                                                           self.position[symbol]['long'].contracts)


        self.position[symbol]['long'].profit_total = self.position[symbol]['long'].profit_unreal + \
                                                     self.position[symbol]['long'].profit_real

        self.position[symbol]['long'].total_pnl = self.position[symbol]['long'].position_pnl + \
                                                  self.position[symbol]['long'].hedge_pnl + \
                                                  self.position[symbol]['long'].funding_pnl
        if self.position[symbol]['long'].contracts:
            self.position[symbol]['long'].timestamp = bar.timestamp
            self.save_position_info(self.position[symbol]['long'], type='long', source='pnl')

            self.update_account(self.position[symbol])
            # account_update = COMMON_ACCOUNTUPDATE_EVENT(self.position[symbol])
            # self.event_manager.send_event(account_update)

        # short
        self.position[symbol]['short'].cur_price = price
        # self.position[symbol]['short'].volume = self.position[symbol]['short'].volume
        # self.position[symbol]['short'].contracts = self.position[symbol]['short'].contracts
        # self.position[symbol]['short'].available = self.position[symbol]['short'].volume
        self.position[symbol]['short'].trade_volume = 0
        self.position[symbol]['short'].hedge_pnl = 0
        self.position[symbol]['short'].funding_pnl = 0

        if self.last_price[symbol] and self.position[symbol]['short'].contracts != 0:
            self.position[symbol]['short'].position_pnl = -cal_position_pnl(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                                            price, self.last_price[symbol],
                                                                            self.position[symbol]['short'].contracts)
        if self.position[symbol]['short'].avg_price:
            self.position[symbol]['short'].profit_unreal = -cal_position_pnl(exchange, signal, contract_type, self.config['Trade_Unit'], price,
                                                                             self.position[symbol]['short'].avg_price,
                                                                             self.position[symbol]['short'].contracts)

        self.position[symbol]['short'].profit_total = self.position[symbol]['short'].profit_unreal + \
                                                      self.position[symbol]['short'].profit_real

        self.position[symbol]['short'].total_pnl = self.position[symbol]['short'].position_pnl + \
                                                   self.position[symbol]['short'].hedge_pnl + \
                                                   self.position[symbol]['short'].funding_pnl
        if self.position[symbol]['short'].contracts:
            self.position[symbol]['short'].timestamp = bar.timestamp
            self.save_position_info(self.position[symbol]['short'], type='short', source='pnl')

            self.update_account(self.position[symbol])
            # account_update = COMMON_ACCOUNTUPDATE_EVENT(self.position[symbol])
            # self.event_manager.send_event(account_update)

        self.last_price[symbol] = price

        # update pnl
        self.strategy.onPosition(self.position)

    def update_funding_pnl(self, bar):
        """
        收到funding数据,更新position的信息
        """
        symbol = bar.symbol
        exchange, signal, contract_type = symbol.split('_')
        funding_rate = bar.funding_rate
        timestamp = bar.timestamp
        
        # price for quanto need to get btc_spot value, for other symbols, price can be get from bar data
        price = self.last_price[symbol] if self.last_price[symbol] else get_spot_price(exchange, signal, timestamp)
        dt_time = timestamp[-8:]
        settlement_time = get_settlement_time(symbol.split('_')[0])
        if dt_time in settlement_time:
            # funding交割影响已实现收益
            if self.position[symbol]['long'].volume != 0:
                self.position[symbol]['long'].direction = PositionDirection.Long
                self.position[symbol]['long'].timestamp = timestamp
                self.position[symbol]['long'].avg_price = self.position[symbol]['long'].avg_price

                self.position[symbol]['long'].contracts = self.position[symbol]['long'].contracts
                self.position[symbol]['long'].available = self.position[symbol]['long'].available
                self.position[symbol]['long'].volume = self.position[symbol]['long'].volume
                ### in trade unit
                self.position[symbol]['long'].trade_volume += funding_rate * self.position[symbol]['long'].volume

                self.position[symbol]['long'].position_pnl = 0
                self.position[symbol]['long'].hedge_pnl = 0
                ### in trade unit
                funding = funding_rate * self.position[symbol]['long'].volume
                self.position[symbol]['long'].funding_pnl = -funding
                self.position[symbol]['long'].profit_real -= funding

                self.position[symbol]['long'].profit_total = self.position[symbol]['long'].profit_real + \
                                                            self.position[symbol]['long'].profit_unreal

                self.position[symbol]['long'].total_pnl = self.position[symbol]['long'].position_pnl + \
                                                          self.position[symbol]['long'].hedge_pnl + \
                                                          self.position[symbol]['long'].funding_pnl

                # 储存position
                self.save_position_info(self.position[symbol]['long'], type='long', source='funding')

                # 用long,short position更新account
                self.update_account(self.position[symbol])
                # account_update = COMMON_ACCOUNTUPDATE_EVENT(self.position[symbol])
                # self.event_manager.send_event(account_update)

            if self.position[symbol]['short'].volume != 0:
                self.position[symbol]['short'].direction = PositionDirection.Short
                self.position[symbol]['short'].timestamp = timestamp
                self.position[symbol]['short'].avg_price = self.position[symbol]['short'].avg_price

                self.position[symbol]['short'].contracts = self.position[symbol]['short'].contracts
                self.position[symbol]['short'].available = self.position[symbol]['short'].available
                self.position[symbol]['short'].volume = self.position[symbol]['short'].volume
                self.position[symbol]['short'].trade_volume += funding_rate * self.position[symbol]['short'].volume

                self.position[symbol]['short'].position_pnl = 0
                self.position[symbol]['short'].hedge_pnl = 0
                ### in trade unit
                funding = funding_rate * self.position[symbol]['short'].volume
                self.position[symbol]['short'].funding_pnl = funding
                self.position[symbol]['short'].profit_real += funding

                self.position[symbol]['short'].profit_total = self.position[symbol]['short'].profit_real + \
                                                             self.position[symbol]['short'].profit_unreal

                self.position[symbol]['short'].total_pnl = self.position[symbol]['short'].position_pnl + \
                                                           self.position[symbol]['short'].hedge_pnl + \
                                                           self.position[symbol]['short'].funding_pnl
                
                # 储存position
                self.save_position_info(self.position[symbol]['short'], type='short', source='funding')

                # 用long,short position更新account
                self.update_account(self.position[symbol])
                # account_update = COMMON_ACCOUNTUPDATE_EVENT(self.position[symbol])
                # self.event_manager.send_event(account_update)

        # update pnl
        self.strategy.onPosition(self.position)


    def update_account(self, event):
        """
        position收到订单回执之后更新,然后发送long short两个position信息到account进行更新
        """
        positions = event
        long_position: POSITION = positions['long']
        short_position: POSITION = positions['short']

        symbol = long_position.symbol
        try:
            timestamp = max(long_position.timestamp, short_position.timestamp)
        except TypeError:
            if type(long_position.timestamp) == str:
                timestamp = short_position.timestamp
            else:
                timestamp = long_position.timestamp

        self.account[symbol].timestamp = timestamp

        # 更新其他account信息
        self.account[symbol].margin_position = long_position.volume + short_position.volume
        self.account[symbol].margin_frozen = long_position.margin_frozen + short_position.margin_frozen

        # position.profit_real 包含fee
        self.account[symbol].profit_real = long_position.profit_real + short_position.profit_real
        self.account[symbol].profit_unreal = long_position.profit_unreal + short_position.profit_unreal

        self.account[symbol].margin_balance = self.account[symbol].init_balance + \
                                              (self.account[symbol].profit_real + self.account[symbol].profit_unreal)

        self.account[symbol].margin_available = self.account[symbol].margin_balance - \
                                                self.account[symbol].profit_unreal - \
                                                self.account[symbol].margin_position - \
                                                self.account[symbol].margin_frozen

        # 保存account信息
        self.save_account_info(self.account[symbol])

        if self.strategy is not None:
            self.strategy.onAccount(self.account)

            if self.last_order_id == self.back_id and self.back_id is not None:
                self.strategy.acc_update = 1


    def save_position_info(self, pos: POSITION, type: str, source="unknown"):
        """
        存储position的信息
        """
        # Track source of position data
        if not hasattr(self, 'position_source_counts'):
            self.position_source_counts = {
                'init': 0,
                'order': 0,
                'pnl': 0,
                'funding': 0,
                'unknown': 0
            }
        self.position_source_counts[source] += 1
        
        symbol = pos.symbol
        position = {"symbol": pos.symbol, "timestamp": pos.timestamp, "volume": pos.volume,
                    "contracts": pos.contracts, "trade_volume": pos.trade_volume, "cur_price": pos.cur_price,
                    "direction": f'{pos.direction}', "available": pos.available, "frozen": pos.frozen,
                    "avg_price": pos.avg_price, "margin_frozen": pos.margin_frozen, "realized profit": pos.profit_real,
                    "unrealized profit": pos.profit_unreal, "hedge_pnl": pos.hedge_pnl,
                    "position_pnl": pos.position_pnl, "funding_pnl": pos.funding_pnl, "total_pnl": pos.total_pnl,
                    "source": source}  # Add source to tracked data

        if type == 'long':
            self.save_position[symbol]['long'].append(position)
        elif type == 'short':
            self.save_position[symbol]['short'].append(position)

        # Data = None
        # position = {"symbol": pos.symbol, "timestamp": pos.timestamp, "volume": pos.volume,
        #             "contracts": pos.contracts, "trade_volume": pos.trade_volume, "cur_price": pos.cur_price,
        #             "direction": f'{pos.direction}', "available": pos.available, "frozen": pos.frozen,
        #             "avg_price": pos.avg_price, "margin_frozen": pos.margin_frozen, "realized profit": pos.profit_real,
        #             "unrealized profit": pos.profit_unreal, "hedge_pnl": pos.hedge_pnl,
        #             "position_pnl": pos.position_pnl, "funding_pnl": pos.funding_pnl, "total_pnl": pos.total_pnl}
        # Info = {'req': 'insert', 'data': position}
        #
        # # print(Info['data'])
        # if type == 'long':
        #     mongo = MONGODATA(DB=self.__position_DB, COL=self.__position_COL_List[pos.symbol]['Long'], Data=Data,
        #                       Info=Info)
        #     fl = self.mongo_service.on_insert(mongo)
        #     if not fl:
        #         self.write_log("fail to insert long position info to MongoDB", logging.ERROR)
        # elif type == 'short':
        #     mongo = MONGODATA(DB=self.__position_DB, COL=self.__position_COL_List[pos.symbol]['Short'], Data=Data,
        #                       Info=Info)
        #     fl = self.mongo_service.on_insert(mongo)
        #     if not fl:
        #         self.write_log("fail to insert short position info to MongoDB", logging.ERROR)

    def save_account_info(self, acc: ACCOUNT):
        """
        存储account信息到mongodb对应的collection
        """
        symbol = acc.symbol
        account = {"symbol": symbol, "timestamp": acc.timestamp, "margin_balance": acc.margin_balance,
                   "margin_position": acc.margin_position, "margin_frozen": acc.margin_frozen,
                   "margin_available": acc.margin_available, "profit_real": acc.profit_real,
                   "profit_unreal": acc.profit_unreal, "init_balance": acc.init_balance,
                   "lever_rate": acc.lever_rate}
        self.save_account[symbol].append(account)

        # symbol = acc.symbol
        # Data = None
        #
        # account = {"symbol": symbol, "timestamp": acc.timestamp, "margin_balance": acc.margin_balance,
        #            "margin_position": acc.margin_position, "margin_frozen": acc.margin_frozen,
        #            "margin_available": acc.margin_available, "profit_real": acc.profit_real,
        #            "profit_unreal": acc.profit_unreal, "init_balance": acc.init_balance,
        #            "lever_rate": acc.lever_rate}  # lever_rate 默认值是1
        # Info = {"req": "insert", "data": account}
        # MongoData = MONGODATA(DB=self.__account_DB, COL=self.__account_COL[symbol], Data=Data, Info=Info)
        #
        # fl = self.mongo_service.on_insert(MongoData)
        # if not fl:
        #     self.write_log("fail to insert account info to MongoDB", logging.ERROR)

    def save_data(self, event):
        # Analyze position sources at the end of the backtest
        # 由于单线程模式，当交易所数据全部推送完毕后，才会返回到这里
        self.write_log("-------------- 数据推送完毕，回测结束 ---------------", logging.INFO)
        
        self.analyze_position_sources()
        
        for symbol in self.trading_symbols:
            # save to csv
            if not self.config['enable_mongodb']:
                out_dir = f"./bt_result/{self.config['user']}/{self.config['bt_time']}"
                os.makedirs(out_dir, exist_ok=True)

                long_df = pd.DataFrame(self.save_position[symbol]['long'])
                long_df.to_csv(os.path.join(out_dir, f"{symbol}_long.csv"), index=False)

                short_df = pd.DataFrame(self.save_position[symbol]['short'])
                short_df.to_csv(os.path.join(out_dir, f"{symbol}_short.csv"), index=False)

                # Save account
                account_df = pd.DataFrame(self.save_account[symbol])
                account_df.to_csv(os.path.join(out_dir, f"{symbol}_account.csv"), index=False)

            else:
                # save to db
                Data = None
                Info = {'req': 'insert', 'data': self.save_position[symbol]['long']}
                col = self.__position_COL_List[symbol]['Long'] + '|' + self.config['user'] + '|' + self.strategy_name + '|' + self.config['bt_time'] 

                mongo = MONGODATA(DB=self.__position_DB, COL=col, Data=Data, Info=Info)
                fl = self.mongo_service.on_insert_many(mongo)
                if not fl:
                    self.write_log("fail to insert long position info to MongoDB", logging.ERROR)

                Info = {'req': 'insert', 'data': self.save_position[symbol]['short']}
                col = self.__position_COL_List[symbol]['Short'] + '|' + self.config['user'] + '|' + self.strategy_name + '|' + self.config['bt_time'] 

                mongo = MONGODATA(DB=self.__position_DB, COL=col, Data=Data, Info=Info)
                fl = self.mongo_service.on_insert_many(mongo)
                if not fl:
                    self.write_log("fail to insert long position info to MongoDB", logging.ERROR)

                Info = {"req": "insert", "data": self.save_account[symbol]}

                col = self.__account_COL[symbol] + '|' + self.config['user'] + '|' + self.strategy_name + '|' + self.config['bt_time'] 

                MongoData = MONGODATA(DB=self.__account_DB, COL=col, Data=Data, Info=Info)

                fl = self.mongo_service.on_insert_many(MongoData)
                if not fl:
                    self.write_log("fail to insert account info to MongoDB", logging.ERROR)

        event = PLOT_EVENT()
        self.event_manager.send_event(event)
        

    def write_log(self, msg: str, level: int = logging.INFO):
        """
        打印信息
        """
        logdata = LOGDATA(log_content=msg,
                          log_level=level)

        event = LOG_EVENT(data=logdata)
        self.event_manager.send_event(event)

    def close(self, event):
        if event.type == Event_Type.EVENT_STOP:
            self.mongo_service.disconnected()

            self.write_log(msg="End Position Engine")
            # self.write_log(msg=event.data['info'])

    def analyze_position_sources(self):
        """Analyze and print statistics about position data sources"""
        if hasattr(self, 'position_source_counts'):
            print(f"Position data source counts: {self.position_source_counts}")
            # Count sources by symbol
            symbol_counts = {}
            for symbol in self.trading_symbols:
                symbol_counts[symbol] = {'long': {}, 'short': {}}
                for direction in ['long', 'short']:
                    # Initialize counters
                    for source in ['init', 'order', 'pnl', 'funding', 'unknown']:
                        symbol_counts[symbol][direction][source] = 0
                    
                    # Count sources
                    positions = self.save_position[symbol][direction]
                    for pos in positions:
                        if 'source' in pos:
                            source = pos['source']
                            symbol_counts[symbol][direction][source] += 1
                        else:
                            symbol_counts[symbol][direction]['unknown'] += 1
            
            # Log detailed counts by symbol
            print(f"Position data source counts by symbol: {symbol_counts}")
        else:
            print("No position source tracking data available")


class OrderEngine(EngineBase):
    def __init__(self, event_engine: Event_Engine, config, cfg):
        super(OrderEngine, self).__init__(event_engine, "order")

        self.config = config
        self.cfg = cfg

        self.__order_db = self.config['DB']['ORDER_DB']
        self.__order_col = self.config['DB']['ORDER_COL']

        self.strategy = None

        self.Connect_MONGO()
        self.init()

    def init(self):
        pass

    def register_event(self):
        self.event_manager.register(Event_Type.EVENT_STOP, self.save_order)

    def Connect_MONGO(self):
        """
        Connect to MongoDB
        """
        self.mongo_service = MongoDBHandler(self.config)
        self.mongo_service.Connect_DB()

    def addStrategy(self, strategy):
        self.strategy = strategy
        self.strategy_name = self.config['strategy_name']

    def _process_order(self, order_event):
        """
        Create a dictionary to save order
        """
        pass

    def save_order(self, event):
        """
        Save order in MongoDB after trading ends
        """
        pass



# temporarily abandoned
# class AccountEngine(EngineBase):
#     def __init__(self, ee: Event_Engine):
#         super(AccountEngine, self).__init__(ee, "account")
#
#         self.__account_DB = self.config["DB"]['ACCOUNT_DB']
#         self.__account_COL = dict()
#
#         self.strategy = None
#         self.account = dict()  # each symbol has its corresponding sub-account
#         self.trading_symbols = [self.config['TradingSymbols'][item] for item in self.config['TradingSymbols']]
#
#         self.Connect_MONGO()
#         self.init()
#         self.strategy = None
#
#         self.last_order_id = None
#         self.back_id = None
#
#     def init(self):
#         for symbol in self.trading_symbols:
#             # init account_collection
#             self.__account_COL[symbol] = self.config["DB"]['ACCOUNT_COL'][symbol]
#             # init account
#             self.account[symbol] = ACCOUNT(symbol=symbol, margin_balance=0, margin_position=0, margin_frozen=0,
#                                            margin_available=0, profit_real=0, profit_unreal=0, lever_rate=1,
#                                            init_balance=int(self.config['Init_Amount']))
#             self.save_account_info(self.account[symbol])
#
#     def register_event(self):
#         """
#         注册函数,用position更新account
#         """
#         self.event_manager.register(Event_Type.EVENT_COMMON_ACCOUNT_UPDATE, self.update_account)
#
#     def Connect_MONGO(self):
#         self.mongo_service = MongoDBHandler()
#         self.mongo_service.Connect_DB()
#
#     def addStrategy(self, strategy):
#         self.strategy = strategy
#
#     def update_account(self, event):
#         """
#         position收到订单回执之后更新,然后发送long short两个position信息到account进行更新
#         """
#         positions = event.data
#         long_position: POSITION = positions['long']
#         short_position: POSITION = positions['short']
#
#         symbol = long_position.symbol
#         try:
#             timestamp = max(long_position.timestamp, short_position.timestamp)
#         except TypeError:
#             if type(long_position.timestamp) == str:
#                 timestamp = short_position.timestamp
#             else:
#                 timestamp = long_position.timestamp
#
#         self.account[symbol].timestamp = timestamp
#
#         # 更新其他account信息
#         self.account[symbol].margin_position = long_position.volume + short_position.volume
#         self.account[symbol].margin_frozen = long_position.margin_frozen + short_position.margin_frozen
#
#         # position.profit_real 包含fee
#         self.account[symbol].profit_real = long_position.profit_real + short_position.profit_real
#         self.account[symbol].profit_unreal = long_position.profit_unreal + short_position.profit_unreal
#
#         self.account[symbol].margin_balance = self.account[symbol].init_balance + \
#                                               (self.account[symbol].profit_real + self.account[symbol].profit_unreal)
#
#         self.account[symbol].margin_available = self.account[symbol].margin_balance - \
#                                                 self.account[symbol].profit_unreal - \
#                                                 self.account[symbol].margin_position - \
#                                                 self.account[symbol].margin_frozen
#
#         # 保存account信息
#         self.save_account_info(self.account[symbol])
#
#         self.strategy.onAccount(self.account)
#
#         if self.last_order_id == self.back_id and self.back_id is not None:
#             self.strategy.acc_update = 1
#
#     def save_account_info(self, acc: ACCOUNT):
#         """
#         存储account信息到mongodb对应的collection
#         """
#         symbol = acc.symbol
#         Data = None
#
#         account = {"symbol": symbol, "timestamp": acc.timestamp, "margin_balance": acc.margin_balance,
#                    "margin_position": acc.margin_position, "margin_frozen": acc.margin_frozen,
#                    "margin_available": acc.margin_available, "profit_real": acc.profit_real,
#                    "profit_unreal": acc.profit_unreal, "init_balance": acc.init_balance,
#                    "lever_rate": acc.lever_rate}  # lever_rate 默认值是1
#         Info = {"req": "insert", "data": account}
#         MongoData = MONGODATA(DB=self.__account_DB, COL=self.__account_COL[symbol], Data=Data, Info=Info)
#
#         fl = self.mongo_service.on_insert(MongoData)
#         if not fl:
#             self.write_log("fail to insert account info to MongoDB", logging.ERROR)
#
#     def write_log(self, msg: str, level: int = logging.INFO):
#         log_data = LOGDATA(log_content=msg,
#                            log_level=level)
#
#         event = LOG_EVENT(log_data)
#         self.event_manager.send_event(event)
#
#     def close(self, event):
#         if event.type == Event_Type.EVENT_STOP:
#             self.mongo_service.disconnected()
#
#             self.write_log(msg="End Account Engine")
#             # self.write_log(msg=event.data['info'])


class LogEngine(EngineBase):
    """
    日志
    """

    def __init__(self, event_engine: Event_Engine):
        super(LogEngine, self).__init__(event_engine, "log")

        self.level = logging.INFO
        self.logger = logging.getLogger("back-testing")
        self.logger.setLevel(self.level)

        self.formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s: %(message)s")

        self.add_null_handler()
        self.add_console_handler()
        self.register_event()

    def add_null_handler(self):
        null_handler = logging.NullHandler()
        self.logger.addHandler(null_handler)

    def add_console_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)

    def register_event(self):
        """
        注册回调函数,(type,function)
        """
        self.event_manager.register(Event_Type.EVENT_LOG, self._process_log_event)

    def _process_log_event(self, event):
        log = event.data
        self.logger.log(log.log_level, log.log_content)

    def close(self, event):
        pass


class PlotEngine(EngineBase):
    """
    画图
    """

    def __init__(self, event_engine: Event_Engine, config, cfg):
        super(PlotEngine, self).__init__(event_engine, "plot")

        self.config = config
        self.cfg = cfg

        self.host = self.config["DB"]["Mongo_Host"]
        self.port = int(self.config["DB"]["Mongo_Port"])
        self.__account_DB = self.config["DB"]['ACCOUNT_DB']
        self.__account_COL = dict()
        self.init_account = float(self.config['init_account'])

        self.strategy = None
        self.account = dict()  # each symbol has its corresponding sub-account
        self.trading_symbols = self.config['TradingSymbols']

        self.init()
        self.Connect_MONGO()
        self.register_event()

    def init(self):
        """
        初始化
        :return:
        """
        for symbol in self.trading_symbols:
            # init account_collection
            self.__account_COL[symbol] = self.config["DB"]['ACCOUNT_COL'][symbol]
            # init account
            self.account[symbol] = ACCOUNT(symbol=symbol, margin_balance=0, margin_position=0, margin_frozen=0,
                                           margin_available=0, profit_real=0, profit_unreal=0, lever_rate=1,
                                           init_balance=int(self.config['init_account']))

    def register_event(self):
        """
        注册函数,所有数据推送结束后,进行画图
        """
        self.event_manager.register(Event_Type.EVENT_PLOT, self.plot)
        # self.event_manager.register(Event_Type.EVENT_STOP, self.plot)
        # self.event_manager.register(Event_Type.EVENT_STOP, self.plot_position)
        # self.event_manager.register(Event_Type.EVENT_STOP, self.plot_kline)
        # self.event_manager.register(Event_Type.EVENT_STOP, self.plot_performance)
        # self.event_manager.register(Event_Type.EVENT_STOP, self.plot_pos)
        # self.event_manager.register(Event_Type.EVENT_STOP, self.plot_beta)
        # self.event_manager.register(Event_Type.EVENT_STOP, self.plot_price)

    def Connect_MONGO(self):
        """
        链接数据库
        """
        self.client = pymongo.MongoClient(host=self.host,
                                          port=self.port)
        self.my_db = self.client[self.__account_DB]
        self.position_db = self.client[f"{self.config['user']}_PositionInfo-{self.config['strategy_name']}"]

    def addStrategy(self, strategy):
        self.strategy = strategy
        self.strategy_name = self.config['strategy_name']

    def plot(self, event):
        # self.plot_kline()
        self.plot_performance()
        # self.plot_position()
        # self.plot_price()
        # self.plot_result()
        
        time.sleep(1)
        exit(0)

    def get_position_data(self):
        print('Getting position data')
        symbol_result = dict()
        sum_symbol_result = dict()

        # print(self.position_db[f"{symbol}_{}"])
        for symbol in self.trading_symbols:
            for direction in ['long', 'short']:
                if self.config['enable_mongodb']:
                    symbol_result[f"{symbol}_{direction}"] = dict()

                    symbol_table = self.position_db[f"{symbol}_{direction}|{self.config['user']}|{self.strategy_name}|{self.config['bt_time']}"]
                    symbol_tb = symbol_table.find({}, {"_id": 0, "position_pnl": 1, "hedge_pnl": 1,
                                                    "funding_pnl": 1, "total_pnl": 1,
                                                    "timestamp": 1})
                
                    symbol_list = list(symbol_tb[:])
                    symbol_result[f"{symbol}_{direction}"]["timestamp"] = list()
                    symbol_result[f"{symbol}_{direction}"]["hedge_pnl"] = list()
                    symbol_result[f"{symbol}_{direction}"]["position_pnl"] = list()
                    symbol_result[f"{symbol}_{direction}"]["funding_pnl"] = list()
                    symbol_result[f"{symbol}_{direction}"]["total_pnl"] = list()
                    for item in symbol_list:
                        symbol_result[f"{symbol}_{direction}"]["timestamp"].append(item["timestamp"])
                        symbol_result[f"{symbol}_{direction}"]["position_pnl"].append(item["position_pnl"])
                        symbol_result[f"{symbol}_{direction}"]["hedge_pnl"].append(item["hedge_pnl"])
                        symbol_result[f"{symbol}_{direction}"]["funding_pnl"].append(item["funding_pnl"])
                        symbol_result[f"{symbol}_{direction}"]["total_pnl"].append(item["total_pnl"])

                    symbol_result[f"{symbol}_{direction}"] = pd.DataFrame(symbol_result[f"{symbol}_{direction}"])

                else:
                    symbol_result[f"{symbol}_{direction}"] = pd.read_csv(f"./bt_result/{self.config['user']}/{self.config['bt_time']}/{symbol}_{direction}.csv").iloc[1:]

                
                symbol_result[f"{symbol}_{direction}"] = symbol_result[f"{symbol}_{direction}"].groupby(
                    by=['timestamp']).sum()
                symbol_result[f"{symbol}_{direction}"] = symbol_result[f"{symbol}_{direction}"].reset_index()

            if self.config['enable_mongodb']:
                combined_df = pd.concat([symbol_result[f"{symbol}_long"], symbol_result[f"{symbol}_short"]],
                                                    axis=0)
                combined_df = combined_df.drop(index='0', errors='ignore')
            else:
                combined_df = pd.concat([symbol_result[f"{symbol}_long"], symbol_result[f"{symbol}_short"]],
                                                    axis=0)

            sum_symbol_result[symbol] = combined_df.groupby('timestamp').sum().sort_index()
            sum_symbol_result[symbol] = sum_symbol_result[symbol].reset_index()
            sum_symbol_result[symbol].index = sum_symbol_result[symbol]["timestamp"]

        return sum_symbol_result

    def get_account_data(self):
        """
        画图逻辑
        :return:
        """
        print('Getting account data')
        symbol_result = dict()
        for symbol in self.trading_symbols:
            symbol_result[symbol] = dict()

        plt.figure(figsize=(10, 6), dpi=80)
        for symbol in self.trading_symbols:
            symbol_table = self.my_db[symbol]
            symbol_tb = symbol_table.find({}, {"_id": 0, "margin_available": 1, "margin_balance": 1,
                                               "margin_position": 1, "profit_real": 1, "profit_unreal": 1,
                                               "timestamp": 1})
            symbol_list = list(symbol_tb[:])
            symbol_result[symbol]["timestamp"] = list()
            symbol_result[symbol]["margin_available"] = list()
            symbol_result[symbol]["margin_balance"] = list()
            symbol_result[symbol]["margin_position"] = list()
            symbol_result[symbol]["profit_real"] = list()
            symbol_result[symbol]["profit_unreal"] = list()
            for item in symbol_list:
                symbol_result[symbol]["timestamp"].append(item["timestamp"])
                symbol_result[symbol]["margin_available"].append(item["margin_available"])
                symbol_result[symbol]["margin_balance"].append(item["margin_balance"])
                symbol_result[symbol]["margin_position"].append(item["margin_position"])
                symbol_result[symbol]["profit_real"].append(item["profit_real"])
                symbol_result[symbol]["profit_unreal"].append(item["profit_unreal"])

            symbol_result[symbol] = pd.DataFrame(symbol_result[symbol])
            symbol_result[symbol].drop_duplicates(['timestamp'], keep='last', inplace=True)
            symbol_result[symbol].drop(index=[0], inplace=True)
            time_list = symbol_result[symbol]["timestamp"]
            symbol_result[symbol].index = time_list

        return symbol_result


    def get_market_data(self):
        symbol_result = {}

        # 关键修复1：配置全局Arrow环境(禁用线程池)
        import pyarrow as pa
        # original_threads = pa.get_cpu_count()
        pa.set_cpu_count(1)  # 强制单线程模式

        for symbol in self.trading_symbols:
            try:
                symbol_result[symbol] = pd.DataFrame()
                parse_symbol = split_symbol(symbol)
                file_path = f"/srv/data/{parse_symbol['exchange']}/klines/1m/{parse_symbol['pair']}/{symbol}.parquet"
                # 关键修复：使用正确的数据访问方式
                with open(file_path, 'rb') as f:
                    buf = f.read()
                    reader = pq.ParquetFile(BytesIO(buf))
                    arrow_table = reader.read(use_threads=False)  # 获取完整Arrow Table
                    
                    results = parse_pyarrow_table(arrow_table)
                    timestamp_col = results['timestamp']

                    results['timestamp'] = timestamp_col
                    # 预计算有效索引(避免循环内判断)
                    mask = (timestamp_col >= self.config['lookback_time']) & \
                        (timestamp_col <= self.config['end_time'])
                    valid_indices = np.where(mask)[0]
                    
                    # 关键修复7：使用原生Python类型存储
                    symbol_result[symbol] = pd.DataFrame({
                        'timestamp': results['timestamp'][valid_indices],
                        'high': results['high'][valid_indices].astype('float'),
                        'low': results['low'][valid_indices].astype('float'),
                        'open': results['open'][valid_indices].astype('float'),
                        'close': results['close'][valid_indices].astype('float'),
                        'volume': results['volume'][valid_indices].astype('float')
                    }, columns=['timestamp', 'high', 'low', 'open', 'close', 'volume'])
                    symbol_result[symbol].set_index('timestamp', inplace=True)
                    print(f"Successfully loaded market data for {symbol}: {len(symbol_result[symbol])} rows")
            except Exception as e:
                print(f"Error loading market data for {symbol}: {e}")
                symbol_result[symbol] = pd.DataFrame()  # Create empty DataFrame for failed symbols

        return symbol_result


    def plot_result(self):
        """
        画图逻辑
        :return:
        """
        print('start plotting result in account')
        symbol_result = self.get_account_data()
        result_resampled = {}

        for symbol in self.trading_symbols:
            result_resampled[symbol] = pd.DataFrame()

        for symbol in self.trading_symbols:
            result_resampled[symbol]["time"] = symbol_result[symbol]["timestamp"]
            result_resampled[symbol]["margin_balance"] = symbol_result[symbol]["margin_balance"]
            # plot for each symbol
            plt.plot(result_resampled[symbol]["time"], result_resampled[symbol]["margin_balance"], label=symbol)

        if bool(int(self.config['MACD'])):
            macd = "macd" + '_' + self.config['N_Fast'] + '_' + self.config['N_Slow'] + '_' + self.config['Mark']
        else:
            macd = ""
        plt.legend(loc='upper left', title=macd)
        plt.title(datetime.now())
        plt.gcf().autofmt_xdate()
        plt.show()

    def plot_position(self):
        """
        画图逻辑
        :return:
        """
        # TODO: plot aimed at strategy
        # pass
        print('start plotting position pnl')
        symbol_result = self.get_position_data()
        result_resampled = {}
        for symbol in self.trading_symbols:
            result_resampled[symbol] = pd.DataFrame()

        plt.figure(figsize=(10, 6), dpi=80)
        for symbol in self.trading_symbols:
            result_resampled[symbol]["timestamp"] = symbol_result[symbol]["timestamp"]
            result_resampled[symbol]["total_pnl"] = symbol_result[symbol]["total_pnl"]
            # plot for each symbol
            plt.plot(result_resampled[symbol]["timestamp"], result_resampled[symbol]["total_pnl"], label=symbol)

        plt.legend(loc='upper left')
        plt.title(datetime.now())
        plt.show()

        plt.figure(figsize=(10, 6), dpi=80)
        df = pd.DataFrame()
        for symbol in self.trading_symbols:
            df = pd.merge(df, result_resampled[symbol]["total_pnl"], on=['timestamp'], how='outer')
        df.columns = self.trading_symbols
        total_pnl = pd.Series()
        for symbol in self.trading_symbols:
            total_pnl += df[symbol]
        plt.plot(df.index, total_pnl, color='green', label='total_pnl')
        plt.title(datetime.now())
        plt.grid(True)
        plt.gcf().autofmt_xdate()
        plt.show()

    def plot_kline(self):
        """
        Plot Price K-Line
        :param ma_line: bool
        :param df: pandas DataFrame with DataFrameIndex and columns: open, high, low, close, volume
        :param save_fig: bool
        :return: Plots
        """
        print('start plotting kline')

        data = self.get_market_data()
        # print(df)

        for symbol in self.trading_symbols:
            # if symbol == 'BitMEX_BTCUSD_perpetual':
            df = data[symbol]
            df.index = df['timestamp']
            fig = plt.figure(figsize=(18, 10))
            ax = fig.add_axes([0.04, 0.28, 0.94, 0.7])
            ax2 = fig.add_axes([0.04, 0.08, 0.94, 0.2])
            # mpf.candlestick2_ohlc(ax, df['open'], df['high'], df['low'], df['close'],
            #                       width=1, colorup='g', colordown='r', alpha=0.6)
            ax.set_xticks(range(0, len(df.index), len(df.index) // 15))
            # if ma_line:
            #     sma_15 = talib.SMA(numpy.array(df['close']), 15)
            #     sma_30 = talib.SMA(numpy.array(df['close']), 30)
            #     ax.plot(sma_15, label='MA15')
            #     ax.plot(sma_30, label='MA30')
            #     ax.legend(loc='upper left')
            ax.grid(True)
            # mpf.volume_overlay(ax2, df['open'], df['close'], df['volume'], colorup='g', colordown='r', width=1,
            #                    alpha=0.5)
            ax2.set_xticks(range(0, len(df.index), len(df.index) // 15))
            ax2.set_xticklabels(df.index[::len(df.index) // 15].format('%Y-%m-%d'), rotation=30)
            # if save_fig:
            #     plt.savefig(f'../image/kline_{"_".join(df["ticket"][0].split("|"))}.svg', bbox_inches='tight', dpi=500)
            ax2.grid(True)
            plt.title(symbol)
            plt.show()

    def create_default_metrics(self, initial_value):
        """
        Create default metrics for symbols without trading data
        Use initial funding value to create a "no trading" metric set
        """
        # Create a Series with only two points, representing unchanged account value
        start_date = pd.to_datetime(self.config['lookback_time'])
        end_date = pd.to_datetime(self.config['end_time'])
        dates = pd.date_range(start=start_date, end=end_date, periods=2)
        
        # Create a Series with fixed value
        flat_equity = pd.Series([initial_value, initial_value], index=dates)
        
        # Return default metrics
        return {
            "total_return": 0.0,
            "annual_return": 0.0,
            "annual_volatility": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "sortino_ratio": 0.0,
            "calmar_ratio": 0.0,
            "omega_ratio": np.nan,
            "win_rate": 0.0,
            "profit_loss_ratio": np.nan,
            "skewness": 0.0,
            "kurtosis": 0.0
        }

    def plot_performance(self):
        """Generate comprehensive PDF performance report with empty data handling"""
        # Prepare data
        print("Starting to generate backtest report...")
        data = self.get_position_data()
        
        # Get market data once for all symbols
        print("Loading market data for all symbols...")
        market_data = self.get_market_data()
        
        df = pd.DataFrame()
        metrics = {}
        
        # Track valid and invalid symbols
        valid_symbols = []
        empty_symbols = []
        
        # Process data for each symbol
        for symbol in self.trading_symbols:
            try:
                # Check if the symbol has enough data
                if symbol in data and len(data[symbol]) >= 2:  # Need at least 2 data points for metrics
                    clean_data = data[symbol].drop_duplicates(subset=['timestamp'], keep='last')
                    
                    # 确保index是timestamp且格式正确
                    if 'timestamp' in clean_data.columns:
                        clean_data.index = pd.to_datetime(clean_data['timestamp'])
                    else:
                        # 如果index已经是timestamp，确保是datetime类型
                        clean_data.index = pd.to_datetime(clean_data.index)
                    
                    # 添加调试信息
                    print(f"Processing {symbol}:")
                    print(f"  Data shape: {clean_data.shape}")
                    print(f"  Index type: {type(clean_data.index)}")
                    print(f"  Index range: {clean_data.index.min()} to {clean_data.index.max()}")
                    
                    # 选择需要的列
                    pnl_columns = ['hedge_pnl', 'funding_pnl', 'position_pnl', 'total_pnl']
                    symbol_data = clean_data[pnl_columns]
                    
                    # 使用 add 方法进行安全的累加，基于timestamp index对齐
                    if df.empty:
                        df = symbol_data.copy()
                        df.index.name = 'timestamp'  # 确保index名称一致
                    else:
                        df = df.add(symbol_data, fill_value=0)
                    
                    # Calculate metrics - corrected calculation logic
                    if not clean_data['total_pnl'].empty:
                        df[f'{symbol}_total_pnl'] = clean_data['total_pnl']
                        # Calculate metrics using initial capital for single symbol
                        cumulative_pnl = df[f'{symbol}_total_pnl'].cumsum()
                        equity_curve = self.init_account + cumulative_pnl
                        metrics[symbol] = strategy_metrics(equity_curve)
                        valid_symbols.append(symbol)
                        print(f"  Successfully processed {symbol}")
                    else:
                        print(f"Warning: total_pnl for {symbol} is empty")
                        empty_symbols.append(symbol)
                        metrics[symbol] = self.create_default_metrics(self.init_account)
                else:
                    # Record symbols with insufficient data
                    print(f"Warning: {symbol} has insufficient data")
                    empty_symbols.append(symbol)
                    metrics[symbol] = self.create_default_metrics(self.init_account)
            except Exception as e:
                print(f"Error processing {symbol}: {str(e)}")
                import traceback
                traceback.print_exc()
                empty_symbols.append(symbol)
                metrics[symbol] = self.create_default_metrics(self.init_account)
        
        # Handle portfolio data
        if df.empty:
            # Create empty DataFrame if no valid data
            print("Warning: No valid trading data, creating empty DataFrame")
            dates = pd.date_range(
                start=pd.to_datetime(self.config['lookback_time']), 
                end=pd.to_datetime(self.config['end_time']), 
                periods=2
            )
            df = pd.DataFrame(
                {
                    'hedge_pnl': [0, 0], 
                    'funding_pnl': [0, 0], 
                    'position_pnl': [0, 0], 
                    'total_pnl': [0, 0]
                }, 
                index=dates
            )
        
        # Calculate portfolio metrics - corrected calculation logic
        if valid_symbols:
            # Calculate portfolio metrics using total initial capital for all symbols
            # Each symbol uses init_account, so total portfolio initial capital = init_account * number of symbols
            total_initial_capital = self.init_account * len(self.trading_symbols)
            cumulative_total_pnl = df['total_pnl'].cumsum()
            portfolio_equity = total_initial_capital + cumulative_total_pnl
            metrics['portfolio'] = strategy_metrics(portfolio_equity)
            print(f"Calculating portfolio metrics: Based on {len(valid_symbols)} valid symbols out of {len(self.trading_symbols)} total symbols")
            print(f"Total initial capital: {total_initial_capital} (= {self.init_account} * {len(self.trading_symbols)})")
            print(f"Portfolio total PnL range: {df['total_pnl'].sum():.2f}")
        else:
            # Use default metrics if all symbols have no data
            total_initial_capital = self.init_account * len(self.trading_symbols)
            metrics['portfolio'] = self.create_default_metrics(total_initial_capital)
            print("Warning: All symbols have insufficient data, using default portfolio metrics")
        
        # Prepare output
        output_dir = f"./bt_result/{self.config['user']}/{self.config['bt_time']}"
        os.makedirs(output_dir, exist_ok=True)
        report_path = f"{output_dir}/{self.config['user']}#{self.config['strategy_name']}#{self.config['bt_time']}.pdf"
        
        # Print statistics
        print(f"Symbol statistics: {len(valid_symbols)} valid, {len(empty_symbols)} empty")
        if empty_symbols:
            print(f"Symbols with no data: {', '.join(empty_symbols)}")
        
        # Print key metrics
        if 'portfolio' in metrics:
            port_metrics = metrics['portfolio']
            print(f"Portfolio key metrics:")
            print(f"  Total Return: {port_metrics['total_return']:.2%}")
            print(f"  Annual Return: {port_metrics['annual_return']:.2%}")
            print(f"  Sharpe Ratio: {port_metrics['sharpe_ratio']:.2f}")
            print(f"  Max Drawdown: {port_metrics['max_drawdown']:.2%}")
            print(f"  Win Rate: {port_metrics['win_rate']:.2%}")
        
        with PdfPages(report_path) as pdf:
            # Create summary page
            self._create_summary_page(pdf, metrics['portfolio'], df)
            
            # Create pages for valid symbols
            if valid_symbols:
                for idx, symbol in enumerate(valid_symbols):
                    self._create_symbol_page(pdf, symbol, metrics[symbol], df, market_data)
            
            # Create page for symbols with no data
            if empty_symbols:
                self._create_empty_data_page(pdf, empty_symbols)

        print(f"Report successfully generated: {report_path}")

    def _create_empty_data_page(self, pdf, empty_symbols):
        """Create a page for symbols with no trading data"""
        fig = plt.figure(figsize=(21, 29.7))  # A4 size
        plt.suptitle("Symbols With No Trading Data", fontsize=16, y=0.95)
        
        # Create text explanation
        text = (
            f"The following {len(empty_symbols)} symbols do not have sufficient trading data:\n\n" + 
            "\n".join([f"• {symbol}" for symbol in empty_symbols]) +
            "\n\nPossible reasons:\n" +
            "• No trading activity during the backtest period\n" +
            "• Error in data retrieval\n" +
            "• Symbol filtered or not used in the strategy"
        )
        
        # Add text to page
        plt.figtext(0.1, 0.5, text, fontsize=12, va='center', ha='left')
        
        # Save page
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        pdf.savefig(fig)
        plt.close()

    def _create_summary_page(self, pdf, metrics, df):
        """Create portfolio summary page with empty data handling"""
        fig = plt.figure(figsize=(21, 29.7))  # A4 size
        gs = GridSpec(12, 2, figure=fig)
        
        # Cumulative returns plot
        ax1 = fig.add_subplot(gs[0:5, :])
        if not df.empty and not df['total_pnl'].isnull().all():
            df['total_pnl'].cumsum().plot(ax=ax1, color='#1f77b4', lw=2)
        else:
            # Draw a straight line if data is empty
            x = [0, 1]
            y = [self.init_account, self.init_account]
            ax1.plot(x, y, color='#1f77b4', lw=2)
            ax1.text(0.5, 0.5, 'No Trading Data', ha='center', va='center', transform=ax1.transAxes, fontsize=14)
            
        ax1.set_title('Portfolio Cumulative PnL', fontsize=14, pad=20)
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(True, alpha=0.3)
        
        # Key metrics table
        ax2 = fig.add_subplot(gs[5:7, 1])
        self._create_metric_table(ax2, [
            ('Annual Return', metrics['annual_return'], 'percentage'),
            ('Sharpe Ratio', metrics['sharpe_ratio'], 'float'),
            ('Max Drawdown', metrics['max_drawdown'], 'percentage'),
            ('Win Rate', metrics['win_rate'], 'percentage')
        ], 'Key Metrics')
        
        # Risk metrics table
        ax3 = fig.add_subplot(gs[7:9, 1])
        self._create_metric_table(ax3, [
            ('Sortino Ratio', metrics['sortino_ratio'], 'float'),
            ('Calmar Ratio', metrics['calmar_ratio'], 'float'),
            ('Omega Ratio', metrics['omega_ratio'], 'float'),
            ('Volatility', metrics['annual_volatility'], 'percentage')
        ], 'Risk Metrics')
        
        # Component breakdown
        ax4 = fig.add_subplot(gs[5:9, 0])
        for col in ['hedge_pnl', 'funding_pnl', 'position_pnl']:
            if not df.empty and not df[col].isnull().all():
                df[col].cumsum().plot(ax=ax4, alpha=0.8)
            else:
                # Draw straight lines for empty data
                x = [0, 1]
                y = [0, 0]
                ax4.plot(x, y, alpha=0.8, label=col)
                
        ax4.set_title('Component Performance Breakdown', fontsize=12)
        ax4.tick_params(axis='x', rotation=45)
        ax4.legend(loc='upper left')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()

    def _create_symbol_page(self, pdf, symbol, metrics, df, market_data):
        """Create individual symbol page with empty data handling"""
        fig = plt.figure(figsize=(21, 29.7))
        gs = GridSpec(12, 2, figure=fig)
        
        # Cumulative returns
        ax1 = fig.add_subplot(gs[0:5, :])
        if f'{symbol}_total_pnl' in df.columns and not df[f'{symbol}_total_pnl'].isnull().all():
            df[f'{symbol}_total_pnl'].cumsum().plot(ax=ax1, color='#2ca02c')
        else:
            # Handle empty data
            x = [0, 1]
            y = [self.init_account, self.init_account]
            ax1.plot(x, y, color='#2ca02c')
            ax1.text(0.5, 0.5, 'No Trading Data', ha='center', va='center', transform=ax1.transAxes, fontsize=14)
            
        ax1.set_title(f'{symbol} Cumulative PnL', fontsize=14, pad=20)
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(True, alpha=0.3)
        
        # Price series
        ax2 = fig.add_subplot(gs[5:8, :])
        try:
            if symbol in market_data and not market_data[symbol].empty and 'close' in market_data[symbol].columns and not market_data[symbol]['close'].empty:
                market_data[symbol]['close'].plot(ax=ax2, color='#9467bd')
            else:
                ax2.text(0.5, 0.5, 'No Price Data', ha='center', va='center', transform=ax2.transAxes, fontsize=14)
        except Exception as e:
            print(f"Error plotting price data for {symbol}: {e}")
            ax2.text(0.5, 0.5, 'Price Data Loading Error', ha='center', va='center', transform=ax2.transAxes, fontsize=14)
        
        ax2.set_title(f'{symbol} Price Series', fontsize=12, pad=20)
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
        
        # Full metrics table
        ax3 = fig.add_subplot(gs[8:, :])
        self._create_full_metrics_table(ax3, metrics)
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()

    def _create_index_page(self, pdf, symbols):
        """Create index/toc page"""
        fig = plt.figure(figsize=(21, 29.7))
        plt.suptitle("Report Index", y=0.95, fontsize=16)
        
        rows = min(10, len(symbols))
        gs = GridSpec(rows, 1, figure=fig)
        
        for i, symbol in enumerate(symbols):
            ax = fig.add_subplot(gs[i, 0])
            ax.text(0.5, 0.5, f"{i+1}. {symbol}",
                   ha='center', va='center', fontsize=12)
            ax.axis('off')
        
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()

    def _create_metric_table(self, ax, data, title):
        """Create standardized metric table"""
        ax.axis('off')
        formatted_data = []
        for name, value, fmt in data:
            formatted_data.append([name, self._format_value(value, fmt)])
        
        table = ax.table(
            cellText=formatted_data,
            colLabels=['Metric', 'Value'],
            colColours=['#f0f0f0', '#ffffff'],
            cellLoc='center',
            loc='center'
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        ax.set_title(title, fontsize=12, pad=20)

    def _create_full_metrics_table(self, ax, metrics):
        """Create detailed metrics table with proper row highlighting"""
        ax.axis('off')
        
        # Metric group configuration
        metric_groups = [
            ('Return Analysis', [
                ('Annual Return', metrics['annual_return'], 'percentage'),
                ('Sharpe Ratio', metrics['sharpe_ratio'], 'float'),
                ('Sortino Ratio', metrics['sortino_ratio'], 'float'),
                ('Calmar Ratio', metrics['calmar_ratio'], 'float')
            ]),
            ('Risk Metrics', [
                ('Max Drawdown', metrics['max_drawdown'], 'percentage'),
                ('Volatility', metrics['annual_volatility'], 'percentage'),
                ('Skewness', metrics['skewness'], 'float'),
                ('Kurtosis', metrics['kurtosis'], 'float')
            ]),
            ('Performance Statistics', [
                ('Win Rate', metrics['win_rate'], 'percentage'),
                ('P/L Ratio', metrics['profit_loss_ratio'], 'float'),
                ('Omega Ratio', metrics['omega_ratio'], 'float')
            ])
        ]

        # Build table data
        cell_text = []
        for group_name, items in metric_groups:
            cell_text.append([group_name, ''])  # Group header row
            for name, value, fmt in items:
                cell_text.append([name, self._format_value(value, fmt)])

        # Create table with header
        table = ax.table(
            cellText=cell_text,
            colLabels=['Category', 'Value'],
            colColours=['#e0e0e0', '#f5f5f5'],
            cellLoc='center',
            loc='center',
            colWidths=[0.4, 0.3]
        )

        # Apply group header styling
        for row_idx in range(len(cell_text)):
            if cell_text[row_idx][1] == '':  # Identify group headers
                # Adjust for header row (+1) and 0-based indexing
                cell = table.get_celld()[(row_idx + 1, 0)]
                cell.set_facecolor('#d9ead3')
                cell.get_text().set_fontweight('bold')
                cell.get_text().set_fontsize(12)

        # Configure table appearance
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.5)
        ax.set_title('Performance Metrics Breakdown', pad=20)


    def _format_value(self, value, fmt_type):
        """Uniform value formatting"""
        if fmt_type == 'percentage':
            return f"{value:.2%}"
        if fmt_type == 'float':
            return f"{value:.2f}"
        return str(value)


    def __plot_performance(self):
        """
        Plot Series and Cumulative Series
        :return: Plots
        """
        # df includes all pnl data, for example: hedge_pnl, funding_pnl, position_pnl, total_pnl
        print('plot performance for all traded symbols')
        # curve = series.to_frame('total')
        # curve['return'] = curve['total'].pct_change()
        # curve['curve'] = (1.0 + curve['return']).cumprod()
        data = self.get_position_data()
        df = pd.DataFrame()
        # column_count = df.shape[1]

        bt_metrics = {}
        for symbol in self.trading_symbols:
            if len(data[symbol]) > 0:
                tmp = data[symbol].drop_duplicates(subset=['timestamp'],keep='last')
                if len(df) == 0:
                    df['hedge_pnl'] = tmp['hedge_pnl']
                    df['funding_pnl'] = tmp['funding_pnl']
                    df['position_pnl'] = tmp['position_pnl']
                    df['total_pnl'] = tmp['total_pnl']
                else:
                    df['hedge_pnl'] += tmp['hedge_pnl']
                    df['funding_pnl'] += tmp['funding_pnl']
                    df['position_pnl'] += tmp['position_pnl']
                    df['total_pnl'] += tmp['total_pnl']
                
                if len(tmp['total_pnl']) > 0:
                    df[f'{symbol}_total_pnl'] = tmp['total_pnl']
                    bt_metrics[symbol] = strategy_metrics(df[f'{symbol}_total_pnl'].cumsum() + self.init_account)
        
        bt_metrics['aggregate'] = strategy_metrics(df['total_pnl'].cumsum() + self.init_account*len(bt_metrics))

        plt_num = 2 * df.shape[1] + len(self.trading_symbols)
        column_count = df.shape[1]
        plt.figure(figsize=(50, 15 * column_count))

        for i in range(column_count):
            plt.subplot(plt_num, 1, 2 * i + 1)
            np.cumsum(df.iloc[:, i]).plot(lw=2.5)
            plt.title(df.columns.values[i] + ' Curve')
            plt.xlabel(df.index.name)
            plt.ylabel('Cumulative ' + df.columns.values[i])
            plt.legend()
            plt.grid(True)

            plt.subplot(plt_num, 1, 2 * i + 2)
            df.iloc[:, i].plot(lw=2.5)
            plt.title(df.columns.values[i])
            plt.xlabel(df.index.name)
            plt.ylabel(df.columns.values[i])
            plt.legend()
            plt.grid(True)

        df = self.get_market_data()
        column_count *= 2
        for symbol in self.trading_symbols:
            column_count += 1
            plt.subplot(plt_num, 1, column_count)
            df[symbol].loc[:, "close"].plot(lw=2.5)
            plt.xlabel(symbol)
            plt.ylabel('price')
            plt.grid(True)
        
        if not os.path.exists(f"./bt_result/{self.config['user']}/"):
            os.makedirs(f"./bt_result/{self.config['user']}/")

        plt.savefig(f"./bt_result/{self.config['user']}/{self.config['user']}#{self.config['strategy_name']}#{self.config['bt_time']}.png", bbox_inches='tight', dpi=500)


    def plot_pos(self, df):
        """
        :param df: pandas DataFrame with DataFrameIndex and any column to plot
        :return: Plots
        """
        # df includes position for all symbols, for example: bitmex_ethusd_perpetual
        # curve = series.to_frame('total')
        # curve['return'] = curve['total'].pct_change()
        # curve['curve'] = (1.0 + curve['return']).cumprod()
        column_count = df.shape[1]
        plt.figure(figsize=(18, 4 * column_count))
        for i in range(column_count):
            plt.subplot(column_count, 1, i + 1)
            plt.plot(df.iloc[:, i], lw=0.5)
            plt.title(df.columns.values[i] + ' Position Curve')
            plt.xlabel(df.index.name)
            plt.ylabel(df.columns.values[i])
            plt.legend()
            plt.grid(True)

        plt.show()

    def plot_beta(self, df):
        """
        :param df: pandas DataFrame with DataFrameIndex and any column to plot
        :return: Plots
        """
        # df includes beta exposed by each symbol
        # curve = series.to_frame('total')
        # curve['return'] = curve['total'].pct_change()
        # curve['curve'] = (1.0 + curve['return']).cumprod()
        column_count = 2
        plt.figure(figsize=(18, 7 * column_count))
        for i in range(column_count):
            plt.subplot(column_count, 1, i + 1)
            plt.plot(df.iloc[:, i], lw=0.5)
            plt.plot(df.iloc[:, i * 2 + 2], lw=1.5)
            plt.plot(df.iloc[:, i * 2 + 3], lw=1.5)
            plt.title(df.columns.values[i] + ' Beta Curve')
            plt.xlabel(df.index.name)
            plt.ylabel(df.columns.values[i])
            plt.legend()
            plt.grid(True)

        plt.show()


    def plot_price(self):
        df = self.get_market_data()
        fig, ax1 = plt.subplots(figsize=(15, 6))
        color = 'tab:red'
        ax1.set_xlabel('datetime')
        ax1.set_ylabel('BitMEX_ETHUSD_perpetual', color=color)
        # data_bm = np.diff(df['BitMEX_ETHUSD_perpetual'].loc[:, "close"]) / df['BitMEX_ETHUSD_perpetual'].iloc[:-1, 4]
        df['BitMEX_ETHUSD_perpetual'].loc[:, "close"].plot(axes=ax1, color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
        color = 'tab:blue'
        ax2.set_ylabel('BitMEX_BTCUSD_perpetual', color=color)  # we already handled the x-label with ax1
        # data_hb = np.diff(df['BitMEX_BTCUSD_perpetual'].loc[:, "close"]) / df['BitMEX_BTCUSD_perpetual'].iloc[:-1, 4]
        df['BitMEX_BTCUSD_perpetual'].loc[:, "close"].plot(axes=ax2, color=color)
        ax2.tick_params(axis='y', labelcolor=color)
        plt.title('Price Series')

        # plt.legend(loc='upper center')
        plt.grid(True)
        plt.gcf().autofmt_xdate()
        plt.show()

        # (data_bm - data_hb).plot()
        # plt.title(datetime.now())
        # plt.gcf().autofmt_xdate()
        # plt.show()

    def close(self, event):
        pass


if __name__ == "__main__":
    ee = Event_Engine()

    # log = LogEngine(event_engine=ee)
    plot = PlotEngine(event_engine=ee)

    # log_data = LOGDATA("Test Log Engine", log_level=logging.INFO)
    # event = LOG_EVENT(log_data)
    event = STOP_EVENT()

    ee.start()
    ee.send_event(event)

    p = PositionEngine(ee)
    # log模块测试通过
