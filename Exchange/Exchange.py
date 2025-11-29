# encoding=utf-8
"""
Author: Wamnzhen Fu
Date: 7-9-2020
"""
import logging
import random
import time
import os
from multiprocessing import Process, Queue
import pyarrow.parquet as pq
import pyarrow as pa  # 添加pyarrow主模块导入
import pandas as pd
from io import BytesIO
import numpy as np  # 预加载numpy避免延迟导入问题
import atexit
from datetime import datetime, timedelta
from abc import abstractmethod, ABC
from logging import INFO
from threading import Thread
from Event_Engine import Event_Engine
from Utils.Constant import *
from Utils.Event import *
from Utils.decorator_functions import thread
from Utils.DataStructure import *
from Utils.util import *
import csv


class ExchangeBase(object):
    """
    交易所基类,可以继承多种不同的交易所,采用不同的撮合方式
    另外开启一个进程来模拟虚拟交易所推送数据
    """

    def __init__(self):
        self.__active = False
        self.ask_trees: dict
        self.bid_trees: dict
        self.order_book: dict
        self.receiving_orders: ORDERBOOK
        self.__Redis_Host = None
        self.__Redis_Port = None
        self.__Redis_Channel = None

    def on_init(self):
        """
        初始化交易所
        """
        pass

    def _run(self):
        """
        交易所开始工作
        """
        pass

    def start(self):
        """
        交易所启动
        """
        pass

    @thread
    def _publish_data(self):
        """
        向symbol对应的channel推送市场数据
        """
        pass

    def on_orders_arrived(self, event):
        """
        订单到达
        """
        pass

    @abstractmethod
    def on_match(self, order):
        """
        订单撮合
        """
        raise NotImplementedError("function on_match() is not implemented")

    def write_log(self, msg: str, level: int = INFO):
        """
        写日志
        """
        log = LOGDATA(log_content=msg,
                      log_level=level)
        event = LOG_EVENT(log)
        self.event_manager.send_event(event)

    def on_close(self, event):
        """
        停止
        """
        self.__active = False


class Exchange_Backtest_Medium_Frequency(ExchangeBase):
    """
    中频回测系统,基于Bar数据
    """

    def __init__(self, ee, is_windows, config, cfg):
        super(Exchange_Backtest_Medium_Frequency, self).__init__()

        self.config = config
        self.cfg = cfg

        self.BarData = dict()
        self.FundingData = dict()
        self.__Windows = is_windows
        # 不再创建线程，而是在start方法中直接调用_run方法
        # self.__MainThread = Thread(target=self._run)

        self.data_source = {}
        self.market_data_symbols = self.config['MARKET_DATA']
        self.trading_symbols = self.config['TradingSymbols']
        self.funding_symbols = self.config['FundingSymbols']
        self.tmp = {}  # 缓存每个symbol的market data
        self.flag = {}

        self.spot = self.cfg['CONTRACT_TYPE']['SPOT']

        self.event_manager = ee
        self.slippage = float(self.config['Slippage'])

        self.register_function()

    def write_log(self, msg: str, level: int = INFO):
        """
        写日志
        """
        log = LOGDATA(log_content=msg,
                     log_level=level)
        event = LOG_EVENT(log)
        self.event_manager.send_event(event)

    def on_close(self, event):
        """
        停止
        """
        super().on_close(event)  # 调用父类方法设置__active为False

    def on_init(self):
        """
        初始化交易所
        """
        if self.__Windows:
            # win system default settings
            from ctypes import windll
            timeBeginPeriod = windll.winmm.timeBeginPeriod
            timeBeginPeriod(1)

        # 不再在初始化时创建生成器,仅初始化数据结构
        for symbol in self.market_data_symbols:
            # 初始化data_source占位符
            self.data_source[symbol] = None
                
            # 初始化不同symbol的bar数据
            if (symbol.find('Funding')) < 0:
                # 初始化不同symbol的bar数据
                self.BarData[symbol] = BAR(symbol=symbol, timestamp=None,
                                         open=0, high=0, low=0, close=0, volume=0, quote_volume=0,
                                         count=0, taker_buy_volume=0,
                                         taker_buy_quote_volume=0)
            else:
                # 初始化不同symbol的funding数据
                self.FundingData[symbol] = FUNDING(symbol=symbol, timestamp=None, funding_rate=0)

            # 初始化缓存
            self.tmp[symbol] = None
            self.flag[symbol] = 1

    def start(self):
        self.__active = True
        self.on_init()
        # 单线程模式，直接运行_run方法，不再启动线程
        self._run()

    def _run(self):
        """
        交易所开始工作 - 单线程模式
        """
        if self.__active:
            # 延迟创建生成器到实际需要时
            for market_symbol in self.market_data_symbols:
                if self.data_source[market_symbol] is None:
                    try:
                        mk_gen = self.__parquet_reader_generator(market_symbol)
                        self.data_source[market_symbol] = mk_gen
                    except Exception as e:
                        print(f"创建{market_symbol}生成器失败: {e}")
            # 开始推送数据 - 单线程模式直接调用
            self._publish_data()

    def register_function(self):
        self.event_manager.register(Event_Type.EVENT_BUY, self.on_orders_arrived)
        self.event_manager.register(Event_Type.EVENT_SELL, self.on_orders_arrived)
        self.event_manager.register(Event_Type.EVENT_COVER, self.on_orders_arrived)
        self.event_manager.register(Event_Type.EVENT_SHORT, self.on_orders_arrived)
        # self.event_manager.register(Event_Type.EVENT_CANCEL_ORDER, self.on_cancel)
        # self.event_manager.register(Event_Type.EVENT_CANCEL_ALL, self.on_cancel_all)


    def __parquet_reader_generator(self, market_symbol: str):
        parse_symbol = split_symbol(market_symbol)
        file_path = (
            f"/srv/data/{parse_symbol['exchange']}/funding/{market_symbol}.parquet"
            if parse_symbol['tag'] == 'funding'
            else f"/srv/data/{parse_symbol['exchange']}/klines/1m/{parse_symbol['pair']}/{market_symbol}.parquet"
        )

        # 关键修复：使用正确的数据访问方式
        with open(file_path, 'rb') as f:
            buf = f.read()
            reader = pq.ParquetFile(BytesIO(buf))
            arrow_table = reader.read(use_threads=False)  # 获取完整Arrow Table
            
            results = parse_pyarrow_table(arrow_table)
            if parse_symbol['tag'] == 'funding':
                # 检查timestamp的类型,并正确处理
                sample_ts = results['timestamp'][0]
                
                # 根据类型选择正确的处理方式
                if isinstance(sample_ts, (np.int64, np.int32, int, str)):
                    # 如果是整数时间戳,无需转换
                    timestamp_col = results['timestamp']
                elif isinstance(sample_ts, np.datetime64):
                    # 如果是numpy datetime,转换为字符串
                    timestamp_col = np.array([pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S") 
                                             for ts in results['timestamp']])
                elif isinstance(sample_ts, datetime):
                    # 如果是Python datetime,转换为字符串
                    timestamp_col = np.array([ts.strftime("%Y-%m-%d %H:%M:%S") 
                                             for ts in results['timestamp']])
                else:
                    # 其他类型,尝试直接转换为字符串
                    print(f"未知类型的timestamp: {type(sample_ts)},尝试直接使用")
                    timestamp_col = results['timestamp']
            else:
                # 非funding数据直接使用原始timestamp
                timestamp_col = results['timestamp']
            
            results['timestamp'] = timestamp_col
            # 预计算有效索引(避免循环内判断)
            mask = (timestamp_col >= self.config['lookback_time']) & \
                (timestamp_col <= self.config['end_time'])
            valid_indices = np.where(mask)[0]
            
            # 批量生成结果
            for idx in valid_indices:
                pub_data = {}
                for col in arrow_table.column_names:
                    pub_data[col] = results[col][idx]
                yield pub_data

        # 显式清理
        del arrow_table, reader, buf
        import gc
        gc.collect()

    def __csv_reader_generator(self, market_symbol):
        """
        Fetch data & generator
        """
        with open(f"./Preprocess/{market_symbol}.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row_dt = row['timestamp']
                    if self.config['lookback_time'] <= row_dt <= self.config['end_time']:
                        yield row
                        
                    if row_dt > self.config['end_time']:
                        break
                        
                except (KeyError, ValueError) as e:
                    print(f"Skip invalid data: {e}")
                    continue

    def _publish_data(self):
        """
        推送Bar数据 & Funding数据 - 单线程模式
        """
        timer = {}

        st = None
        while 1:
            # 单线程模式下不再检查队列大小，总是处理
            try:
                for symbol in self.market_data_symbols:
                    if self.flag[symbol] == 1 and self.tmp[symbol] is None:
                        self.tmp[symbol] = next(self.data_source[symbol])

                    timer[symbol] = self.tmp[symbol]['timestamp']

                min_value = min(timer.values())
                least_timer_symbols = [k for k, v in timer.items() if v == min_value]

                if st is not None:
                    if st > min_value:
                        break
                
                publish_data = {k: self.tmp[k] for k in least_timer_symbols if k in self.tmp}
                
                self.update_bar_data(publish_data)
                st = min_value

                # includes funding data
                BAR_Event = BAR_EVENT(publish_data)
                self.event_manager.send_event(BAR_Event)

                # 更新缓存
                for symbol in self.market_data_symbols:
                    if symbol in least_timer_symbols:
                        self.flag[symbol] = 1
                        self.tmp[symbol] = None
                    else:
                        self.flag[symbol] = 0

            except StopIteration:
                stop = STOP_EVENT()
                self.event_manager.send_event(stop)
                break

    def update_bar_data(self, msg):
        """
        更新bar数据
        """
        for symbol in self.trading_symbols:
            if symbol in msg:
                self.BarData[symbol].timestamp = msg[symbol]['timestamp']
                self.BarData[symbol].open = float(msg[symbol]['open'])
                self.BarData[symbol].high = float(msg[symbol]['high'])
                self.BarData[symbol].low = float(msg[symbol]['low'])
                self.BarData[symbol].close = float(msg[symbol]['close'])
                self.BarData[symbol].volume = float(msg[symbol]['volume'])

                self.BarData[symbol].quote_volume = float(msg[symbol]['quote_volume'])
                self.BarData[symbol].count = float(msg[symbol]['count'])
                self.BarData[symbol].taker_buy_volume = float(msg[symbol]['taker_buy_volume'])
                self.BarData[symbol].taker_buy_quote_volume = float(msg[symbol]['taker_buy_quote_volume'])
        for symbol in self.funding_symbols:
            if symbol in msg:
                self.FundingData[symbol].timestamp = msg[symbol]['timestamp']
                self.FundingData[symbol].funding_rate = float(msg[symbol]['fundingRate'])
                # self.FundingData[symbol].next_funding_rate = float(msg['next_funding_rate'])

    def on_orders_arrived(self, event):
        """
        收到订单
        """
        order = event.data
        symbol = order.symbol
        order_timestamp = order.timestamp
        bar = order.bar[symbol]
        if order_timestamp >= bar.timestamp:
            # if order.time1 > 990 or order.time2 > 990:
            #     print(symbol, order_timestamp, order.time1, order.time2)
            self.on_match(order)

    def on_match(self, order: ORDER):
        """
        在medium frequency backtest里面,只考虑在close以market订单的形式成交
        """
        symbol = order.symbol
        bar = order.bar[symbol]
        time_ = order.timestamp
        exchange, signal, contract_type = symbol.split('_')

        direction = order.direction
        if contract_type != self.spot:
            if "USDC" in signal:
                symbol_base = "usdc"
            elif "USDT" in signal:
                symbol_base = "usdt"

            fee_rate = self.cfg["FEES"][exchange]['future'][symbol_base][order.orderType.value]
        else:
            fee_rate = self.cfg["FEES"][exchange]['spot'][order.orderType.value]

        OrderBack = ORDERBACK(timestamp=time_, symbol=symbol, volume=0,
                              volume_in_contract=0, 
                              price=order.price, orderType=order.orderType, direction=direction,
                              status=None, order_id=order.order_id, trade_volume=0,
                              trade_volume_in_contract=0,
                              traded_avg_price=0, fee=0, offset=order.offset, last_price=0)

        if direction == OrderAction.Buy:
            traded_vol_in_contract = order.volume_in_contract
            avg_price = bar.close * (1 + self.slippage)

            volume_in_trade_unit = cal_value_in_trade_unit(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                       traded_vol_in_contract, avg_price)  # convert contract to trade unit
            OrderBack.volume = volume_in_trade_unit  # in trade_unit
            OrderBack.volume_in_contract = traded_vol_in_contract
            OrderBack.traded_avg_price = avg_price
            OrderBack.trade_volume = volume_in_trade_unit  # in trade_unit 
            OrderBack.trade_volume_in_contract = traded_vol_in_contract
            OrderBack.fee = cal_value_in_trade_unit(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                       fee_rate*traded_vol_in_contract, avg_price)  # in trade_unit 
            OrderBack.last_price = bar.close
            OrderBack.status = OrderStatus.AllTraded

        elif direction == OrderAction.Sell:
            traded_vol_in_contract = order.volume_in_contract
            avg_price = bar.close * (1 - self.slippage)
            volume_in_trade_unit = cal_value_in_trade_unit(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                       traded_vol_in_contract, avg_price)  # convert contract to trade unit
            OrderBack.volume = volume_in_trade_unit  # in trade_unit
            OrderBack.volume_in_contract = traded_vol_in_contract
            OrderBack.traded_avg_price = avg_price
            OrderBack.trade_volume = volume_in_trade_unit  # in trade_unit
            OrderBack.trade_volume_in_contract = traded_vol_in_contract
            OrderBack.fee = cal_value_in_trade_unit(exchange, signal, contract_type, self.config['Trade_Unit'],
                                                       fee_rate*traded_vol_in_contract, avg_price)  # in trade_unit 
            OrderBack.last_price = bar.close
            OrderBack.status = OrderStatus.AllTraded

        if OrderBack.status is not None:
            OrderBack_Event = ORDERBACK_EVENT(data=OrderBack)
            self.event_manager.send_event(OrderBack_Event)
        # else:
            # self.write_log("order matching error", logging.ERROR)

    def on_cancel(self):
        """
        medium frequency trading暂时不需要实现该功能
        """
        pass

    def on_cancel_all(self):
        """
        medium frequency trading暂时不需要实现该功能
        """
        pass


class Exchange_RealTime_Simulator(ExchangeBase, ABC):
    """
    链接真实市场数据进行测试
    """

    def __init__(self):
        super(Exchange_RealTime_Simulator, self).__init__()

        pass


