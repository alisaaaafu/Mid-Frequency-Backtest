# encoding=utf-8
"""
Author: Richard
Date: 8-18-2020
"""

from Strategy.Strategy import StrategyTemplate
from datetime import datetime
import pytz

import time
import numpy as np
import pandas as pd
import logging
from Utils.util import *
from Utils.DataStructure import *
from Utils.Event import *
from Utils.Constant import *
from collections import deque

# TODO: read this!
# Trade units can be in BTC / CONTRACTS / USD / COIN, need to be stated in config document.
# Trade unit in COIN is settled in COIN, other trade units are settle in BTC.
# Trade unit should not be changed with time: for example, trade bimex_btcusd in btc, the btc value is variable.
# symbol in bar data with the format: exchange_symbol_contractType.

def is_exact_ny_time(timestamp_str, target_hour, target_minute=0, target_second=0):
    """
    Check if the timestamp string (UTC) matches exactly the given hour:minute:second in New York time.

    Args:
        timestamp_str (str): Timestamp in "YYYY-MM-DD HH:MM:SS" format, assumed in UTC.
        target_hour (int): Target hour in New York time (0–23).
        target_minute (int): Target minute in New York time (0–59).
        target_second (int): Target second in New York time (0–59).

    Returns:
        bool: True if the New York time matches exactly, False otherwise.
    """
    dt_utc = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
    ny_time = dt_utc.astimezone(pytz.timezone("America/New_York"))
    return (ny_time.hour == target_hour and
            ny_time.minute == target_minute and
            ny_time.second == target_second)

def is_ny_weekday(timestamp_str, target_weekdays):
    """
    Check if the given UTC timestamp string falls on any of the specified weekdays in New York time.

    Args:
        timestamp_str (str): Timestamp in "YYYY-MM-DD HH:MM:SS" format (UTC).
        target_weekdays (list[int]): List of target weekdays (0=Mon, ..., 6=Sun).

    Returns:
        bool: True if the date in New York time matches any weekday in target_weekdays.
    """
    dt_utc = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
    ny_time = dt_utc.astimezone(pytz.timezone("America/New_York"))
    return ny_time.weekday() in target_weekdays

def profit_taking_stop_loss(price, open_price, profit_taking, stop_loss, long=True):
    """
    Determine whether to exit a trade based on profit taking or stop loss thresholds.

    Args:
        price (float): Current price.
        open_price (float): Entry price.
        profit_taking (float): Profit taking threshold as a percentage (e.g., 0.05 for 5%).
        stop_loss (float): Stop loss threshold as a percentage (e.g., 0.02 for 2%).
        long (bool): True if it's a long position, False for short.

    Returns:
        bool: True if exit condition is met, False otherwise.
    """
    if open_price is None:
        return False
    if long:
        if price >= open_price * (1 + profit_taking):
            return True  # take profit
        elif price <= open_price * (1 - stop_loss):
            return True  # stop loss
    else:
        if price <= open_price * (1 - profit_taking):
            return True  # take profit for short
        elif price >= open_price * (1 + stop_loss):
            return True  # stop loss for short
    return False




class strategy_overnight_01(StrategyTemplate):
    def __init__(self, config):
        super(strategy_overnight_01, self).__init__()

        self.config = config
        self.trading_symbols = self.config['TradingSymbols']  # 交易的品种
        self.MAX_PRICE_HISTORY = 1440  # Maximum length of price history to maintain
        self.min_open_amount = 10 if self.config['Trade_Unit'] == 'USD' else 0.01

        self.price = dict()
        self.open_price = dict()
        self.realized_pnls = dict()
        self.unrealized_pnls = dict()
        self.occupied_margins = dict()
        self.available_margins = dict()
        self.available_pos = dict()

        self.Bar = dict()
        self.account = None
        self.position = None

        self.pnl_rate = dict()
        self.acc_update = 1
        self.pos_update = 1
        self.min_unit = float(self.config['Min_Unit'])

    def onInit(self, **kwargs):
        """
        初始化
        """
        for symbol in self.trading_symbols:
            self.realized_pnls[symbol] = {"long": 0, "short": 0}
            self.unrealized_pnls[symbol] = {"long": 0, "short": 0}
            self.occupied_margins[symbol] = {"long": 0, "short": 0}  # 多空头寸占用的保证金,position.volume
            self.available_pos[symbol] = {"long": 0, "short": 0}
            self.available_margins[symbol] = float(self.config['init_account'])  # account.available
            self.open_price[symbol] = None

        for symbol in self.trading_symbols:
            self.Bar[symbol] = None
            self.price[symbol] = deque(maxlen=self.MAX_PRICE_HISTORY)

    def onStart(self):
        """
        策略启动
        """
        pass

    def onStop(self):
        """
        停止
        """
        pass

    def onBar(self, bar):
        """
        接收bar数据,
        bar is a dictionary {symbol: BAR_DATA}
        """
        for symbol in self.trading_symbols:
            if symbol in bar:
                self.timestamp = bar[symbol].timestamp
                self.Bar[symbol] = bar[symbol]
                self.price[symbol].append(self.Bar[symbol].close)

        # one_day_range = max(self.price[symbol]) - min(self.price[symbol])
        # if one_day_range == 0:
        #     return
        # range_pct = 0.005 / (one_day_range / self.price[target][-1])

        # # Define range bounds and corresponding allocations
        # # Example: if range is 0.5%, use 60% margin; if 2%, use 20%
        # if range_pct < 0.1:
        #     margin_allocation = 0.25
        # elif range_pct < 0.2:
        #     margin_allocation = 0.15
        # elif range_pct < 0.3:
        #     margin_allocation = 0.05
        # else:
        #     margin_allocation = 0.0

        # margin_allocation = 0.2
        # alloc_margin = self.available_margins[target] * margin_allocation
        
            if len(self.price[symbol]) >= self.MAX_PRICE_HISTORY:
                # Exit
                if is_exact_ny_time(self.timestamp, 9, 30) or \
                    profit_taking_stop_loss(self.price[symbol][-1], self.open_price[symbol], 0.35, 0.2):
                    if self.available_pos[symbol]['long'] > 0:
                        ### self.available_pos[target]['long'] is in contract
                        self.executionOrder(symbol, OrderType.Limit, self.price[symbol][-1],
                                            self.available_pos[symbol]['long'], OrderAction.Sell, OrderOffset.Close,
                                            bar)
                        self.open_price[symbol] = None
                        
                # Enter
                if is_exact_ny_time(self.timestamp, 16) and is_ny_weekday(self.timestamp, [6, 0, 1, 2, 3]):
                    one_day_range = max(self.price[symbol]) - min(self.price[symbol])
                    if one_day_range == 0:
                        continue
                    range_pct = 0.005 / (one_day_range / self.price[symbol][-1])
                    if range_pct < 0.1:
                        margin_allocation = 0.25
                    elif range_pct < 0.2:
                        margin_allocation = 0.15
                    elif range_pct < 0.3:
                        margin_allocation = 0.05
                    else:
                        margin_allocation = 0.0
                    alloc_margin = self.available_margins[symbol] * margin_allocation

                    if alloc_margin > self.min_open_amount:
                        sub_symbol = symbol.split("_")[1]
                        open_contract = cal_contracts(exchange="BinanceU", symbol=sub_symbol, contract_type="perp",
                                                    trade_unit=self.config['Trade_Unit'],
                                                    price=self.price[symbol][-1], volume=alloc_margin)
                        self.executionOrder(symbol, OrderType.Limit, self.price[symbol][-1],
                                            open_contract, OrderAction.Buy, OrderOffset.Open,
                                            bar)
                        self.open_price[symbol] = self.price[symbol][-1]

    def onFunding(self, funding):
        """
        接收funding数据: 不影响仓位、beta等持仓信息,影响账户剩余可用保证金信息
        """
        pass

    def onOrder(self, orderback):
        """
        接收order回执
        """
        self.OrderBack = orderback

    def onAccount(self, account):
        """
        更新account
        """
        self.account = account
        for symbol in self.trading_symbols:
            self.available_margins[symbol] = account[symbol].margin_available

    def onPosition(self, position):
        """
        更新position
        """
        self.position = position
        for symbol in self.trading_symbols:
            self.realized_pnls[symbol]['long'] = position[symbol]['long'].tmp_real_pnl
            self.unrealized_pnls[symbol]['long'] = position[symbol]['long'].tmp_unreal_pnl

            self.realized_pnls[symbol]['short'] = position[symbol]['short'].tmp_real_pnl
            self.unrealized_pnls[symbol]['short'] = position[symbol]['short'].tmp_unreal_pnl

            self.occupied_margins[symbol]['long'] = position[symbol]['long'].volume
            self.occupied_margins[symbol]['short'] = position[symbol]['short'].volume

            self.available_pos[symbol]['long'] = position[symbol]['long'].available
            self.available_pos[symbol]['short'] = position[symbol]['short'].available

            long_pnl = self.realized_pnls[symbol]['long'] + self.unrealized_pnls[symbol]['long']
            short_pnl = self.realized_pnls[symbol]['short'] + self.unrealized_pnls[symbol]['short'] 

    def executionOrder(self, symbol, type, price, volume_in_contract, direction, offset, bar):
        """
        调用buy,cover,sell,short函数
        """
        if volume_in_contract > self.min_unit:
            self.pos_update = 0
            self.acc_update = 0
            if direction == OrderAction.Buy and offset == OrderOffset.Open:
                # self.write_log("buy open",logging.INFO)
                self.buy(symbol=symbol, type=type, price=price, volume_in_contract=volume_in_contract, bar=bar)

            elif direction == OrderAction.Sell and offset == OrderOffset.Close:
                # self.write_log("sell close", logging.INFO)
                self.sell(symbol=symbol, type=type, price=price, volume_in_contract=volume_in_contract, bar=bar)

            elif direction == OrderAction.Sell and offset == OrderOffset.Open:
                # self.write_log("sell open",logging.INFO)
                self.short(symbol=symbol, type=type, price=price, volume_in_contract=volume_in_contract, bar=bar)

            elif direction == OrderAction.Buy and offset == OrderOffset.Close:
                # self.write_log("buy close", logging.INFO)
                self.cover(symbol=symbol, type=type, price=price, volume_in_contract=volume_in_contract, bar=bar)
