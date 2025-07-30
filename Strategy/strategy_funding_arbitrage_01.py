# encoding=utf-8
"""
Author: Richard
Date: 8-18-2020
"""

from Strategy.Strategy import StrategyTemplate
from datetime import datetime
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


class strategy_funding_arbitrage_01(StrategyTemplate):
    def __init__(self, config):
        super(strategy_funding_arbitrage_01, self).__init__()

        self.config = config
        self.trading_symbols = self.config['TradingSymbols']  # 交易的品种
        self.funding_symbols = self.config['FundingSymbols']  # funding结算
        self.MAX_PRICE_HISTORY = 5000  # Maximum length of price history to maintain
        self.min_open_amount = 50 if self.config['Trade_Unit'] == 'USD' else 0.01

        self.CUMSUM_FUNDING_MA_LENGTH = 180
        self.thres = 0.05
        self.price = dict()
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


        self.usdt_perp = f"BinanceU_{self.config['coin'].upper()}USDT_perp"
        self.usdc_perp = f"BinanceU_{self.config['coin'].upper()}USDC_perp"

    def onInit(self, **kwargs):
        """
        初始化
        """
        self.funding_diff = [0]

        for symbol in self.trading_symbols:
            self.realized_pnls[symbol] = {"long": 0, "short": 0}
            self.unrealized_pnls[symbol] = {"long": 0, "short": 0}
            self.occupied_margins[symbol] = {"long": 0, "short": 0}  # 多空头寸占用的保证金,position.volume
            self.available_pos[symbol] = {"long": 0, "short": 0}
            self.available_margins[symbol] = float(self.config['init_account'])  # account.available

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

        cumsum_funding_diff = np.cumsum(self.funding_diff)
        cumsum_funding_diff_ma = np.mean(cumsum_funding_diff[-self.CUMSUM_FUNDING_MA_LENGTH:]) if len(cumsum_funding_diff) >= self.CUMSUM_FUNDING_MA_LENGTH else cumsum_funding_diff[-1]

        current_cumsum_funding_diff = cumsum_funding_diff[-1] if len(cumsum_funding_diff) > 0 else 0
        margin = (self.available_margins[self.usdt_perp] + self.available_margins[self.usdc_perp]) * 0.97 / 2

        if len(cumsum_funding_diff) >= self.CUMSUM_FUNDING_MA_LENGTH and len(self.price[self.usdt_perp]) > 0 and len(self.price[self.usdc_perp]) > 0:
            if current_cumsum_funding_diff < cumsum_funding_diff_ma * (1 - self.thres):        
                # Open new positions
                if margin > self.min_open_amount and self.available_pos[self.usdt_perp]['short'] == 0 and self.available_pos[self.usdc_perp]['long'] == 0:
                    # Open USDT long
                    usdt_open_contract = cal_contracts(exchange="BinanceU", symbol=f"{self.config['coin'].upper()}USDT", contract_type="perp",
                                                  trade_unit=self.config['Trade_Unit'],
                                                  price=self.price[self.usdt_perp][-1], volume=margin)
                    # Open USDC short
                    usdc_open_contract = cal_contracts(exchange="BinanceU", symbol=f"{self.config['coin'].upper()}USDC", contract_type="perp",
                                                  trade_unit=self.config['Trade_Unit'],
                                                  price=self.price[self.usdc_perp][-1], volume=margin)
                    
                    open_contract = min(usdt_open_contract, usdc_open_contract)
                    self.executionOrder(self.usdt_perp, OrderType.Limit, self.price[self.usdt_perp][-1],
                                        open_contract, OrderAction.Buy, OrderOffset.Open,
                                        self.Bar)
                    
                    self.executionOrder(self.usdc_perp, OrderType.Limit, self.price[self.usdc_perp][-1],
                                        open_contract, OrderAction.Sell, OrderOffset.Open,
                                        self.Bar)

            elif current_cumsum_funding_diff > cumsum_funding_diff_ma * (1 + self.thres):
                # Open new positions
                if margin > self.min_open_amount and self.available_pos[self.usdc_perp]['short'] == 0 and self.available_pos[self.usdt_perp]['long'] == 0:
                    # Open USDC long
                    usdc_open_contract = cal_contracts(exchange="BinanceU", symbol=f"{self.config['coin'].upper()}USDC", contract_type="perp",
                                                  trade_unit=self.config['Trade_Unit'],
                                                  price=self.price[self.usdc_perp][-1], volume=margin)
                    # Open USDT short
                    usdt_open_contract = cal_contracts(exchange="BinanceU", symbol=f"{self.config['coin'].upper()}USDT", contract_type="perp",
                                                  trade_unit=self.config['Trade_Unit'],
                                                  price=self.price[self.usdt_perp][-1], volume=margin)
                    open_contract = min(usdt_open_contract, usdc_open_contract)

                    self.executionOrder(self.usdc_perp, OrderType.Limit, self.price[self.usdc_perp][-1],
                                        open_contract, OrderAction.Buy, OrderOffset.Open,
                                        self.Bar)
                    
                    self.executionOrder(self.usdt_perp, OrderType.Limit, self.price[self.usdt_perp][-1],
                                        open_contract, OrderAction.Sell, OrderOffset.Open,
                                        self.Bar)
                    

    def onFunding(self, funding):
        """
        接收funding数据: 不影响仓位、beta等持仓信息,影响账户剩余可用保证金信息
        funding is a dictionary {funding_symbol: FUNDING_DATA}
        """
        if f"Funding_{self.usdt_perp}" in funding and f"Funding_{self.usdc_perp}" in funding:
            diff = funding[f"Funding_{self.usdt_perp}"].funding_rate  - funding[f"Funding_{self.usdc_perp}"].funding_rate
            self.funding_diff.append(diff)

        cumsum_funding_diff = np.cumsum(self.funding_diff)
        cumsum_funding_diff_ma = np.mean(cumsum_funding_diff[-self.CUMSUM_FUNDING_MA_LENGTH:]) if len(cumsum_funding_diff) >= self.CUMSUM_FUNDING_MA_LENGTH else cumsum_funding_diff[-1]

        current_cumsum_funding_diff = cumsum_funding_diff[-1] if len(cumsum_funding_diff) > 0 else 0

        if len(cumsum_funding_diff) >= self.CUMSUM_FUNDING_MA_LENGTH and len(self.price[self.usdt_perp]) > 0 and len(self.price[self.usdc_perp]) > 0:
            if current_cumsum_funding_diff < cumsum_funding_diff_ma * (1 - self.thres):
                # Close existing positions first
                if self.available_pos[self.usdt_perp]['short'] > self.min_unit:
                    self.executionOrder(self.usdt_perp, OrderType.Limit, self.price[self.usdt_perp][-1],
                                        self.available_pos[self.usdt_perp]['short'], OrderAction.Buy, OrderOffset.Close,
                                        self.Bar)
                    self.executionOrder(self.usdc_perp, OrderType.Limit, self.price[self.usdc_perp][-1],
                                        self.available_pos[self.usdc_perp]['long'], OrderAction.Sell, OrderOffset.Close,
                                        self.Bar)

            elif current_cumsum_funding_diff > cumsum_funding_diff_ma * (1 + self.thres):
                # Close existing positions first
                if self.available_pos[self.usdc_perp]['short'] > self.min_unit:
                    self.executionOrder(self.usdc_perp, OrderType.Limit, self.price[self.usdc_perp][-1],
                                        self.available_pos[self.usdc_perp]['short'], OrderAction.Buy, OrderOffset.Close,
                                        self.Bar)
                    self.executionOrder(self.usdt_perp, OrderType.Limit, self.price[self.usdt_perp][-1],
                                        self.available_pos[self.usdt_perp]['long'], OrderAction.Sell, OrderOffset.Close,
                                        self.Bar)
        


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
