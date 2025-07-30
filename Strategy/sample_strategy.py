# encoding=utf-8

from Strategy.Strategy import StrategyTemplate
from datetime import datetime
import numpy as np
from Utils.DataStructure import *
from Utils.util import *
from Utils.Event import *
from Utils.Constant import *
from collections import deque

from TSeries.bar_series import Bar, Close
from TSeries.ma import MA
from TSeries.tseries_graph import tseries_graph

# TODO: read this!
# Trade units can be in BTC / CONTRACTS / USD / COIN, need to be stated in config document.
# Trade unit in COIN is settled in COIN, other trade units are settle in BTC.
# Trade unit should not be changed with time: for example, trade bimex_btcusd in btc, the btc value is variable.
# symbol in bar data with the format: exchange_symbol_contractType.


class sample_strategy(StrategyTemplate):
    def __init__(self, config):
        super(sample_strategy, self).__init__()

        self.config = config
        self.trading_symbols = self.config['TradingSymbols']  # 交易的品种
        self.MAX_PRICE_HISTORY = 5000  # Maximum length of price history to maintain

        self.price = dict()
        self.bar_series = {}
        self.close_series = {}
        self.ma_1500 = {}

        self.realized_pnls = dict()
        self.unrealized_pnls = dict()
        self.occupied_margins = dict()
        self.available_margins = dict()
        self.available_pos = dict()

        self.Bar = dict()

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

        for symbol in self.trading_symbols:
            self.Bar[symbol] = None
            self.price[symbol] = deque(maxlen=self.MAX_PRICE_HISTORY)
            self.bar_series[symbol] = Bar(f"{symbol}_bar_series")
            self.close_series[symbol] = Close(self.bar_series[symbol])
            self.ma_1500[symbol] = MA(self.close_series[symbol], period=1500, name="MA1500")

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
        target = 'BinanceU_BTCUSDT_perp'
        for symbol in self.trading_symbols:
            if symbol in bar:
                self.timestamp = bar[symbol].timestamp
                self.Bar[symbol] = bar[symbol]
                self.price[symbol].append(self.Bar[symbol].close)
                self.bar_series[symbol].update(bar[symbol], self.timestamp)
                tseries_graph.update_all(self.timestamp)
        
        if len(self.price[target]) >= 5000:
            if self.close_series[target].value < self.ma_1500[target].value:
                if self.available_pos[target]['long'] > self.min_unit:
                    ### self.available_pos[target]['long'] is in contract
                    self.executionOrder(target, OrderType.Limit, self.close_series[target].value,
                                        self.available_pos[target]['long'], OrderAction.Sell, OrderOffset.Close,
                                        bar)

            if self.close_series[target].value > np.max(list(self.price[target])[-3000:-1]):
                if self.available_margins[target]*0.9 > self.min_unit:
                    ### convert to contract
                    open_contract = cal_contracts(exchange="BinanceU", symbol='BTCUSDT', contract_type="perp",
                                                  trade_unit=self.config['Trade_Unit'],
                                                  price=self.close_series[target].value, volume=self.available_margins[target]*0.9)
                    self.executionOrder(target, OrderType.Limit, self.close_series[target].value,
                                        open_contract, OrderAction.Buy, OrderOffset.Open,
                                        bar)

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
        for symbol in self.trading_symbols:
            self.available_margins[symbol] = account[symbol].margin_available

    def onPosition(self, position):
        """
        更新position
        """
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
