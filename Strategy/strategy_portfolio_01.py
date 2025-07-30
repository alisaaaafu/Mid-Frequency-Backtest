from Strategy.Strategy import StrategyTemplate
from datetime import datetime
import numpy as np
from Utils.DataStructure import *
from Utils.util import *
from Utils.Event import *
from Utils.Constant import *
from collections import deque
import pandas as pd

class strategy_portfolio_01(StrategyTemplate):
    def __init__(self, config):
        super(strategy_portfolio_01, self).__init__()
        
        # Strategy parameters
        self.config = config
        self.ma_threshold = 0.01
        self.momentum_window = {
            '4H': 18,  # 6*3 hours
            '1H': 24
        }
        self.min_momentum = 0.0
        self.momentum_reverse_window = 10
        self.open_ma_4h_window = 60
        self.open_ma_1h_window = 160
        self.exit_ma_4h_window = 60
        self.exit_ma_1h_window = 160
        self.atr_window = 24
        self.atr_threshold = 0.02
        self.trailing_stop_ratio = 0.05
        
        # Account and position tracking
        self.trading_symbols = self.config['TradingSymbols']
        self.min_unit = float(self.config['Min_Unit'])
        
        self.realized_pnls = {}
        self.unrealized_pnls = {}
        self.occupied_margins = {}
        self.available_margins = {}
        self.available_pos = {}

        # Bar data storage
        self.current_bars = {}
        self.history = {}

    
    def onInit(self, **kwargs):
        """
        初始化
        """
        for symbol in self.trading_symbols:
            self.realized_pnls[symbol] = {"long": 0, "short": 0}
            self.unrealized_pnls[symbol] = {"long": 0, "short": 0}
            self.occupied_margins[symbol] = {"long": 0, "short": 0}
            self.available_pos[symbol] = {"long": 0, "short": 0}
            self.available_margins[symbol] = float(self.config['init_account'])
        
        # Period configuration
        self.period_config = {
            '1H': {'interval': pd.Timedelta(hours=1), 'ma_window': self.open_ma_1h_window},
            '4H': {'interval': pd.Timedelta(hours=4), 'ma_window': self.open_ma_4h_window}
        }
        
        for symbol in self.trading_symbols:
            # Current bar data for each timeframe
            self.current_bars[symbol] = {
                '1H': {'open': None, 'high': -np.inf, 'low': np.inf, 'close': None, 'volume': 0, 'start': None},
                '4H': {'open': None, 'high': -np.inf, 'low': np.inf, 'close': None, 'volume': 0, 'start': None}
            }
            
            # Historical completed bars
            self.history[symbol] = {
                '1H': deque(maxlen=self.period_config['1H']['ma_window'] * 2),
                '4H': deque(maxlen=self.period_config['4H']['ma_window'] * 2)
            }
        
        # Technical indicators
        self.tr_history = {}
        self.current_atr = {}
        self.momentum_history = {}
        self.current_momentum = {}
        
        for symbol in self.trading_symbols:
            self.tr_history[symbol] = deque(maxlen=self.atr_window)
            self.current_atr[symbol] = 0.0
            
            self.momentum_history[symbol] = {
                '1H': deque(maxlen=8),
                '4H': deque(maxlen=self.momentum_window['4H'])
            }
            self.current_momentum[symbol] = {
                '1H': 0,
                '4H': 0
            }
        
        # Trade tracking
        self.active_trades = {symbol: None for symbol in self.trading_symbols}
        self.max_prices = {symbol: 0.0 for symbol in self.trading_symbols}

    def _update_ohlc(self, bar, symbol, period):
        """Update OHLC data for a specific period"""
        freq_config = self.period_config[period]
        current_bar = self.current_bars[symbol][period]
        period_start = pd.to_datetime(bar.timestamp).floor(freq_config['interval'])
        
        if current_bar['start'] != period_start:
            # Save completed period
            if current_bar['start'] is not None:
                completed_bar = {
                    'open': current_bar['open'],
                    'high': current_bar['high'],
                    'low': current_bar['low'],
                    'close': current_bar['close'],
                    'volume': current_bar['volume'],
                    'end_time': current_bar['start'] + freq_config['interval']
                }
                self.history[symbol][period].append(completed_bar)
                
                # Calculate TR (only for 1H period)
                if period == '1H' and len(self.history[symbol]['1H']) >= 2:
                    prev_bar = self.history[symbol]['1H'][-2]
                    tr = max(
                        completed_bar['high'] - completed_bar['low'],
                        abs(completed_bar['high'] - prev_bar['close']),
                        abs(completed_bar['low'] - prev_bar['close'])
                    )
                    self.tr_history[symbol].append(tr)
                    
                    # Update current ATR
                    if len(self.tr_history[symbol]) >= self.atr_window:
                        self.current_atr[symbol] = np.mean(list(self.tr_history[symbol])[-self.atr_window:]) / current_bar['close']
                    else:
                        self.current_atr[symbol] = 0.0
            
            # Initialize new period
            current_bar.update({
                'start': period_start,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume
            })
        else:
            # Update current period
            current_bar['high'] = max(current_bar['high'], bar.high)
            current_bar['low'] = min(current_bar['low'], bar.low)
            current_bar['close'] = bar.close
            current_bar['volume'] += bar.volume

    def _calculate_ma(self, symbol, period, window=None):
        """Calculate moving average for a specific timeframe"""
        if window is None:
            window = self.period_config[period]['ma_window']
        
        valid_closes = []
        for bar in self.history[symbol][period]:
            valid_closes.append(bar['close'])
        
        if self.current_bars[symbol][period]['start'] is not None:
            valid_closes.append(self.current_bars[symbol][period]['close'])
        
        if len(valid_closes) >= window:
            return np.mean(valid_closes[-window:])
        return None

    def _update_momentum(self, symbol):
        """Update momentum indicators"""
        for period in self.period_config.keys():
            completed_bars = [bar['close'] for bar in self.history[symbol][period]]
            if self.current_bars[symbol][period]['start'] is not None:
                completed_bars.append(self.current_bars[symbol][period]['close'])
            
            if len(completed_bars) >= self.momentum_window[period]:
                start_price = completed_bars[-self.momentum_window[period]]
                end_price = completed_bars[-1]
                self.current_momentum[symbol][period] = (end_price - start_price) / start_price
            else:
                self.current_momentum[symbol][period] = 0.0
            
            self.momentum_history[symbol][period].append(self.current_momentum[symbol][period])

    def _check_momentum_reversal(self, symbol):
        """Check for momentum reversal signals"""
        if len(self.momentum_history[symbol]['1H']) < self.momentum_reverse_window + 3:
            return False
        
        current = self.momentum_history[symbol]['1H'][-1]
        prev1 = self.momentum_history[symbol]['1H'][-2]
        prev2 = self.momentum_history[symbol]['1H'][-3]
        prev_avg = np.mean(list(self.momentum_history[symbol]['1H'])[-self.momentum_reverse_window-1:-1])
        
        return (current < prev1 < prev2) and (current < prev_avg)

    def _check_entry_conditions(self, symbol, bar):
        """Check entry conditions"""
        if len(self.history[symbol]['1H']) < self.open_ma_1h_window:
            return False
            
        ma4h = self._calculate_ma(symbol, '4H', self.open_ma_4h_window)
        ma1h = self._calculate_ma(symbol, '1H', self.open_ma_1h_window)
        
        if ma4h is None or ma1h is None:
            return False
        
        price_condition = (bar.close > ma4h * (1 + self.ma_threshold)) and \
                         (bar.close > ma1h * (1 + self.ma_threshold))
        
        momentum_condition = self.current_momentum[symbol]['4H'] > self.min_momentum
        volatility_condition = self.current_atr[symbol] <= self.atr_threshold
        
        volume_condition = True
        if len(self.history[symbol]['4H']) >= 6:
            last_6_volumes = [bar['volume'] for bar in list(self.history[symbol]['4H'])[-6:]]
            avg_volume = np.mean(last_6_volumes)
            current_volume = self.history[symbol]['4H'][-1]['volume']
            volume_condition = current_volume > avg_volume * 1.1
        
        return price_condition and momentum_condition and volatility_condition and volume_condition

    def _check_exit_conditions(self, symbol, bar):
        """Check exit conditions"""
        if not self.active_trades[symbol]:
            return False
        
        # Time condition - only check at the start of each hour
        time_condition = pd.to_datetime(bar.timestamp).minute == 0
        
        ma4h_fast = self._calculate_ma(symbol, '4H', self.exit_ma_4h_window)
        ma1h_fast = self._calculate_ma(symbol, '1H', self.exit_ma_1h_window)
        
        if ma4h_fast is None or ma1h_fast is None:
            return False
        
        # MA conditions
        ma_condition = (bar.close < ma4h_fast) or (bar.close < ma1h_fast)
        
        # Stop loss
        stop_loss = self.active_trades[symbol]['entry_price'] * 0.95
        stop_condition = bar.close < stop_loss
        
        # Calculate current profit
        profit_pct = (bar.close - self.active_trades[symbol]['entry_price']) / self.active_trades[symbol]['entry_price']
        
        # Dynamic trailing stop (only check at hour start)
        trailing_stop_condition = False
        if profit_pct > 0.05 and time_condition:
            self.max_prices[symbol] = max(self.max_prices[symbol], bar.close)
            dynamic_threshold = self.trailing_stop_ratio + min(0.1, profit_pct * 0.3)
            trailing_stop_condition = bar.close < self.max_prices[symbol] * (1 - dynamic_threshold)
        
        # Momentum reversal
        momentum_condition = self._check_momentum_reversal(symbol)
        
        # Final exit condition
        # Stop loss can trigger anytime, other conditions need to meet time condition
        return stop_condition or ((ma_condition or trailing_stop_condition or momentum_condition) and time_condition)

    def onBar(self, bar):
        """Process incoming bar data"""
        for symbol in self.trading_symbols:
            if symbol not in bar:
                continue
            
            current_bar = bar[symbol]
            
            # Update OHLC data
            for period in ['1H', '4H']:
                self._update_ohlc(current_bar, symbol, period)
            
            # Update momentum
            self._update_momentum(symbol)
            
            # Check for exit conditions first
            if self.active_trades[symbol] and self._check_exit_conditions(symbol, current_bar):
                close_size = self.available_pos[symbol]['long']
                
                if close_size > self.min_unit:
                    self.executionOrder(
                        symbol,
                        OrderType.Limit,
                        current_bar.close,
                        close_size,
                        OrderAction.Sell,
                        OrderOffset.Close,
                        bar
                    )
                    self.active_trades[symbol] = None
                    self.max_prices[symbol] = 0.0
            
            # Check for entry conditions
            if not self.active_trades[symbol] and self._check_entry_conditions(symbol, current_bar):
                if self.available_margins[symbol] * 0.9 > self.min_unit:
                    # Convert available margin to contract size
                    open_contract = cal_contracts(
                        exchange=symbol.split('_')[0],
                        symbol=symbol.split('_')[1],
                        contract_type=symbol.split('_')[2],
                        trade_unit=self.config['Trade_Unit'],
                        price=current_bar.close,
                        volume=self.available_margins[symbol] * 0.9
                    )
                    
                    if open_contract > self.min_unit:
                        self.active_trades[symbol] = {
                            'entry_price': current_bar.close,
                            'entry_time': pd.to_datetime(current_bar.timestamp)
                        }
                        self.max_prices[symbol] = current_bar.close
                        
                        self.executionOrder(
                            symbol,
                            OrderType.Limit,
                            current_bar.close,
                            open_contract,
                            OrderAction.Buy,
                            OrderOffset.Open,
                            bar
                        )

    def onFunding(self, funding):
        """Process funding rate updates"""
        pass

    def onOrder(self, orderback):
        """Process order updates"""
        self.OrderBack = orderback

    def onAccount(self, account):
        """Process account updates"""
        for symbol in self.trading_symbols:
            self.available_margins[symbol] = account[symbol].margin_available

    def onPosition(self, position):
        """Process position updates"""
        for symbol in self.trading_symbols:
            self.realized_pnls[symbol]['long'] = position[symbol]['long'].tmp_real_pnl
            self.unrealized_pnls[symbol]['long'] = position[symbol]['long'].tmp_unreal_pnl
            self.realized_pnls[symbol]['short'] = position[symbol]['short'].tmp_real_pnl
            self.unrealized_pnls[symbol]['short'] = position[symbol]['short'].tmp_unreal_pnl
            
            self.occupied_margins[symbol]['long'] = position[symbol]['long'].volume
            self.occupied_margins[symbol]['short'] = position[symbol]['short'].volume
            
            self.available_pos[symbol]['long'] = position[symbol]['long'].available
            self.available_pos[symbol]['short'] = position[symbol]['short'].available

    def executionOrder(self, symbol, type, price, volume_in_contract, direction, offset, bar):
        """Execute trading orders"""
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