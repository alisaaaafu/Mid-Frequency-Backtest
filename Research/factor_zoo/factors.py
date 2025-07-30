import numpy as np
import pandas as pd
import sys
import os

sys.path.append("..")  # Add the parent directory to the Python path
from Research.operators import OperatorSuite as opr
from Research.factor_zoo.factor_base import FactorBase

#### factors with correlation > 0.07 will be stored here
class volatility_adjusted_momentum(FactorBase):
    def __init__(self, df: pd.DataFrame):
        """
        :param name: volatility_adjusted_momentum
        :param data: data
        """
        super().__init__('volatility_adjusted_momentum', df)

    def calculate_factor(self):
        mom = self.signal_df['close'].pct_change(10)
        vol = self.signal_df['close'].pct_change().rolling(20).std()
        self.signal_df['factor'] = mom / (vol + 1e-5)

        return self.signal_df[['timestamp', 'factor', 'close']].copy()
    

class time_decay_momentum(FactorBase):
    def __init__(self, df: pd.DataFrame):
        """
        :param name: time_decay_momentum
        :param data: data
        """
        super().__init__('time_decay_momentum', df)

    def calculate_factor(self, window=30):
        weights = np.exp(np.linspace(0, -1, window)) 
        
        def weighted_ret(series):
            return (series * weights[::-1]).sum()
        
        self.signal_df['factor'] = self.signal_df['close'].pct_change().rolling(window).apply(weighted_ret)
        
        return self.signal_df[['timestamp', 'factor', 'close']].copy()


class abnormal_trade_volume(FactorBase):
    def __init__(self, df: pd.DataFrame):
        """
        :param name: abnormal_trade_volume
        :param data: data
        """
        super().__init__('abnormal_trade_volume', df)

    def calculate_factor(self):
        indicator1 = 1.*(self.signal_df.taker_buy_volume/self.signal_df.volume >= 0.5) * self.signal_df.taker_buy_volume/self.signal_df.volume - 1.*(self.signal_df.taker_buy_volume/self.signal_df.volume < 0.5) * (1 - self.signal_df.taker_buy_volume/self.signal_df.volume)
        indicator2 = abs(self.signal_df.high - self.signal_df.low)/self.signal_df.open
        self.signal_df['factor'] = self.signal_df.volume/self.signal_df.volume.rolling(10).max() * indicator1 * indicator2
            
        return self.signal_df[['timestamp', 'factor', 'close']].copy()


class high_vol_time_decay_momentum(FactorBase):
    def __init__(self, df: pd.DataFrame):
        """
        :param name: high_vol_time_decay_momentum
        :param data: data
        """
        super().__init__('high_vol_time_decay_momentum', df)

    def calculate_factor(self, window=30):
        def time_decay_momentum(df, window=30):
            """时间衰减加权动量"""
            df = df.copy()
            weights = np.exp(np.linspace(0, -1, window))  # 指数衰减权重
            
            # 计算加权收益率
            def weighted_ret(series):
                return (series * weights[::-1]).sum()
            
            return df['close'].pct_change().rolling(window).apply(weighted_ret)
        
        ret = self.signal_df.close.pct_change()
        vol_ratio = opr.stddev(ret, 5) / opr.stddev(ret, 30)
        high_vol = (vol_ratio > 1.5).astype(float)
        
        mom = time_decay_momentum(self.signal_df, window)
        self.signal_df['factor'] = mom * (1 + high_vol)  # 高波动时放大动量
        
        return self.signal_df[['timestamp', 'factor', 'close']].copy()
    

class vol_weighted_deviation(FactorBase):
    def __init__(self, df: pd.DataFrame):
        """
        :param name: vol_weighted_deviation
        :param data: data
        """
        super().__init__('vol_weighted_deviation', df)

    def calculate_factor(self, window=30):
        avg_price = self.signal_df['close'].rolling(window).mean()
        vol_weight = self.signal_df['volume'] / (self.signal_df['volume'].rolling(window).sum() + 1e-6)
        self.signal_df['factor'] = (self.signal_df['close'] - avg_price) * vol_weight
        
        return self.signal_df[['timestamp', 'factor', 'close']].copy()


class breakout_strength(FactorBase):
    def __init__(self, df: pd.DataFrame):
        """
        :param name: breakout_strength
        :param data: data
        """
        super().__init__('breakout_strength', df)

    def calculate_factor(self):
        high_break = (self.signal_df['close'] - opr.ts_max(self.signal_df['high'], 30)) / opr.ts_max(self.signal_df['high'], 30)
        low_break = (self.signal_df['close'] - opr.ts_min(self.signal_df['low'], 30)) / opr.ts_min(self.signal_df['low'], 30)
        self.signal_df['factor'] = opr.scale(high_break + low_break)
        
        return self.signal_df[['timestamp', 'factor', 'close']].copy()
    

class path_efficiency(FactorBase):
    def __init__(self, df: pd.DataFrame):
        """
        :param name: path_efficiency
        :param data: data
        """
        super().__init__('path_efficiency', df)

    def calculate_factor(self):
        path_length = opr.ts_max(self.signal_df['high'], 10) - opr.ts_min(self.signal_df['low'], 10)
        net_change = opr.delta(self.signal_df['close'], 10)
        self.signal_df['factor'] = net_change / (path_length + 1e-6)
        
        return self.signal_df[['timestamp', 'factor', 'close']].copy()
    

class relative_volatility(FactorBase):
    def __init__(self, df: pd.DataFrame):
        """
        :param name: relative_volatility
        :param data: data
        """
        super().__init__('relative_volatility', df)

    def calculate_factor(self):
        rs_vol = opr.stddev(self.signal_df['close'] / opr.ts_max(self.signal_df['close'], 60), 20)
        self.signal_df['factor'] = opr.scale(rs_vol)
        
        return self.signal_df[['timestamp', 'factor', 'close']].copy()