import pandas as pd
import numpy as np
from scipy.stats import rankdata

class OperatorSuite:
    @staticmethod
    def ts_max(series: pd.Series, window: int):
        """时间序列最大值"""
        return series.rolling(window).max()
    
    @staticmethod
    def ts_argmax(series: pd.Series, window: int):
        """最大值位置(距离当前时间)"""
        def _argmax(x):
            if len(x) < window:
                return np.nan
            idx = np.argmax(x)
            return (window - 1) - idx  # 0=当前时间,window-1=窗口最旧时间
        return series.rolling(window).apply(_argmax, raw=True)

    @staticmethod
    def ts_rank(series: pd.Series, window: int):
        """时间序列百分位排名"""
        return series.rolling(window).apply(
            lambda x: rankdata(x)[-1]/window,  # 当前值在窗口中的排名
            raw=True
        )

    @staticmethod
    def delay(series: pd.Series, periods: int):
        """滞后操作"""
        return series.shift(periods)

    @staticmethod
    def correlation(x: pd.Series, y: pd.Series, window: int):
        """滚动窗口相关系数"""
        return x.rolling(window).corr(y)

    @staticmethod
    def covariance(x: pd.Series, y: pd.Series, window: int):
        """滚动窗口协方差"""
        return x.rolling(window).cov(y)
    
    @staticmethod
    def variance(x: pd.Series, window: int):
        """滚动窗口方差"""
        return x.rolling(window).var()

    @staticmethod
    def scale(series: pd.Series, a: float = 1):
        """横截面缩放"""
        total = series.abs().sum()
        return series / total * a if total != 0 else pd.Series(0, index=series.index)

    @staticmethod
    def delta(series: pd.Series, periods: int):
        """差值计算"""
        return series.diff(periods)

    @staticmethod
    def signedpower(series: pd.Series, exponent: float):
        """带符号幂运算"""
        return np.sign(series) * (np.abs(series) ** exponent)

    @staticmethod
    def decay_linear(series: pd.Series, window: int):
        """线性衰减加权平均"""
        weights = np.arange(window, 0, -1)
        def _apply_decay(x):
            return np.dot(x, weights) / weights.sum()
        return series.rolling(window).apply(_apply_decay, raw=True)

    @staticmethod
    def stddev(series: pd.Series, window: int):
        """滚动标准差"""
        return series.rolling(window).std()

    @staticmethod 
    def ts_min(series: pd.Series, window: int):
        """时间序列最小值"""
        return series.rolling(window).min()
    
    @staticmethod 
    def ts_sum(series: pd.Series, window: int):
        """时间序列求和"""
        return series.rolling(window).sum()
    
    @staticmethod 
    def ts_mean(series: pd.Series, window: int):
        """时间序列求平均"""
        return series.rolling(window).mean()

    @staticmethod
    def product(series: pd.Series, window: int):
        """滚动乘积"""
        return series.rolling(window).apply(lambda x: np.prod(x), raw=True)

    @staticmethod
    def rank(series: pd.Series):
        """横截面排名(单资产时返回常数)"""
        return pd.Series(0.5, index=series.index)  # 单资产场景下无意义

    @staticmethod
    def indneutralize(series: pd.Series, group: pd.Series):
        """行业中性化(单资产返回原值)"""
        return series  # 单资产场景无需处理

    @staticmethod
    def ts_argmin(series: pd.Series, window: int):
        """最小值位置(距离当前时间)"""
        def _argmin(x):
            if len(x) < window:
                return np.nan
            idx = np.argmin(x)
            return (window - 1) - idx
        return series.rolling(window).apply(_argmin, raw=True)


if __name__ == '__main__':
    # 使用示例
    ops = OperatorSuite()

    # 在因子计算中调用
    def alpha_vsr(df):
        term1 = (df['low'] / ops.delay(df['high'], 1) - 1)
        term2 = ops.ts_rank(df['volume'], 30)
        term3 = np.sqrt(np.log(df['close']).rolling(10).std())
        return term1 * term2 * term3

    def calculate_factors(df):
        factors = pd.DataFrame(index=df.index)
        factors['vsr'] = alpha_vsr(df)
        factors['vol_regime'] = ops.decay_linear(df['close'], 30)
        factors['momentum'] = ops.ts_rank(df['close'], 60) - ops.ts_rank(df['close'], 10)

        return factors