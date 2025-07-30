import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
sys.path.append("..")  # Add the parent directory to the Python path
from Research.single_factor_analysis import SignalAnalyzer
from Research.signal_miner import SignalMiner

# Test Parameters
TEST_SYMBOL = "BTCUSDT"
TEST_LENGTH = 4000


import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_crypto_synthetic_data(symbol: str, start_date: str, num_points: int) -> pd.DataFrame:
    """
    生成合成加密货币K线数据
    参数：
    symbol: 交易对符号 (如BTCUSDT)
    start_date: 起始日期 (格式'YYYY-MM-DD')
    num_points: 需要生成的数据点数(1分钟K线)
    """
    # 生成时间序列
    base_time = datetime.strptime(start_date, "%Y-%m-%d")
    timestamps = [base_time + timedelta(minutes=i) for i in range(num_points)]
    
    # 生成基础价格序列（几何布朗运动模型）
    np.random.seed(42)
    returns = np.random.normal(loc=0.0001, scale=0.005, size=num_points)
    price_series = 30000 * np.cumprod(1 + returns)  # 初始价格设为30000
    
    # 生成OHLC数据
    df = pd.DataFrame(index=timestamps)
    df['close'] = price_series
    
    # 生成波动幅度（基于真实市场特征）
    daily_volatility = np.random.uniform(0.005, 0.03, num_points)
    
    df['high'] = df['close'] * (1 + daily_volatility/2)
    df['low'] = df['close'] * (1 - daily_volatility/2)
    df['open'] = df['close'].shift(1).fillna(df['close'][0])
    
    # 处理OHLC逻辑关系
    for i in range(1, len(df)):
        df.iloc[i, df.columns.get_loc('high')] = max(
            df.open.iloc[i],
            df.close.iloc[i],
            df.high.iloc[i]
        )
        df.iloc[i, df.columns.get_loc('low')] = min(
            df.open.iloc[i],
            df.close.iloc[i],
            df.low.iloc[i]
        )
    
    # 生成成交量数据（带自相关性）
    base_volume = np.abs(np.random.normal(500, 200, num_points))
    spike_days = np.random.choice(num_points, size=int(num_points*0.1), replace=False)
    base_volume[spike_days] *= 5  # 随机交易量峰值
    
    df['volume'] = np.round(base_volume + np.random.poisson(50, num_points), 2)
    df['quote_volume'] = df['volume'] * df['close']
    
    # Taker买入量（与价格变动相关）
    df['taker_buy_volume'] = np.round(
        df['volume'] * 0.4 * (1 + (df['close'].pct_change() > 0).astype(int) * 0.3),
        2
    )
    
    # 计算衍生指标
    df['price_range'] = df['high'] - df['low']
    df['volume_ratio'] = df['taker_buy_volume'] / (df['volume'] + 1e-8)  # 防止除零
    df['avg_price'] = (df['high'] + df['low'] + df['close']) / 3
    
    # 添加时间戳列
    df['timestamp'] = df.index.astype(np.int64) // 10**9
    
    # 添加噪声（使数据更真实）
    noise_columns = ['high', 'low', 'open', 'volume']
    for col in noise_columns:
        df[col] = df[col] * np.random.normal(1, 0.001, len(df))
    
    return df


def test_signal_miner():
    # 读取数据
    df = generate_crypto_synthetic_data(
    symbol=TEST_SYMBOL,
    start_date="2024-01-01",
    num_points=TEST_LENGTH
    )

    # 初始化挖掘器
    miner = SignalMiner(df.fillna(method='ffill'), exclude_columns={'timestamp'})

    # 运行信号挖掘
    results = miner.mine_signals(
        population_size=100,  # 更大的种群
        generations=50,      # 更多的迭代次数
        n_best=5,           # 保留前5个最佳信号
        verbose=True
    )

    assert len(results) > 0, "应该至少挖掘出一个信号"
    assert all('expression' in result for result in results), "每个结果都应该包含表达式"
    assert all('signal' in result for result in results), "每个结果都应该包含信号数据"
    assert all('ic' in result for result in results), "每个结果都应该包含IC值"

    # 将挖掘出的信号组合成一个DataFrame以便分析
    signals_df = pd.DataFrame(index=df.index)
    for i, result in enumerate(results):
        signals_df[f'signal_{i+1}'] = result['signal']

    # 计算信号之间的相关性矩阵
    correlation_matrix = signals_df.corr()
    assert not correlation_matrix.isnull().all().all(), "相关性矩阵不应该全是NaN"

    # 使用SignalAnalyzer分析表现最好的信号
    best_signal = pd.DataFrame({
        'timestamp': df.timestamp,
        'close': df.close,
        'factor': results[0]['signal']  # 使用最佳信号
    })

    # 初始化分析器
    analyzer = SignalAnalyzer(
        freq='1min',      # 与数据频率一致
        roll_window=4*60, # 4小时滚动窗口
        n_strata=5        # 5分位数分层
    )

    # 执行分析
    analysis_df, stratified_df = analyzer.analyze(best_signal)

    assert isinstance(stratified_df, pd.DataFrame), "应该返回分层分析DataFrame"
    assert len(stratified_df) == 5, "应该有5个分层"
    assert 'Mean Return' in stratified_df.columns, "应该包含平均收益率"
    assert 'Hit Rate' in stratified_df.columns, "应该包含胜率"
    assert 'Samples' in stratified_df.columns, "应该包含样本数"
    
    # 检查信号质量
    assert not analysis_df['corr'].isnull().all(), "应该计算出有效的相关性"
    assert not stratified_df['Mean Return'].isnull().all(), "应该计算出有效的分层收益"
    
    # 检查数据一致性
    assert (stratified_df['Samples'] > 0).all(), "每个分层都应该有样本"
    assert (stratified_df['Hit Rate'] >= 0).all() and (stratified_df['Hit Rate'] <= 1).all(), "胜率应该在0-1之间"


def test_signal_miner_error_handling():
    """测试信号挖掘器的错误处理"""
    # 创建一个包含无效数据的DataFrame
    df = pd.DataFrame({
        'close': [1, 2, np.nan, 4, 5],
        'volume': [100, 200, 300, 400, np.inf]
    })
    
    # 测试包含无效数据的情况
    miner = SignalMiner(df)
    results = miner.mine_signals(population_size=10, generations=5, n_best=1)
    
    # 检查结果是否正确处理了无效数据
    assert len(results) <= 1, "应该最多返回n_best个结果"
    if len(results) > 0:
        assert not np.isnan(results[0]['signal']).all(), "不应该返回全是NaN的信号"