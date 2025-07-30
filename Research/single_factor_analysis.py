import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr


class SignalAnalyzer:
    def __init__(self, freq='1min', roll_window=24*60, n_strata=5):
        """
        高频信号分析器(支持收益分层和相关性衰减分析)
        
        参数：
        freq : str
            数据频率(默认'1min')
        roll_window : int
            滚动分析窗口(分钟数, default=24*60)
        n_strata : int
            自动分层数量(默认5层)
        """
        self.freq = freq
        self.roll_window = roll_window
        self.n_strata = n_strata
        self.strata_labels = [f'Q{i+1}' for i in range(n_strata)]

    def preprocess_data(self, df):
        """高频数据预处理"""
        # 转换为分钟级时间序列
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.timestamp)
        df = df.resample(self.freq).last()
        
        # 处理缺失值
        df['factor'] = df['factor'].ffill().bfill()
        df['close'] = df['close'].ffill().bfill()
        return df

    def get_ret_interval(self, df, interval=10):
        """计算指定间隔的未来收益率"""
        df = df.copy()
        shifted = df.close.rolling(int(interval)).mean().shift(-int(interval))
        df['ret'] = shifted / df.close - 1
        return df

    @staticmethod
    def _rolling_corr(factor, returns, window):
        """滚动窗口的Pearson相关系数"""
        n = len(factor)
        corr = np.full(n, np.nan)
        for i in range(window, n):
            valid = ~np.isnan(factor[i-window:i]) & ~np.isnan(returns[i-window:i])
            if valid.sum() < 5: continue
            corr[i] = pearsonr(factor[i-window:i][valid], returns[i-window:i][valid])[0]
        return corr

    def _stratified_analysis(self, df):
        """执行自动分层测试"""
        df['strata'] = pd.qcut(df['factor'], q=self.n_strata, labels=self.strata_labels)
        stats = []
        
        for stratum, group in df.groupby('strata'):
            stats.append({
                'Strata': stratum,
                'Mean Return': group['ret'].mean(),
                'Hit Rate': (group['ret'] > 0).mean(),
                'Samples': len(group)
            })
        return pd.DataFrame(stats)

    def analyze(self, df, ret_decay_windows=np.linspace(1, 240, 12), corr_decay_windows=np.linspace(1, 240, 12)):
        """执行完整分析"""
        # 数据预处理
        df = self.preprocess_data(df)
        
        # 核心分析设置(默认使用10个间隔的收益率)
        df = self.get_ret_interval(df, interval=10)
        
        # 滚动相关性计算
        df['corr'] = self._rolling_corr(df['factor'].values.astype(np.float64),
                                       df['ret'].values.astype(np.float64),
                                       self.roll_window)
        
        # 计算衰减数据
        ret_decay = self.calc_ret_decay_by_quantile(
            df, 
            windows=ret_decay_windows,
            quantiles=np.linspace(0, 1, 10)  # 默认5个分位数
        )
        corr_decay = self.calc_corr_decay(df, windows=corr_decay_windows)
        
        # 累积相关性
        df['cum_corr'] = df['corr'].expanding().sum()
        
        # 日内模式分析
        df['time'] = df.index.time
        intraday_stats = df.groupby('time')['corr'].agg(['mean', 'count'])
        
        # 分层测试
        stratified_df = self._stratified_analysis(df)
        
        # 可视化(新增衰减分析)
        self._plot_results(df, intraday_stats, stratified_df, ret_decay, corr_decay,
                          ret_decay_windows, corr_decay_windows)
        
        # 打印统计结果
        self._print_stats(df, stratified_df)
        
        return df, stratified_df

    def _plot_results(self, df, intraday_stats, stratified_df, ret_decay, corr_decay,
                     ret_decay_windows, corr_decay_windows):
        """生成可视化图表(整合衰减分析)"""
        plt.figure(figsize=(20, 30))
        
        # 滚动相关性时序图
        plt.subplot(6, 1, 1)
        df['corr'].plot(title=f'Rolling {self.roll_window}min Correlation', color='steelblue')
        plt.axhline(df['corr'].mean(), color='darkorange', ls='--')
        plt.grid(alpha=0.3)
        
        # 累积相关性
        plt.subplot(6, 1, 2)
        df['cum_corr'].plot(title='Cumulative Correlation', color='forestgreen')
        plt.grid(alpha=0.3)
        
        # 分层测试结果
        plt.subplot(6, 1, 3)
        ax = stratified_df.plot(
            x='Strata', 
            y=['Mean Return', 'Hit Rate'],
            kind='bar',
            secondary_y='Hit Rate',
            color=['steelblue', 'lightcoral'],
            title='Stratified Analysis',
            ax=plt.gca()
        )
        ax.set_xticklabels(stratified_df['Strata'], rotation=45)
        ax.right_ax.set_ylim(0, 1)
        ax.right_ax.set_ylabel('Hit Rate')
        plt.grid(alpha=0.3)

        # 日内模式分析
        plt.subplot(6, 1, 4)
        time_labels = intraday_stats.index.astype(str)
        
        selected_indices = []
        selected_labels = []
        for idx, label in enumerate(time_labels):
            if pd.to_datetime(label).minute % 30 == 0:
                selected_indices.append(idx)
                selected_labels.append(label[:5])
                
        intraday_stats['mean'].plot(
            kind='bar',
            title='Intraday Correlation Pattern',
            color='mediumpurple',
            width=0.8
        )
        plt.xticks(ticks=selected_indices, labels=selected_labels, rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)

        # 收益衰减分析
        plt.subplot(6, 1, 5)
        for i in range(ret_decay.shape[1]):
            plt.plot(ret_decay_windows, ret_decay[:, i], 
                    label=f'Quantile {i+1}', 
                    alpha=0.8,
                    linewidth=2)
        plt.title('Return Decay by Quantile')
        plt.xlabel('Time Window (minutes)')
        plt.ylabel('Mean Return')
        plt.legend(bbox_to_anchor=(1.05, 0.5), loc='center left')
        plt.grid(alpha=0.3)

        # 相关性衰减分析
        plt.subplot(6, 1, 6)
        plt.plot(corr_decay_windows, corr_decay, 
                marker='o', 
                color='darkred',
                linestyle='--',
                linewidth=2)
        plt.title('Correlation Decay Over Time')
        plt.xlabel('Time Window (minutes)')
        plt.ylabel('Pearson Correlation')
        plt.grid(alpha=0.3)

        plt.tight_layout()
        plt.show()

    def calc_ret_decay_by_quantile(self, df, quantiles=np.linspace(0, 1, 11), windows=np.linspace(0, 240, 31)):
        """计算不同时间窗口的分位数收益衰减"""
        sig_decay = np.zeros((len(windows), len(quantiles)-1))
        for win_idx, win in enumerate(windows):
            df_win = self.get_ret_interval(df, interval=win)
            df_win = df_win.sort_values(by=['factor'])
            for q_idx in range(len(quantiles)-1):
                low = int(quantiles[q_idx] * len(df_win))
                high = int(quantiles[q_idx+1] * len(df_win))
                sig_decay[win_idx, q_idx] = df_win.iloc[low:high].ret.mean()
        return sig_decay

    # def calc_corr_decay(self, df, windows=np.linspace(0, 60, 31)):
    #     """计算相关性随时间衰减"""
    #     corr_decay = [np.nan]
    #     for win in windows[1:]:
    #         df_win = self.get_ret_interval(df, interval=win)
    #         valid = ~np.isnan(df_win.factor) & ~np.isnan(df_win.ret)
    #         corr = pearsonr(df_win.factor[valid], df_win.ret[valid])[0]
    #         corr_decay.append(corr)
    #     return corr_decay
    def calc_corr_decay(self, df, windows=np.linspace(0, 240, 31)):
        df = df.copy()
        df = df.dropna(how='any')
        corr_decay = [np.nan] + [pearsonr(df.factor[:-int(win)-1], self.get_ret_interval(df, win).ret[:-int(win)-1])[0] \
                        for win in windows[1:]]

        return corr_decay

    def _print_stats(self, df, stratified_df):
        """打印统计信息"""
        print("\n【Overall Statistics】")
        overall_stats = pd.Series({
            'Mean Correlation': df['corr'].mean(),
            'Corr Std': df['corr'].std(),
            'Hit Rate': (df['ret'] > 0).mean(),
            'Total Samples': len(df)
        })
        print(overall_stats.to_string(float_format=lambda x: f"{x:.4f}"))
        
        print("\n【Stratified Statistics】")
        print(stratified_df.to_string(index=False, float_format=lambda x: f"{x:.4f}"))