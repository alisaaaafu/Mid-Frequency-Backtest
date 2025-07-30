import random
import numpy as np
import pandas as pd
from deap import base, creator, tools, algorithms
from typing import List, Callable, Union, Dict, Set
import sys
sys.path.append("..")  # 添加上级目录到Python路径
from Research.operators import OperatorSuite
import warnings
warnings.filterwarnings("ignore")

class SignalMiner:
    def __init__(self, data: pd.DataFrame, exclude_columns: Set[str] = {'timestamp'}):
        """
        初始化信号挖掘器
        
        Args:
            data: 输入数据DataFrame,可以包含OHLCV及其他特征列
            exclude_columns: 不参与特征挖掘的列名集合
        """
        self.data = data
        self.ops = OperatorSuite()
        self.feature_columns = [col for col in data.columns if col not in exclude_columns]
        self.setup_deap()
        
        # 定义可用的基础运算符
        self.basic_operators = {
            'add': lambda x, y: x + y,
            'sub': lambda x, y: x - y,
            'mul': lambda x, y: x * y,
            'div': lambda x, y: x / y if isinstance(y, (int, float)) and y != 0 else x / (y + 1e-8),
            'abs': lambda x: np.abs(x),
            'log': lambda x: np.log(np.abs(x) + 1e-8),
            'sign': lambda x: np.sign(x),
            'power': lambda x, n: np.sign(x) * (np.abs(x) ** n),
            'rank': lambda x: x.rank(pct=True),  # 截面百分比排名
            'zscore': lambda x: (x - x.mean()) / (x.std() + 1e-8),  # 标准化
            'sigmoid': lambda x: 1 / (1 + np.exp(-x))  # Sigmoid变换
        }
        
        # 定义 OperatorSuite 中的时间序列运算符
        self.ts_operators = {
            'ts_max': self.ops.ts_max,
            'ts_argmax': self.ops.ts_argmax,
            'ts_min': self.ops.ts_min,
            'ts_argmin': self.ops.ts_argmin,
            'ts_rank': self.ops.ts_rank,
            'delay': self.ops.delay,
            'correlation': self.ops.correlation,
            'covariance': self.ops.covariance,
            'variance': self.ops.variance,
            'scale': self.ops.scale,
            'stddev': self.ops.stddev,
            'decay_linear': self.ops.decay_linear,
            'delta': self.ops.delta,
            'signedpower': self.ops.signedpower,
            'prod': self.ops.product,
        }

        # 定义窗口大小选项
        self.window_options = [5, 10, 20, 30, 60, 120, 240]

    def setup_deap(self):
        """设置 DEAP 遗传算法环境"""
        # 定义适应度类
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        # 定义个体类
        creator.create("Individual", list, fitness=creator.FitnessMax)
        
        self.toolbox = base.Toolbox()
        # 注册遗传算法操作
        self.toolbox.register("expr_init", self._generate_expression)
        self.toolbox.register("individual", tools.initIterate, creator.Individual, self.toolbox.expr_init)
        self.toolbox.register("population", tools.initRepeat, list, self.toolbox.individual)
        
        # 注册遗传算法操作符
        self.toolbox.register("evaluate", self._evaluate)
        self.toolbox.register("mate", tools.cxTwoPoint)
        self.toolbox.register("mutate", self._mutate)
        self.toolbox.register("select", tools.selTournament, tournsize=3)

    def _generate_expression(self) -> List:
        """生成一个随机的信号表达式"""
        expr = []
        max_depth = 3
        
        def grow(depth):
            if depth >= max_depth:
                # 到达最大深度，返回基础特征列
                return random.choice(self.feature_columns)
            
            # 随机选择操作符
            if random.random() < 0.7:  # 70% 概率选择操作符
                op = random.choice(list(self.basic_operators.keys()) + list(self.ts_operators.keys()))
                if op in self.basic_operators:
                    if op in ['abs', 'log', 'sign', 'rank', 'zscore', 'sigmoid']:
                        return [op, grow(depth + 1)]
                    else:
                        return [op, grow(depth + 1), grow(depth + 1)]
                else:  # 时间序列操作符
                    window = random.choice(self.window_options)
                    return [op, grow(depth + 1), window]
            else:
                return random.choice(self.feature_columns)
        
        expr = grow(0)
        return expr

    def _evaluate(self, individual: List) -> tuple:
        """评估信号表达式的性能"""
        try:
            signal = self._execute_expression(individual)
            if signal is None or signal.isnull().all() or signal.isna().all():
                return (-np.inf,)
            
            # 移除无效值
            signal = signal.replace([np.inf, -np.inf], np.nan)
            signal = signal.fillna(method='ffill').fillna(method='bfill')
            
            if signal.isnull().any() or signal.std() == 0:
                return (-np.inf,)
            
            # 计算信号与未来收益的IC值
            if 'close' in self.data.columns:
                future_returns = self.data['close'].pct_change(1).shift(-1)
            else:
                future_returns = self.data[self.feature_columns[0]].pct_change(1).shift(-1)
            
            # 确保未来收益有效
            future_returns = future_returns.replace([np.inf, -np.inf], np.nan)
            future_returns = future_returns.fillna(method='ffill').fillna(method='bfill')
            
            # 使用非空值计算相关性
            valid_mask = ~(signal.isna() | future_returns.isna())
            if valid_mask.sum() < 10:  # 要求至少10个有效观测
                return (-np.inf,)
                
            ic = signal[valid_mask].corr(future_returns[valid_mask])
            if np.isnan(ic):
                return (-np.inf,)
                
            # 计算信息比率 (IR)
            rolling_ic = signal.rolling(20).corr(future_returns)
            ir = rolling_ic.mean() / (rolling_ic.std() + 1e-8)
            
            # 计算信号的自相关性惩罚项
            autocorr = signal.autocorr(lag=1)
            penalty = abs(autocorr) if not np.isnan(autocorr) else 1
            
            # 综合得分 = IC + IR - 自相关性惩罚
            # 对所有指标使用 tanh 进行归一化，避免极端值
            score = np.tanh(ic) + 0.5 * np.tanh(ir) - 0.3 * penalty
            
            return (score,)
        except Exception as e:
            # print(f"评估出错: {str(e)}")  # 用于调试
            return (-np.inf,)

    def _execute_expression(self, expr: List) -> pd.Series:
        """执行信号表达式并返回结果"""
        try:
            if isinstance(expr, str):  # 基础特征列
                return self.data[expr]
            
            op = expr[0]
            if op in self.basic_operators:
                if len(expr) == 2:  # 单参数操作符
                    x = self._execute_expression(expr[1])
                    if x is None or x.isnull().all():
                        return None
                    # 对无效值进行处理
                    x = x.replace([np.inf, -np.inf], np.nan)
                    x = x.fillna(method='ffill').fillna(method='bfill')
                    return self.basic_operators[op](x)
                else:  # 双参数操作符
                    x = self._execute_expression(expr[1])
                    y = self._execute_expression(expr[2])
                    if x is None or y is None or x.isnull().all() or y.isnull().all():
                        return None
                    # 对无效值进行处理
                    x = x.replace([np.inf, -np.inf], np.nan)
                    y = y.replace([np.inf, -np.inf], np.nan)
                    x = x.fillna(method='ffill').fillna(method='bfill')
                    y = y.fillna(method='ffill').fillna(method='bfill')
                    return self.basic_operators[op](x, y)
            elif op in self.ts_operators:
                x = self._execute_expression(expr[1])
                if x is None or x.isnull().all():
                    return None
                # 对无效值进行处理
                x = x.replace([np.inf, -np.inf], np.nan)
                x = x.fillna(method='ffill').fillna(method='bfill')
                window = expr[2]
                try:
                    result = self.ts_operators[op](x, window)
                    return result
                except Exception as e:
                    # print(f"时间序列运算错误: {str(e)}")  # 用于调试
                    return None
            return None
        except Exception as e:
            # print(f"表达式执行错误: {str(e)}")  # 用于调试
            return None

    def _mutate(self, individual: List) -> tuple:
        """突变操作"""
        if random.random() < 0.3:  # 降低完全重生成的概率
            # 30% 概率完全重新生成
            individual[:] = self._generate_expression()
        else:
            # 70% 概率修改部分表达式
            def mutate_subexpr(expr):
                if isinstance(expr, str):  # 如果是特征列名
                    if random.random() < 0.2:  # 20%概率换另一个特征
                        return random.choice(self.feature_columns)
                    return expr
                
                if random.random() < 0.2:  # 20% 概率修改当前节点
                    return self._generate_expression()
                if isinstance(expr, list):
                    if len(expr) == 2:
                        expr[1] = mutate_subexpr(expr[1])
                    elif len(expr) == 3:
                        if random.random() < 0.5:
                            expr[1] = mutate_subexpr(expr[1])
                        else:
                            if isinstance(expr[2], int):  # 如果是窗口参数
                                expr[2] = random.choice(self.window_options)
                            else:
                                expr[2] = mutate_subexpr(expr[2])
                return expr
            
            individual[:] = mutate_subexpr(individual[:])
        return individual,

    def mine_signals(self, 
                    population_size: int = 50, 
                    generations: int = 30, 
                    n_best: int = 3,
                    verbose: bool = True) -> Dict:
        """
        运行遗传算法挖掘信号
        
        Args:
            population_size: 种群大小
            generations: 迭代代数
            n_best: 保留最优信号的数量
            verbose: 是否打印进度信息
            
        Returns:
            dict: 包含最优信号表达式及其性能指标
        """
        pop = self.toolbox.population(n=population_size)
        hof = tools.HallOfFame(n_best)
        
        # 记录每代的统计信息
        stats = tools.Statistics(lambda ind: ind.fitness.values)
        stats.register("avg", np.mean)
        stats.register("max", np.max)
        
        # 运行遗传算法
        pop, logbook = algorithms.eaSimple(
            pop, self.toolbox,
            cxpb=0.7,  # 交叉概率
            mutpb=0.3,  # 突变概率
            ngen=generations,
            stats=stats,
            halloffame=hof,
            verbose=verbose
        )
        
        # 返回最优结果
        best_results = []
        for expr in hof:
            signal = self._execute_expression(expr)
            future_returns = self.data['close'].pct_change(1).shift(-1) if 'close' in self.data.columns else None
            
            result = {
                'expression': expr,
                'signal': signal,
                'fitness': expr.fitness.values[0]
            }
            
            # 如果有未来收益数据，计算更多指标
            if future_returns is not None:
                ic = signal.corr(future_returns)
                ir = ic / (signal.std() + 1e-8)
                result.update({
                    'ic': ic,
                    'ir': ir,
                    'autocorr': signal.autocorr(lag=1)
                })
            
            best_results.append(result)
        
        return best_results

    def explain_expression(self, expr: List) -> str:
        """将表达式转换为可读的字符串形式"""
        if isinstance(expr, str):
            return expr
        
        op = expr[0]
        if op in self.basic_operators:
            if len(expr) == 2:
                x = self.explain_expression(expr[1])
                return f"{op}({x})"
            else:
                x = self.explain_expression(expr[1])
                y = self.explain_expression(expr[2])
                return f"{op}({x}, {y})"
        elif op in self.ts_operators:
            x = self.explain_expression(expr[1])
            window = expr[2]
            return f"{op}({x}, window={window})"
        
        return str(expr)