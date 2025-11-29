# encoding=utf-8
"""
Author: Wamnzhen Fu
Date: 7-23
This is a test script
"""
from Trade.MainEngine import MainEngine
from research.strategy.VolatilityStrategy import testVolatilityStrategy
from Event_Engine import Event_Engine
import multiprocessing
from datetime import datetime
import itertools
import time

CPU_COUNT = multiprocessing.cpu_count() - 1


class optimizer(object):
    def __init__(self):
        self.param_dict = {}
        self.param_name = []
        self.setting_list = []
        self.results = []

        self.heatmap = {}
        self.box = {}
        self.violin = {}
        self.weave = {}
        self.dist = {}

        self.x = []
        self.y = []

    def add_parameter(self, name, start, end=None, step=None):
        if not None and not step:
            self.param_dict[name] = [start]
            return
        if end < start:
            print('wrong parameter: start > end ')
            return
        if step <= 0:
            print('Wrong step: step must be positive integer')
            return

        self.param_dict[name] = []
        param = start
        while param <= end:
            self.param_dict[name].append(param)
            param += step

        self.param_name.append(name)

    def _generate_setting(self):
        name_list = self.param_dict.keys()
        param_list = self.param_dict.values()

        # parameter setting
        product_list = list(itertools.product(*param_list))

        # parameter setting of dict
        for p in product_list:
            d = dict(zip(name_list, p))
            self.setting_list.append(d)

    def backtest(self, *args):
        # print(args)
        strategy = args[0]
        count = args[1]
        total_count = args[2]
        btc_limit = args[3]
        eth_limit = args[4]
        args = []
        print(f"[{'>' * count}{'-' * (total_count - count)}]")

        ee = Event_Engine()
        main = MainEngine(ee, btc_limit=btc_limit, eth_limit=eth_limit)

        main.addStrategy(strategy)
        main.start()

    def parallel_optimization(self, strategy):
        """
        Parameter Optimization by Multi-Processing Parallel Computing
        """
        self._generate_setting()

        start_time = datetime.now()
        pool = multiprocessing.Pool(CPU_COUNT)
        results = []
        count = 0
        all_task_count = len(self.setting_list)
        for item in self.setting_list:
            count += 1
            arg = [strategy, count, all_task_count] + list(item.values())
            results.append(pool.apply_async(self.backtest, args=arg))
            time.sleep(3)

        pool.close()
        pool.join()

        end_time = datetime.now()
        print('Optimization Time(microseconds): ', (end_time - start_time).microseconds)


if __name__ == '__main__':

    PO = optimizer()
    PO.add_parameter('btc_limit', start=0.5, end=2, step=0.5)
    PO.add_parameter('eth_limit', start=0.5, end=2, step=0.5)
    PO.parallel_optimization(testVolatilityStrategy)
