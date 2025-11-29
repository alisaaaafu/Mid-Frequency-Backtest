# encoding=utf-8
"""
Author: Wamnzhen Fu
Refactored to support CLI and function call.
"""
import argparse
from importlib import import_module
import json
from datetime import datetime, timedelta
import warnings
import random
import os
import numpy as np

warnings.filterwarnings("ignore", category=UserWarning, message="use an explicit session")
random.seed(42)
np.random.seed(42)
os.environ['PYTHONHASHSEED'] = '42'

def build_config(config_path: str = "./config.json") -> tuple:
    with open("cfg.json", 'r') as f:
        CFG = json.load(f)

    with open(config_path, 'r') as f:
        full_cfg = json.load(f)

    if len(full_cfg) != 1:
        raise ValueError("Config file must contain exactly one strategy key.")

    strategy_name = next(iter(full_cfg))
    cfg = full_cfg[strategy_name]
    coin = cfg['coin']
    user = cfg['user']
    symbols = cfg['futures']
    market_data = symbols.copy()
    market_data.extend(cfg['funding'])

    pos_list = []
    for symbol in symbols:
        pos_list.append({
            "Long": f"{symbol}_long",
            "Short": f"{symbol}_short"
        })

    CONFIG = {
        "coin": coin,
        "user": user,
        "start_time": datetime.strftime(datetime.strptime(cfg['start_time'], "%Y-%m-%d"), "%Y-%m-%d %H:%M:%S"),
        "end_time": datetime.strftime(datetime.strptime(cfg['end_time'], "%Y-%m-%d") + timedelta(1), "%Y-%m-%d %H:%M:%S"),
        "lookback_time": datetime.strftime(datetime.strptime(cfg['start_time'], "%Y-%m-%d") - timedelta(cfg['warmup_days']), "%Y-%m-%d %H:%M:%S"),
        'bt_time': datetime.strftime(datetime.now(), "%Y%m%d%H%M%S"),
        "strategy_name": strategy_name,
        'is_windows': cfg['is_windows'],
        "enable_mongodb": cfg['enable_mongodb'],
        "TradingSymbols": symbols,
        "FundingSymbols": cfg['funding'],
        "MARKET_DATA": market_data,
        "DB": {
            "Mongo_Host": "localhost",
            "Mongo_Port": "27017",
            "ACCOUNT_DB": f"{user}_AccountInfo-{strategy_name}",
            "ACCOUNT_COL": dict(zip(symbols, symbols)),
            "POSITION_DB": f"{user}_PositionInfo-{strategy_name}",
            "POSITION_COL": dict(zip(symbols, pos_list)),
            "ORDER_DB": f"{user}_OrderInfo-{strategy_name}",
            "ORDER_COL": dict(zip(symbols, pos_list)),
        },
        "init_account": cfg['init_account'],
        "Trade_Unit": cfg["trade_unit"],
        "Min_Unit": cfg["min_unit"],
        "Slippage": cfg['slippage']
    }

    return CONFIG, CFG

def run_strategy(config_path: str = "./config.json"):
    CONFIG, CFG = build_config(config_path)

    strategy_name = CONFIG["strategy_name"]

    try:
        module = import_module(f"Strategy.{strategy_name}")
        strategy_cls = getattr(module, strategy_name)
    except ModuleNotFoundError:
        raise ValueError(f"策略模块 Strategy.{strategy_name} 不存在！")
    except AttributeError:
        raise ValueError(f"策略类 {strategy_name} 未在模块中找到！")

    from Trade.MainEngine import MainEngine
    from Event_Engine import Event_Engine

    ee = Event_Engine()
    main_engine = MainEngine(ee, CONFIG, CFG)
    main_engine.addStrategy(strategy_cls)
    main_engine.start()


def parse_args():
    parser = argparse.ArgumentParser(description="Run trading strategy from config file")
    parser.add_argument(
        "-c", "--config", type=str, default="./config.json",
        help="Path to the strategy config file (must contain a single strategy key)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_strategy(args.config)
