# encoding: utf-8
"""
class util
Author: Cynthia
Date: 21-10-2020
Statement:
1.if trade unit in BTC / CONTRACTS / USD, settle in BTC if trade unit in COIN, settle in COIN
    contracts represents contracts
2.volume param is in trade unit
3.in cal_position_pnl: different exchange need different trade unit, for example,
    contracts * multiplier for BitMEX, contracts * contract_value for HUOBI or OKEX,

TODO: settlement_time / contract_forward(bool: true for forward, false for inverse)
TODO: complete the functions for exchanges
"""

from datetime import datetime
import pandas as pd
import numpy as np
import json


with open("cfg.json", 'r') as f:
    CFG = json.load(f)

def get_contract_multiplier(exchange, symbol, contract_type):
    if contract_type == CFG["CONTRACT_TYPE"]["SPOT"]:
        return 1

    if exchange == CFG["EXCHANGE"]["BITMEX"]:
        if symbol.startswith('BTC'):
            return 1

        if symbol.endswith('USD'):
            map = {
                "BTCUSD": 1,
                "ETHUSD": 0.000001,
                # "EOSUSD": 10,
                "XRPUSD": 0.0002,
                "LTCUSD": 0.000002,
                "BCHUSD": 0.000001,
                # TRXUSD:
            }
            if not map[symbol]:
                print('invalid symbol for {}: {}'.format(exchange, symbol))
            return map[symbol]
        return 1
    elif exchange == CFG["EXCHANGE"]["HUOBISWAP"] or exchange == CFG["EXCHANGE"]["HUOBIFUTURE"] \
            or exchange == CFG["EXCHANGE"]["HUOBI"]:
        if symbol.endswith('USDT'):
            map = {
                "BTCUSDT": 0.001,
                "ETHUSDT": 0.01,
                "BCHUSDT": 0.01,
                "BSVUSDT": 0.01,
            }
            if not map[symbol]:
                print('invalid symbol for {}: {}'.format(exchange, symbol))
            return map[symbol]
        elif symbol == CFG["SYMBOL"]["BTCUSD"]:
            return 100
        else:
            return 10
    elif (exchange == CFG["EXCHANGE"]["OKEXSWAP"] or exchange == CFG["EXCHANGE"]["OKEXFUTURE"]):
        if symbol.endswith('USDT'):
            map = {
                "BTCUSDT": 0.01,
                "ETHUSDT": 0.1,
                "EOSUSDT": 10,
                "XRPUSDT": 100,
                "LTCUSDT": 1,
                "BCHUSDT": 0.1,
                "ETCUSDT": 0.01,
                "BSVUSDT": 1,
                "TRXUSDT": 1000
            }
            if not map[symbol]:
                print('invalid symbol for {}: {}'.format(exchange, symbol))
            return map[symbol]
        elif symbol == CFG["SYMBOL"]["BTCUSD"]:
            return 100
        else:
            return 10
    elif exchange == CFG["EXCHANGE"]["BINANCEU"]:
        return 1
    elif exchange == CFG["EXCHANGE"]["BINANCEC"]:
        if symbol == CFG["SYMBOL"]["BTCUSD"]:
            return 100
        else:
            return 10
    elif exchange == CFG["EXCHANGE"]["BYBITC"]:
        return 1
    elif exchange == CFG["EXCHANGE"]["BYBITU"]:
        return 1
    elif exchange == CFG["EXCHANGE"]["FTX"]:
        return 1
    elif exchange == CFG["EXCHANGE"]["DERIBIT"]:
        if symbol == CFG["SYMBOL"]["BTCUSD"]:
            return 10
        else:
            return 1
    elif exchange == CFG["EXCHANGE"]["KRAKENFUTURE"]:
        return 1
    elif exchange == CFG["EXCHANGE"]["GATEIO"]:
        map = {
            "BTCUSDT": 0.0001,
            "ETHUSDT": 0.01,
            "LINKUSDT": 1,
            "ALGOUSDT": 10,
            "ALTUSDT": 0.001,
            "AMPLUSDT": 1,
            "ANTUSDT": 0.1,
            "ATOMUSDT": 1,
            "BANDUSDT": 0.1,
            "BCHUSDT": 0.01,
            "BNBUSDT": 0.1,
            "BSVUSDT": 0.01,
            "BTMUSDT": 10,
            "COMPUSDT": 0.01,
            "CRVUSDT": 0.1,
            "DASHUSDT": 0.01,
            "DEFIUSDT": 0.001,
            "DOGEUSDT": 1000,
            "DOTUSDT": 1,
            "EOSUSDT": 1,
            "ETCUSDT": 0.1,
            "EXCHUSDT": 0.001,
            "FILUSDT": 0.1,
            "HIVEUSDT": 1,
            "HTUSDT": 1,
            "IRISUSDT": 10,
            "LTCUSDT": 0.1,
            "NESTUSDT": 10,
            "OKBUSDT": 0.1,
            "OMGUSDT": 1,
            "ONTUSDT": 1,
            "PRIVUSDT": 0.001,
            "SRMUSDT": 1,
            "SXPUSDT": 1,
            "TRXUSDT": 100,
            "XAUGUSDT": 0.001,
            "XMRUSDT": 0.01,
            "XRPUSDT": 0.01,
            "XTZUSDT": 1,
            "ZECUSDT": 0.01,

            "BTCUSD": 1,
            "ETHUSD": 0.000001,
            "ADAUSD": 0.01,
            "BCHUSD": 0.000001,
            "BNBUSD": 0.00001,
            "BSVUSD": 0.000001,
            "BTMUSD": 0.001,
            "BTTUSD": 0.1,
            "DASHUSD": 0.000001,
            "EOSUSD": 0.0001,
            "ETCUSD": 0.0001,
            "HTUSD": 0.0001,
            "LTCUSD": 0.00001,
            "MDAUSD": 0.0001,
            "NEOUSD": 0.00001,
            "ONTUSD": 0.001,
            "TRXUSD": 0.01,
            "WAVESUSD": 0.0001,
            "XLMUSD": 0.001,
            "XMRUSD": 0.00001,
            "XRPUSD": 0.001,
            "ZECUSD": 0.000001,
            "ZRXUSD": 0.001
        }
        if not map[symbol]:
            print('invalid symbol for {}: {}'.format(exchange, symbol))
        return map[symbol]
    elif exchange == CFG["EXCHANGE"]["BITCOKE"]:
        return 1
    elif exchange == CFG["EXCHANGE"]["BITFLYER"]:
        return 1
    else:
        print('{} not implemented.'.format(exchange))


def get_settlement_time(exchange):
    if exchange == CFG["EXCHANGE"]["BITMEX"]:
        return ['04:00:00', '12:00:00', '20:00:00']
    elif exchange == CFG["EXCHANGE"]["HUOBISWAP"] or exchange == CFG["EXCHANGE"]["HUOBI"]:
        return ['00:00:00', '08:00:00', '16:00:00']
    elif exchange == CFG["EXCHANGE"]["OKEXSWAP"]:
        return ['00:00:00', '08:00:00', '16:00:00']
    elif exchange == CFG["EXCHANGE"]["BINANCEC"] or exchange == CFG["EXCHANGE"]["BINANCEU"]:
        return ['00:00:00', '08:00:00', '16:00:00']


def get_contract_forward(exchange, symbol, contract_type):
    if exchange == CFG["EXCHANGE"]["BITMEX"]:
        if symbol.startswith('BTC'):
            return False
        elif symbol.endswith('USD'):
            return True
        else:
            return '{}: {} not completed'.format(exchange, symbol)
    if exchange == CFG["EXCHANGE"]["HUOBISWAP"] or exchange == CFG["EXCHANGE"]["HUOBIFUTURE"] or exchange == \
            CFG["EXCHANGE"]["HUOBI"]:
        if symbol.endswith('USD'):
            return False
        elif symbol.endswith('USDT'):
            return True
        else:
            return '{}: {} not completed'.format(exchange, symbol)
    if exchange == CFG["EXCHANGE"]["OKEXSWAP"] or exchange == CFG["EXCHANGE"]["OKEXFUTURE"]:
        if symbol.endswith('USD'):
            return False
        elif symbol.endswith('USDT'):
            return True
        else:
            return '{}: {} not completed'.format(exchange, symbol)
    if exchange == CFG["EXCHANGE"]["BINANCEC"] or exchange == CFG["EXCHANGE"]["BINANCEU"]:
        if symbol.endswith('USD'):
            return False
        elif symbol.endswith('USDT') or symbol.endswith("USDC"):
            return True
        else:
            return '{}: {} not completed'.format(exchange, symbol)
    else:
        return '{}: {} not completed'.format(exchange, symbol)


def cal_value_in_trade_unit(exchange, symbol, contract_type, trade_unit, contract, price):
    # if trade unit in BTC/CONTRACTS/USD, return BTC_value if in coin, return COIN
    multiplier = get_contract_multiplier(exchange, symbol, contract_type)
    # btc_spot_price = get_btc_spot_price(exchange, timestamp)
    if trade_unit == "BTC":
        # return btc
        return contract
    elif trade_unit == "CONTRACTS":
        # return volume
        if exchange == CFG["EXCHANGE"]["BITMEX"]:
            if symbol.startswith('BTC'):
                return contract * 1 / price
            elif symbol.endswith('USD'):
                return contract * multiplier * price
            elif symbol.endswith('BTC'):
                return contract * price
            else:
                return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
        elif exchange == CFG["EXCHANGE"]["HUOBISWAP"] or exchange == CFG["EXCHANGE"]["HUOBIFUTURE"] \
                or exchange == CFG["EXCHANGE"]["HUOBI"] \
                or exchange == CFG["EXCHANGE"]["OKEXSWAP"] or exchange == CFG["EXCHANGE"]["OKEXFUTURE"]:
            if symbol.endswith('USDT'):
                return '{}: {}: {} trade in coin'.format(exchange, symbol, contract_type)
            else:
                # TODO: complete get_btc_spot_price, price is btc_price
                return contract * multiplier / price
                # return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
        elif exchange == CFG["EXCHANGE"]["BINANCEC"]:
            # TODO: complete get_btc_spot_price
            # return volume * multiplier / btc_spot_price
            return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
        elif exchange == CFG["EXCHANGE"]["BINANCEU"]:
            return '{}: {}: {} trade in coin'.format(exchange, symbol, contract_type)
    elif trade_unit == "USD":
        return contract * price * multiplier
    elif trade_unit == "COIN":
        # return coin
        return contract
    else:
        return 'cal_value_in_trade_unit for {}: {}: {} not completed'.format(exchange, symbol, contract_type)


def convert_quantity_to_size_in_btc(exchange, symbol, contract_type, quantity, symbol_price, btcusd_price):
    if quantity == 0:
        return 0

    identifier = exchange + '.' + symbol + '.' + contract_type
    contract_multiplier = get_contract_multiplier(exchange, symbol, contract_type)

    if identifier in CFG.QUANTO:
        return symbol_price * quantity * CFG.QUANTO[identifier]

    if symbol.endsWith('BTC'):
        return quantity * contract_multiplier * symbol_price   # including vanilla contract

    if contract_type == 'spot' or contract_type == 'ltfx':
        if not symbol.endsWith('USD'):
            return 'convert_quantity_to_size_in_btc for {} not completed'.format(symbol)
            # return quantity * symbol_price * getGlobalIndex('USD' + symbol.slice(-3)) / btcusd_price
        return quantity * symbol_price / btcusd_price   # USD quoted

    if symbol.endsWith('USDT'):
        return quantity * contract_multiplier * symbol_price / btcusd_price

    # quarter, week, month, perp
    if symbol == 'BTCUSD':
        return quantity * contract_multiplier / symbol_price
    return quantity * contract_multiplier / btcusd_price


def cal_profit_real(exchange, symbol, contract_type, trade_unit, traded_avg_price, avg_price, trade_volume_in_contract):
    # contracts = cal_contracts(exchange, symbol, contract_type, trade_unit, price, trade_volume_in_contract)
    return cal_position_pnl(exchange, symbol, contract_type, trade_unit, traded_avg_price, avg_price, trade_volume_in_contract)


def cal_position_pnl(exchange, symbol, contract_type, trade_unit, price, last_price, contracts):
    multiplier = get_contract_multiplier(exchange, symbol, contract_type)
    forward = get_contract_forward(exchange, symbol, contract_type)
    # btc_spot_price = get_btc_spot_price(exchange)
    if exchange == CFG["EXCHANGE"]["BITMEX"]:
        # return BTC
        if trade_unit == 'COIN':
            if not forward:
                return contracts * (1 / last_price - 1 / price)
            else:
                return contracts * (price - last_price) / last_price
        else:
            if not forward:
                return contracts * multiplier * (1 / last_price - 1 / price)
            else:
                return contracts * multiplier * (price - last_price)
    elif exchange == CFG["EXCHANGE"]["HUOBISWAP"] or exchange == CFG["EXCHANGE"]["HUOBIFUTURE"] or exchange == \
            CFG["EXCHANGE"]["HUOBI"] or exchange == CFG["EXCHANGE"]["OKEXSWAP"] or exchange == CFG["EXCHANGE"][
            "OKEXFUTURE"] or exchange == CFG["EXCHANGE"]["BINANCEC"] or exchange == CFG["EXCHANGE"]["BINANCEU"]:
        if trade_unit == 'BTC':
            # return BTC
            # TODO: complete get_btc_spot_price
            return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
        elif trade_unit == 'CONTRACTS':
            if not forward:
                return contracts * multiplier * (1/last_price - 1/price)
                # return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
                # return contracts * multiplier * (1 / last_price - 1 / price) * price / btc_spot_price
            else:
                return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
        elif trade_unit == 'COIN':
            if not forward:
                return contracts * multiplier * (1 / last_price - 1 / price) / last_price
            else:
                return contracts * multiplier * (price - last_price) / last_price
        elif trade_unit == 'USD':
            ### convert contract to USD
            if not forward:
                return contracts * multiplier * price * (1 / last_price - 1 / price) / last_price
            else:
                return contracts * multiplier * price * (price - last_price) / last_price
        else:
            return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
    # TODO: 若为现货,contracts则为美元价值,pnl以美元计

def cal_avg_price(exchange, symbol, contract_type, trade_unit, traded_avg_price, trade_vol_in_contract, pos_vol_in_contract, pos_avg_price):
    multiplier = get_contract_multiplier(exchange, symbol, contract_type)
    if trade_unit == "COIN":
        ### both in coin
        total_value = traded_avg_price * trade_vol_in_contract + pos_vol_in_contract * pos_avg_price
                        
        return total_value / (trade_vol_in_contract + pos_vol_in_contract)
    elif trade_unit == "USD":
        ### trade_volume and pos_volume in contract
        total_value = traded_avg_price * trade_vol_in_contract + pos_vol_in_contract * pos_avg_price
                        
        return total_value / (trade_vol_in_contract + pos_vol_in_contract)
    else:
        raise ValueError(f"Unit {trade_unit} is not supported!")


def cal_contracts(exchange, symbol, contract_type, trade_unit, price, volume):
    multiplier = get_contract_multiplier(exchange, symbol, contract_type)
    # btc_spot_price = get_btc_spot_price(exchange, symbol)
    if trade_unit == "COIN":
        return volume
    else:
        if exchange == CFG["EXCHANGE"]["BITMEX"]:
            # return contracts
            if trade_unit == "BTC":
                if symbol.startswith('BTC'):
                    return volume * price
                elif symbol.endswith('USD'):
                    return volume / (price * multiplier)
                elif symbol.endswith('BTC'):
                    return volume / price
                else:
                    return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
            elif trade_unit == "CONTRACTS":
                return volume
            elif trade_unit == "USD":
                pass
            else:
                return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
        elif exchange == CFG["EXCHANGE"]["HUOBISWAP"] or exchange == CFG["EXCHANGE"]["HUOBIFUTURE"] \
                or exchange == CFG["EXCHANGE"]["HUOBI"] \
                or exchange == CFG["EXCHANGE"]["OKEXSWAP"] or exchange == CFG["EXCHANGE"]["OKEXFUTURE"]:
            if trade_unit == "BTC":
                return volume * price / multiplier
                # return volume * btc_spot_price / multiplier
                # return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
            elif trade_unit == "USD":
                return volume / multiplier
            elif trade_unit == "CONTRACTS":
                return volume
            else:
                '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
        elif exchange == CFG["EXCHANGE"]["BINANCEC"] or exchange == CFG["EXCHANGE"]["BINANCEU"]:
            if trade_unit == "BTC":
                # return volume * btc_spot_price / multiplier
                return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)
            elif trade_unit == "USD":
                if symbol.endswith('USDT') or symbol.endswith("USDC"):
                    return volume / (multiplier * price)
                else:
                    return volume / multiplier
            elif trade_unit == "CONTRACTS":
                return volume
        else:
            return '{}: {}: {} not completed'.format(exchange, symbol, contract_type)


def get_spot_price(exchange, symbol, timestamp):
    # TODO: complete fetching btc spot price from exchanges
    return 'spot price for {}: {} not completed'.format(exchange, symbol)


def _util_get_human_readable_timestamp():
    now = datetime.now()
    year = str(now.year)
    month = _util_padding_number(now.month, 2)
    day = _util_padding_number(now.day, 2)
    hour = _util_padding_number(now.hour, 2)
    minute = _util_padding_number(now.minute, 2)
    second = _util_padding_number(now.second, 2)
    millisecond = _util_padding_number(int(now.microsecond / 1000), 3)
    return year + month + day + hour + minute + second + millisecond


def _util_padding_number(num, digits):
    len_diff = digits - len(str(num))
    diff = ''
    for i in range(len_diff):
        diff += '0'
    return diff + str(num)

def split_symbol(symbol):
    re = symbol.split("_")
    if len(re) == 4:
        ## funding symbol
        return {
            'tag': 'funding',
            'exchange': re[1],
            'pair': re[2],
            'contract_type': re[3]
        }
    
    else:
        ## future/spot
        return {
            'tag': 'future',
            'exchange': re[0],
            'pair': re[1],
            'contract_type': re[2]
        }

def parse_pyarrow_table(arrow_table):
    result = {}
    for col in arrow_table.column_names:
        result[col] = arrow_table[col].to_numpy()
    return result

def calculate_daily_returns(account_value):
    """从账户价值计算日收益率"""
    return account_value.pct_change().dropna()

def calculate_total_return(account_value):
    """计算总收益率"""
    if len(account_value) < 2:
        return 0.0
    return account_value.iloc[-1] / account_value.iloc[0] - 1

def calculate_annual_return(account_value):
    """计算年化收益率"""
    if len(account_value) < 2:
        return 0.0
    
    # 计算总天数
    days = (account_value.index[-1] - account_value.index[0]).days
    if days == 0:
        return 0.0
    
    total_return = calculate_total_return(account_value)
    return (1 + total_return) ** (365 / days) - 1

def calculate_max_drawdown(account_value):
    """计算最大回撤(直接使用账户价值)"""
    peak = account_value.expanding(min_periods=1).max()
    drawdown = (account_value - peak) / peak
    return drawdown.min()

def calculate_annual_volatility(account_value):
    """计算年化波动率"""
    daily_returns = calculate_daily_returns(account_value)
    return daily_returns.std() * np.sqrt(365)

def calculate_sharpe_ratio(account_value, risk_free_rate=0.0):
    """计算夏普比率"""
    daily_returns = calculate_daily_returns(account_value)
    excess_return = daily_returns.mean() * 365 - risk_free_rate
    annualized_volatility = daily_returns.std() * np.sqrt(365)
    return excess_return / annualized_volatility if annualized_volatility != 0 else np.inf

def calculate_sortino_ratio(account_value, risk_free_rate=0.0, target_return=0.0):
    """计算索提诺比率"""
    daily_returns = calculate_daily_returns(account_value)
    adjusted_returns = daily_returns - risk_free_rate / 365
    downside_returns = adjusted_returns[adjusted_returns < target_return]
    
    if downside_returns.empty or downside_returns.std() == 0:
        return np.nan
    
    return (adjusted_returns.mean() * 365 - risk_free_rate) / (downside_returns.std() * np.sqrt(365))

def calculate_calmar_ratio(account_value):
    """计算卡玛比率"""
    annual_return = calculate_annual_return(account_value)
    max_dd = calculate_max_drawdown(account_value)
    return annual_return / abs(max_dd) if max_dd != 0 else np.nan

def calculate_omega_ratio(account_value, threshold=0.0):
    """计算欧米茄比率"""
    daily_returns = calculate_daily_returns(account_value)
    excess = daily_returns - threshold
    upside = excess[excess > 0].sum()
    downside = -excess[excess < 0].sum()
    return upside / downside if downside != 0 else np.nan

def calculate_win_rate(account_value):
    """计算胜率"""
    daily_returns = calculate_daily_returns(account_value)
    return (daily_returns > 0).mean()

def calculate_profit_loss_ratio(account_value):
    """计算盈亏比"""
    daily_returns = calculate_daily_returns(account_value)
    gains = daily_returns[daily_returns > 0]
    losses = daily_returns[daily_returns <= 0]
    
    avg_gain = gains.mean() if not gains.empty else 0
    avg_loss = losses.mean() if not losses.empty else 0
    
    return avg_gain / abs(avg_loss) if avg_loss != 0 else np.inf

def calculate_skew_kurtosis(account_value):
    """计算偏度和峰度"""
    daily_returns = calculate_daily_returns(account_value)
    return daily_returns.skew(), daily_returns.kurtosis()

def strategy_metrics(account_value):
    """
    计算完整策略指标
    account_value: 账户价值序列,包含时间戳索引
    """
    # 转换为pandas Series(如果输入是其他类型)
    account_value = pd.Series(account_value)
    
    # 确保时间索引是datetime类型
    account_value.index = pd.to_datetime(account_value.index, format='mixed')
    
    # 按天重采样(处理可能的不连续数据)
    account_value = account_value.resample('D').last().ffill()
    
    return {
        "total_return": calculate_total_return(account_value),
        "annual_return": calculate_annual_return(account_value),
        "annual_volatility": calculate_annual_volatility(account_value),
        "sharpe_ratio": calculate_sharpe_ratio(account_value),
        "max_drawdown": calculate_max_drawdown(account_value),
        "sortino_ratio": calculate_sortino_ratio(account_value),
        "calmar_ratio": calculate_calmar_ratio(account_value),
        "omega_ratio": calculate_omega_ratio(account_value),
        "win_rate": calculate_win_rate(account_value),
        "profit_loss_ratio": calculate_profit_loss_ratio(account_value),
        "skewness": calculate_skew_kurtosis(account_value)[0],
        "kurtosis": calculate_skew_kurtosis(account_value)[1]
    }

