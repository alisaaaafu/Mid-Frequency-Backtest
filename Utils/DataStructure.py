# encoding=utf-8
"""
Author: Wamnzhen Fu
Date: 7-7-2020
"""
from dataclasses import dataclass
import logging
from datetime import datetime
from collections import OrderedDict
from uuid import UUID

from Utils.Constant import *


@dataclass
class BaseDataStructure(object):
    """
    数据结构基类
    """
    pass


@dataclass
class TICK(BaseDataStructure):
    """
    10档的深度数据
    """
    symbol : str
    timestamp : int
    asks : dict  ### price:size
    bids : dict

@dataclass
class BAR(BaseDataStructure):
    """
    BAR数据
    """
    symbol : str
    timestamp: None
    open : float
    high : float
    low : float
    close : float
    volume : float
    quote_volume : float
    count : int
    taker_buy_volume : float
    taker_buy_quote_volume : float
    # meta_data : dict

@dataclass
class FUNDING(BaseDataStructure):
    """
    交易所目前的资金费率
    """
    timestamp: None
    symbol: str
    funding_rate: float


@dataclass
class REALTIMEDATA(BaseDataStructure):
    """
    依据行情接口获取的数据
    can be implemented if necessary
    """
    pass


@dataclass
class MONGODATA(BaseDataStructure):
    """
    MONGO数据库
    """
    DB : str
    COL : str
    Data : None  # 生成器
    Info : dict  # req和data两部分,用于对mongo下达操作指令


@dataclass
class ORDER(BaseDataStructure):
    """
    发单
    """
    timestamp: int
    symbol: str
    volume_in_contract: float  # in contract
    price: float
    orderType: str  # market or limit
    direction: str  # sell or buy or cancel
    status = None
    bar : BAR
    offset: str  # 开仓还是平仓
    lever_rate = 1
    order_id: UUID   # uuid.uuid4()
    trade_volume = 0
    fee = 0
    traded_avg_price = 0

@dataclass
class CANCELORDER(BaseDataStructure):
    """
    撤销一个订单
    """
    timestamp : int
    symbol : str
    order : ORDER

@dataclass
class CANCELALL(BaseDataStructure):
    """
    撤销全部订单
    """
    timestamp : int
    symbol : str


@dataclass
class ORDERBACK(BaseDataStructure):
    """
    发单回执,成员在ORDER内均可以找到,用来更新对应ORDER的状态；
    查询订单状态所返回的也是该类
    在实现完成Exchange功能之后再完成该类并使用
    """
    timestamp: int
    symbol: str
    volume: float   ## 订单总量, in trade_unit
    volume_in_contract: float  ## 订单总量, in contract
    first_time = None

    cal_margin = True
    price: float
    orderType: str
    direction: str
    status: None
    offset : str
    order_id: None
    trade_volume: float  # in trade_unit
    trade_volume_in_contract: float ## in contract
    fee: float  ## in trade_unit
    traded_avg_price: float

    last_price : float


@dataclass
class POSITION(BaseDataStructure):
    """
    仓位信息
    """
    symbol: str
    volume: float = 0.0  # in trade_unit
    contracts: float = 0.0  # in contract
    trade_volume: float = 0.0  # in trade_unit
    cur_price: float = 0.0
    available: float = 0.0  # 可平仓数量 in contract
    frozen: float = 0.0  # 正在挂单平仓的数量 in contract
    margin_frozen: float = 0.0  # 开仓时冻结的保证金数量 in trade_unit
    avg_price: float = 0.0
    profit_real: float = 0.0  # in trade_unit
    profit_unreal: float = 0.0  # in trade_unit
    direction: str = PositionDirection.Net
    timestamp: str = '0'

    tmp_real_pnl: float = 0.0  # in trade_unit
    tmp_unreal_pnl: float = 0.0  # in trade_unit
    hedge_pnl: float = 0.0  # in trade_unit
    position_pnl: float = 0.0  # in trade_unit
    funding_pnl: float = 0.0  # in trade_unit
    total_pnl: float = 0.0  # in trade_unit

@dataclass
class ACCOUNT(BaseDataStructure):
    """
    账户信息
    """
    symbol: str
    init_balance: float  # 初始资产数量, in trade_unit
    timestamp: int = 0
    margin_balance: float = 0.0  # 账户权益, in trade_unit
    margin_position: float = 0.0  # 仓位占用保证金, in trade_unit
    margin_frozen: float = 0.0  # 冻结保证金, in trade_unit
    margin_available: float = 0.0  # 可用保证金,可开仓数量, in trade_unit
    profit_real: float = 0.0  # in trade_unit
    profit_unreal: float = 0.0  # in trade_unit
    lever_rate: float = 1

"""@dataclass
class DEPTHLEVEL(BaseDataStructure):
    
    //订单簿中每个level的结构

    timestamp : int
    level : dict  ## price:(size,cusum_size)  level上的size和累计到该level的size
"""

@dataclass
class DEPTHTREE(BaseDataStructure):
    """
    订单簿的一侧,树形结构
    """
    timestamp : int
    data : dict

@dataclass
class DEPTHBOOK(BaseDataStructure):
    """
    订单簿,bids+asks,timestamp
    """
    timestamp : int
    asks : DEPTHTREE
    bids : DEPTHTREE
    last_price : float
    last_vol : float


@dataclass
class ORDERLIST(BaseDataStructure):
    """
    交易者手上的订单列表,bid/ask
    """
    orders : dict   ## 挂的订单先直接插入到order list当中,然后进行排序成orderedDict,插入到ORDERBOOK中


@dataclass
class ORDERBOOK(BaseDataStructure):
    """
    交易者目前所挂的订单
    """
    timestamp : int
    ask_orders : ORDERLIST
    bid_orders : ORDERLIST

@dataclass
class LOGDATA(BaseDataStructure):
    """
    日志
    """
    log_content: str # 日志内容
    log_level: int = logging.INFO

    def __post_init__(self):
        self.log_time = datetime.now()
