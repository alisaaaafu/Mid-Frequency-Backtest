# encoding = utf-8
"""
Author: Wamnzhen Fu
Date: 7-7-2020
"""
from enum import Enum


class Event_Type(Enum):
    EVENT_START = 'EVENT_START'  # 链接数据库,开始推送数据
    # EVENT_TICK = 'EVENT_TICK'  # 读取tick数据,(整合成Bar数据,if possible)推送给signal模块分析
    EVENT_BAR = 'EVENT_BAR'  # 读取Bar数据,if possible
    EVENT_FUNDING = 'EVENT_FUNDING'  # 读取Funding数据,if possible
    EVENT_SIGNAL = 'EVENT_SIGNAL'  # 从signal模块生成signal,先由risk模块进行过滤,然后关联到position和account模块
    # 发单,交给exchange进行撮合
    EVENT_BUY = "EVENT_BUY" 
    EVENT_SELL = "EVENT_SELL"
    EVENT_SHORT = "EVENT_SHORT"
    EVENT_COVER = "EVENT_COVER"
    EVENT_CANCEL_ORDER = 'EVENT_CANCEL_ORDER'  # 撤销订单
    EVENT_CANCEL_ALL = 'EVENT_CANCEL_ALL'  # 撤销所有订单

    EVENT_ORDERBACK = 'EVENT_ORDERBACK'  # 收到发单回执,更新当前position,account,并进行记录
    EVENT_ORDERCHECK = 'EVENT_ORDERCHECK'  # 检查当前所有order状态
    EVENT_ORDERCHECK_BACK = 'EVENT_ORDERCHECK_BACK'

    EVENT_STOP = 'EVENT_STOP'  # 停止回测,断开数据库链接
    EVENT_PLOT = 'EVENT_PLOT'
    EVENT_RESULT = 'EVENT_RESULT'  # 读取回测中各项信息,绘制结果图以及计算相关指标
    EVENT_TIMER = 'EVENT_TIMER'  # 计时器
    EVENT_LOG = 'EVENT_LOG'  # 日志

    EVENT_CANCELBACK = 'EVENT_CANCELBACK'
    EVENT_CANCEL_ALLBACK = 'EVENT_CANCEL_ALLBACK'

    EVENT_COMMON_ACCOUNT_UPDATE = 'EVENT_COMMON_ACCOUNT_UPDATE'

    EVENT_ORDERBOOK_UPDATE = 'EVENT_ORDERBOOK_UPDATE'
    # EVENT_CANCEL_ACCOUNT_UPDATE = 'EVENT_CANCEL_ACCOUNT_UPDATE'
    # EVENT_CANCEL_ALL_ACCOUNT_UPDATE = 'EVENT_CANCEL_ALL_ACCOUNT_UPDATE'
    

class OrderType(Enum):
    Market = 'taker'
    Limit = 'maker'
    FAK = 'fak'


class OrderAction(Enum):
    Buy = 'Buy'
    Sell = 'Sell'
    Cancel = 'Cancel'
    CancelAll = 'CancelALL'


class OrderOffset(Enum):
    """
    Buy + Open -> Sell + Close
    Sell + Open -> Buy + Close
    """
    Open = 'Open'
    Close = 'Close'


class PositionDirection(Enum):
    Long = 'Long'
    Short = 'Short'
    Net = 'Net' # 不持有头寸


class OrderStatus(Enum):
    NotTraded = 'NotTraded' # 未成交
    PartialTraded = 'PartialTraded' # 部分成交
    AllTraded = 'AllTraded' # 完全成交
    Cancelled = 'Cancelled' # 已撤单
    Refused = 'Refused' # 订单被拒绝

