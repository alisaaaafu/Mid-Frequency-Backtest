# encoding=utf-8
"""
Author: Wamnzhen Fu
Date: 7-7-2020
"""

from abc import ABCMeta
from dataclasses import dataclass
from Utils.DataStructure import *
from Utils.Constant import Event_Type


class BaseEvent(object):
    """
    事件基类
    """
    __metaclass__ = ABCMeta

@dataclass
class START_EVENT(BaseEvent):
    """
    链接数据库,并开始读取推送数据
    """
    type = Event_Type.EVENT_START
    data = dict()

@dataclass
class BAR_EVENT(BaseEvent):
    """
    推送bar数据
    """
    type = Event_Type.EVENT_BAR
    data: BAR

@dataclass
class BUY_EVENT(BaseEvent):
    """
    买开事件
    """
    type = Event_Type.EVENT_BUY
    data: ORDER


@dataclass
class SELL_EVENT(BaseEvent):
    """
    卖平事件
    """
    type = Event_Type.EVENT_SELL
    data: ORDER


@dataclass
class SHORT_EVENT(BaseEvent):
    """
    卖开事件
    """
    type = Event_Type.EVENT_SHORT
    data: ORDER


@dataclass
class COVER_EVENT(BaseEvent):
    """
    买平事件
    """
    type = Event_Type.EVENT_COVER
    data: ORDER


@dataclass
class CANCEL_ORDER_EVENT(BaseEvent):
    """
    撤销订单
    """
    type = Event_Type.EVENT_CANCEL_ORDER
    data: CANCELORDER


@dataclass
class CANCEL_ALL_EVENT(BaseEvent):
    """
    撤销所有
    """
    type = Event_Type.EVENT_CANCEL_ALL
    data: CANCELALL


@dataclass
class CANCEL_BACK_EVENT(BaseEvent):
    """
    撤销回执
    """
    type = Event_Type.EVENT_CANCELBACK
    data: ORDER


@dataclass
class CANCEL_ALLBACK_EVENT(BaseEvent):
    """
    撤销所有回执
    """
    type = Event_Type.EVENT_CANCEL_ALLBACK
    data: dict


@dataclass
class BAR_EVENT(BaseEvent):
    """
    推送bar数据
    """
    type = Event_Type.EVENT_BAR
    data: BAR

@dataclass
class FUNDING_EVENT(BaseEvent):
    """
    推送funding数据
    """
    type = Event_Type.EVENT_FUNDING
    data: FUNDING


@dataclass
class SIGNAL_EVENT(BaseEvent):
    """
    生成signal,推送给risk和position,account检查过滤
    """
    type = Event_Type.EVENT_SIGNAL
    data = dict()


@dataclass
class ORDERBACK_EVENT(BaseEvent):
    """
    收到发单绘制
    """
    type = Event_Type.EVENT_ORDERBACK
    data: ORDERBACK


@dataclass
class ORDERCHECK_EVENT(BaseEvent):
    """
    检查订单状态
    """
    type = Event_Type.EVENT_ORDERCHECK
    data = dict()


@dataclass
class STOP_EVENT(BaseEvent):
    """
    回测停止
    """
    type = Event_Type.EVENT_STOP
    data = dict()

@dataclass
class PLOT_EVENT(BaseEvent):
    """
    回测停止
    """
    # 绘图 在mainEngine中添加plotEngine
    type = Event_Type.EVENT_PLOT
    data = dict()

@dataclass
class RESULT_EVENT(BaseEvent):
    """
    绘制结果
    """
    type = Event_Type.EVENT_RESULT
    data = dict()


@dataclass
class TIMER_EVENT(BaseEvent):
    """
    计时器
    """
    type = Event_Type.EVENT_TIMER
    data = dict()


@dataclass
class LOG_EVENT(BaseEvent):
    """
    日志
    """
    type = Event_Type.EVENT_LOG
    data: LOGDATA  ### 需要LOGDATA对log event进行初始化


@dataclass
class COMMON_ACCOUNTUPDATE_EVENT(BaseEvent):
    """
    一般position更新,发送消息更新account
    """
    type = Event_Type.EVENT_COMMON_ACCOUNT_UPDATE
    data: dict


@dataclass
class ORDERCHECK_EVENT(BaseEvent):
    """
    order book
    """
    type = Event_Type.EVENT_ORDERCHECK
    data: dict  # symbol


@dataclass
class ORDERCHECK_BACK_EVENT(BaseEvent):
    """
    check back
    """
    type = Event_Type.EVENT_ORDERCHECK_BACK
    data: dict  # symbol, orderbook


@dataclass
class ORDERBOOK_UPDATE_EVENT(BaseEvent):
    """
    挂单有变化
    """
    type = Event_Type.EVENT_ORDERBOOK_UPDATE
    data: dict  # symbol, orderbook


"""
@dataclass
class CANCEL_ACCOUNTUPDATE_EVENT(BaseEvent):
    #订单取消,更新position之后更新account
    type = Event_Type.EVENT_CANCEL_ACCOUNT_UPDATE
    data : dict
"""

"""
@dataclass
class CANCEL_ALL_ACCOUNTUPDATE_EVENT(BaseEvent):
    #取消全部订单,更新position之后更新account
    type = Event_Type.EVENT_CANCEL_ALL_ACCOUNT_UPDATE
    data : dict
"""
