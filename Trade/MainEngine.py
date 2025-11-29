# encoding=utf-8
"""
Author: Wamnzhen Fu
Date: 7-20
"""
import logging
from Trade.Engine import *
from Event_Engine import Event_Engine
from Utils.Event import *
from Utils.Constant import *
from Utils.DataStructure import *
from Utils.decorator_functions import thread
from Exchange.Exchange import *
from uuid import uuid4


class MainEngine(EngineBase):
    """
    主引擎
    """
    def __init__(self, event_engine: Event_Engine, config, cfg, **kwargs):
        super(MainEngine, self).__init__(event_engine, "main")

        self.config = config
        self.cfg = cfg

        self.position_manager = PositionEngine(event_engine, config, cfg, **kwargs)  # 已完成init和register
        self.kwargs = kwargs
        # self.account_manager = AccountEngine(event_engine)  # 已完成init和register
        self.plot_manager = PlotEngine(event_engine, config, cfg)  # 画图
        self.log_manager = LogEngine(event_engine)
        self.order_manager = OrderEngine(event_engine, config, cfg)

        self.exchange = Exchange_Backtest_Medium_Frequency(ee=event_engine, is_windows=self.config["is_windows"], config=config, cfg=cfg)

        self.trading_symbols = self.config['TradingSymbols']  # 交易的品种
        self.funding_symbols = self.config['FundingSymbols']  # funding结算

        # bar数据
        self.BAR = dict()
        # funding数据
        self.FUNDING = dict()

        self.init()
        self.Connect_MONGO()

        self.timestamp = None

        self.register_function()
        self.position_manager.register_event()
        self.plot_manager.register_event()
        self.order_manager.register_event()
        self.log_manager.register_event()


    def init(self):
        """
        初始化
        """
        for symbol in self.trading_symbols:
            self.BAR[symbol] : BAR # type: ignore
        for symbol in self.funding_symbols:
            self.FUNDING[symbol] : FUNDING # type: ignore

    def start(self):
        """
        启动主引擎 - 单线程模式
        """
        # 首先启动事件引擎 - 在单线程模式下，这只是设置active标志
        self.event_manager.start()
        
        # 启动交易所 - 在单线程模式下，这会直接运行数据推送循环
        # 注意：这个调用会阻塞直到所有数据都被处理完毕
        self.write_log(f"--------- Backtest {self.config['strategy_name']} --------", level=logging.INFO)

        self.exchange.start()

    def Connect_MONGO(self):
        """
        链接数据库
        """
        self.mongo_service = MongoDBHandler(self.config)
        self.mongo_service.Connect_DB()

    def addStrategy(self, strategy):
        """
        链接指定策略
        """
        strategy_instance = strategy(self.config)
        strategy_instance.init(self)
        self.strategy = strategy_instance

        self.strategy.onInit(**self.kwargs)

        self.position_manager.addStrategy(self.strategy)
        self.plot_manager.addStrategy(self.strategy)
        self.order_manager.addStrategy(self.strategy)

        # position engine和account engine也需要用strategy初始化

    def process_bar_data(self, event):
        """
        处理收到的bar data,存储到本地的DepthBook
        """
        try:
            data = event.data
            
            for symbol in data.keys():
                if symbol in self.funding_symbols:
                    funding = FUNDING(symbol=symbol, funding_rate=float(data[symbol]['fundingRate']), timestamp=data[symbol]['timestamp'])
                    self.timestamp = data[symbol]['timestamp']
                    self.FUNDING[symbol] = funding
                    # update unrealized pnl
                    funding.symbol = funding.symbol.replace('Funding_', '')
                    self.position_manager.update_funding_pnl(funding)

                if symbol in self.trading_symbols:
                    self.timestamp = data[symbol]['timestamp']
                    self.BAR[symbol] = BAR(symbol=symbol, timestamp=data[symbol]['timestamp'], open=float(data[symbol]['open']), 
                                           high=float(data[symbol]['high']), low=float(data[symbol]['low']), close=float(data[symbol]['close']),
                                           volume=float(data[symbol]['volume']), quote_volume=float(data[symbol]['quote_volume']), 
                                           count=float(data[symbol]['count']), taker_buy_volume=float(data[symbol]['taker_buy_volume']), 
                                           taker_buy_quote_volume=float(data[symbol]['taker_buy_quote_volume']))

                    self.position_manager.update_pnl(self.BAR[symbol])

            if len(self.FUNDING) > 0:
                self.strategy.onFunding(self.FUNDING)

                self.FUNDING = dict()

            if len(self.BAR) > 0:
                # 调用策略处理数据
                self.strategy.onBar(self.BAR)

                self.BAR = dict()
                    
        except ValueError as e:
            self.write_log(f'trading data wrong, {symbol}, {self.timestamp}, error: {str(e)}', logging.ERROR)

    # def process_funding_data(self, event):
    #     """
    #     处理收到的funding数据
    #     """
    #     if type(event.data) == FUNDING:
    #         funding = event.data
    #         self.timestamp = funding.timestamp
    #         symbol = funding.symbol

    #         try:
    #             self.FUNDING[symbol] = funding

    #             # TODO: 更新账户beta信息
    #             self.strategy.onFunding(self.FUNDING[symbol])

    #             # TODO: update position, account
    #             # update unrealized pnl
    #             if symbol in self.funding_symbols:
    #                 funding.symbol = funding.symbol.replace('Funding_', '')
    #                 self.position_manager.update_funding_pnl(funding)

    #         except ValueError:
    #             self.write_log(f'funding data wrong,{symbol},{self.timestamp}', logging.ERROR)

    #     else:
    #         pass

    def register_function(self):
        """
        注册事件的处理函数
        """
        # 绑定到数据处理函数上
        self.event_manager.register(Event_Type.EVENT_BAR, self.process_bar_data)
        self.event_manager.register(Event_Type.EVENT_ORDERBACK, self.updateOrder)

        # 注册停止事件的处理顺序：先处理位置引擎保存数据，然后是交易所打印数据，最后是主引擎关闭
        self.event_manager.register(Event_Type.EVENT_STOP, self.position_manager.save_data)
        # 添加绘图事件
        self.event_manager.register(Event_Type.EVENT_PLOT, self.plot_manager.plot)
        # 注册关闭事件
        self.event_manager.register(Event_Type.EVENT_STOP, self.position_manager.close)
        self.event_manager.register(Event_Type.EVENT_STOP, self.exchange.on_close)
        # 主引擎的close方法放在最后执行
        self.event_manager.register(Event_Type.EVENT_STOP, self.close)

    def sendOrder(self, symbol, price, volume_in_contract, action, type, offset, bar):
        """
        发单
        """
        if self.config['start_time'] <= self.timestamp <= self.config['end_time']:
            # 创建订单
            order = ORDER(timestamp=self.timestamp, symbol=symbol, price=price, volume_in_contract=volume_in_contract,
                        orderType=type, direction=action, offset=offset, order_id=uuid4(), bar=bar)

            self.position_manager.last_order_id = order.order_id
            # self.account_manager.last_order_id = order.order_id

            global event
            if action == OrderAction.Buy and offset == OrderOffset.Open:
                event = BUY_EVENT(order)

            elif action == OrderAction.Sell and offset == OrderOffset.Open:
                event = SHORT_EVENT(order)

            elif action == OrderAction.Buy and offset == OrderOffset.Close:
                event = COVER_EVENT(order)

            elif action == OrderAction.Sell and offset == OrderOffset.Close:
                event = SELL_EVENT(order)

            self.event_manager.send_event(event)

    def write_log(self, msg: str, level=logging.INFO):
        """
        写日志
        """
        log = LOGDATA(log_content=msg, log_level=level)
        event = LOG_EVENT(log)
        self.event_manager.send_event(event)

    def close(self, event):
        """
        停止
        """
        # 避免重复调用
        if event.type != Event_Type.EVENT_STOP:
            return
        # 退出程序
        import sys
        sys.exit(0)

    def updateOrder(self, event):
        """
        发单回执
        """
        if event.type == Event_Type.EVENT_ORDERBACK:
            orderBack = event.data
            self.position_manager.back_id = orderBack.order_id
            # self.account_manager.back_id = orderBack.order_id
            self.strategy.onOrder(orderBack)

    def save_order_info(self, order):
        """
        保存order信息到本地(仅send order)
        """
        DB = self.__order_DB

        Data = None
        Order = {"timestamp": order.timestamp, "id": order.order_id, "symbol": order.symbol, "volume": order.volume,
                 "price": order.price,
                 "orderType": f'{order.orderType}', "direction": f'{order.direction}', "offset": f'{order.offset}'}
        Info = {"req": 'insert', "data": Order}

        COL = order.symbol

        mongo = MONGODATA(DB=DB, COL=COL, Info=Info, Data=Data)
        self.mongo_service.on_insert(mongo)
