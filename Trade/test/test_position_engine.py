import pytest
import json
from uuid import UUID

from Trade.Engine import PositionEngine
from Event_Engine import Event_Engine
from Utils.Constant import OrderType, OrderAction, OrderOffset, OrderStatus
from Utils.DataStructure import POSITION, ACCOUNT, ORDERBACK
from Utils.Event import ORDERBACK_EVENT


hardcoded_uuid = UUID("123e4567-e89b-12d3-a456-426614174000")

CONFIG = {
    "coin": "btc",
    "user": "yc",
    "start_time": "2024-01-10 00-10-00",
    "end_time": "2024-01-10 00-30-00",
    "lookback_time": "2024-01-01 00-10-00",
    'bt_time': "20250419120000",
    "strategy_name": "test_strategy",
    'is_windows': False,
    "TradingSymbols": ["BinanceU_BTCUSDT_perp"],
    "FundingSymbols": ["Funding_BinanceU_BTCUSDT_perp"],
    "MARKET_DATA": ["BinanceU_BTCUSDT_perp", "Funding_BinanceU_BTCUSDT_perp"],
    "DB": {
        "Mongo_Host": "localhost",
        "Mongo_Port": "27017",
        "ACCOUNT_DB": "test_AccountInfo-test_strategy",
        "ACCOUNT_COL": {"BinanceU_BTCUSDT_perp": "BinanceU_BTCUSDT_perp"},
        "POSITION_DB": "test_PositionInfo-test_strategy",
        "POSITION_COL": {"BinanceU_BTCUSDT_perp": {"Long": "BinanceU_BTCUSDT_perp_long", "Short": "BinanceU_BTCUSDT_perp_short"}},
        "ORDER_DB": "test_OrderInfo-test_strategy",
        "ORDER_COL": {"BinanceU_BTCUSDT_perp": {"Long": "BinanceU_BTCUSDT_perp_long", "Short": "BinanceU_BTCUSDT_perp_short"}},
    },
    "init_account": "300",
    "Trade_Unit": "COIN",
    "Min_Unit": "0.01",
    "Fee_Type": "taker",
    "Slippage": "0.0005"
}

symbol = CONFIG['TradingSymbols'][0]

with open("cfg.json", 'r') as f:
    CFG = json.load(f)

@pytest.fixture(scope="module")
def event_engine():
    return Event_Engine()

@pytest.fixture(scope="module")
def position_engine(event_engine):
    pe = PositionEngine(event_engine, CONFIG, CFG)
    pe.init()
    pe.register_event()
    pe.event_manager.start()
    return pe

# Now every test can take either or both as needed
def test_init_position_engine(position_engine):
    
    assert list(position_engine.position.keys()) == [symbol]
    assert position_engine.position[symbol]['long'] == POSITION(symbol=symbol)
    assert position_engine.position[symbol]['short'] == POSITION(symbol=symbol)
    assert position_engine.account[symbol] == ACCOUNT(symbol=symbol, init_balance=float(CONFIG['init_account']), margin_available=float(CONFIG['init_account']))

def test_event_engine_access(event_engine):
    assert hasattr(event_engine, "register")  # basic check that engine is functional

def test_orderback_event(position_engine):
    orderback = ORDERBACK(timestamp=1, symbol=symbol, volume=0.5,
                        volume_in_contract=0.5, 
                        price=100.5, orderType=OrderType.Market, direction=OrderAction.Buy,
                        order_id=hardcoded_uuid, trade_volume=0.5,
                        trade_volume_in_contract=0.5,
                        traded_avg_price=100.5, fee=0, offset=OrderOffset.Open, last_price=100.5,
                        status=OrderStatus.AllTraded)
    orderback_event = ORDERBACK_EVENT(data=orderback)
    position_engine.event_manager.send_event(orderback_event)
    
    assert position_engine.position[symbol]['long'].cur_price == 100.5
    assert position_engine.position[symbol]['long'].avg_price == 100.5
    assert position_engine.position[symbol]['long'].volume == 0.5
    assert position_engine.position[symbol]['long'].trade_volume == 0.5

    orderback = ORDERBACK(timestamp=5, symbol=symbol, volume=0.5,
                        volume_in_contract=0.5, 
                        price=103.5, orderType=OrderType.Market, direction=OrderAction.Sell,
                        order_id=hardcoded_uuid, trade_volume=0.5,
                        trade_volume_in_contract=0.5,
                        traded_avg_price=103.5, fee=0, offset=OrderOffset.Close, last_price=103.5,
                        status=OrderStatus.AllTraded)
    orderback_event = ORDERBACK_EVENT(data=orderback)
    position_engine.event_manager.send_event(orderback_event)
    assert position_engine.position[symbol]['long'].cur_price == 103.5
    assert position_engine.position[symbol]['long'].avg_price == 0
    assert position_engine.position[symbol]['long'].volume == 0.0
    assert position_engine.position[symbol]['long'].trade_volume == 1.0
    assert position_engine.position[symbol]['long'].profit_real == 0.014925373134328358