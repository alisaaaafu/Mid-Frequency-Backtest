import pytest
from TSeries.ma import MA
from TSeries.bar_series import Bar, High, Low, Close
from TSeries.pivot import PivotHigh, PivotLow
from Utils.DataStructure import BAR
from TSeries.tseries_graph import tseries_graph
import datetime


def test_pivot_high_detection_with_limit():
    bar = Bar("bar_series")
    close = Close(bar)
    pivot_high = PivotHigh(close, size=1, max_num=2)

    # Create synthetic price data with multiple pivot highs
    prices = [1, 3, 2, 4, 2, 5, 2, 6, 2, 7]  # pivots at 3, 4, 5, 6, 7 (every odd index)
    timestamps = [datetime.datetime(2024, 1, 1, 0, i) for i in range(len(prices))]

    for i, (price, ts) in enumerate(zip(prices, timestamps)):
        bar_data = BAR(
            symbol="BTCUSDT",
            timestamp=int(ts.timestamp() * 1000),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=100,
            quote_volume=100,
            count=1,
            taker_buy_volume=50,
            taker_buy_quote_volume=50
        )
        bar.update(bar_data, ts)
        tseries_graph.update_all(ts)

    # Assert only the 2 most recent pivot highs are kept
    assert len(pivot_high.value) == 2
    # The last 2 pivots should correspond to (6, 6) and (8, 7)
    assert pivot_high.value[-1][1] == 6
    assert pivot_high.value[-2][1] == 5


def test_pivot_low_detection_with_limit():
    bar = Bar("bar_series")
    close = Close(bar)
    pivot_low = PivotLow(close, size=1, max_num=2)

    # Modified prices: multiple pivot lows, all positive
    prices = [5, 2, 6, 1, 6, 3, 6, 2, 6, 4]  # pivot lows at 2, 1, 3, 2

    timestamps = [datetime.datetime(2024, 1, 1, 0, i) for i in range(len(prices))]

    for i, (price, ts) in enumerate(zip(prices, timestamps)):
        bar_data = BAR(
            symbol="BTCUSDT",
            timestamp=int(ts.timestamp() * 1000),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=100,
            quote_volume=100,
            count=1,
            taker_buy_volume=50,
            taker_buy_quote_volume=50
        )
        bar.update(bar_data, ts)
        tseries_graph.update_all(ts)

    # Expect last two pivot lows to be 3 and 2
    assert len(pivot_low.value) == 2
    assert pivot_low.value[-1][1] == 2  # timestamp index 7
    assert pivot_low.value[-2][1] == 3  # timestamp index 5