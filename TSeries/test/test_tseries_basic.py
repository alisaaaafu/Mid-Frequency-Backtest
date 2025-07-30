import pytest
from TSeries.ma import MA
from TSeries.bar_series import Bar, Open, High, Low, Close, Volume
from Utils.DataStructure import BAR
from TSeries.tseries_graph import tseries_graph
import datetime

def test_bar_series_derived_fields():
    # Create the source Bar series
    bar = Bar("bar_series")

    # Create derived series
    open_series = Open(bar)
    high_series = High(bar)
    low_series = Low(bar)
    close_series = Close(bar)
    volume_series = Volume(bar)

    # Create a mock BAR data object
    bar_data = BAR(
        symbol="BTCUSDT",
        timestamp=1700000000000,
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=1234.5,
        quote_volume=5678.9,
        count=42,
        taker_buy_volume=600.0,
        taker_buy_quote_volume=3000.0
    )

    # Update the source bar and propagate updates
    ts = datetime.datetime(2024, 1, 1, 0, 0)
    bar.update(bar_data, ts)
    tseries_graph.update_all(ts)

    # Assertions
    assert open_series.value == 100.0
    assert high_series.value == 110.0
    assert low_series.value == 90.0
    assert close_series.value == 105.0
    assert volume_series.value == 1234.5

def test_ma_computation():
    bar = Bar("bar_series")
    close = Close(bar)
    ma = MA(close, period=3)

    prices = [10, 11, 12, 13, 14, 15]
    expected_sma = [None, None, 11.0, 12.0, 13.0, 14.0]  # 3-period MA

    for i, price in enumerate(prices):
        bar_data = BAR(
            symbol="BTCUSDT",
            timestamp=1700000000000 + i,
            open=price,
            high=price + 1,
            low=price - 1,
            close=price,
            volume=1000,
            quote_volume=2000,
            count=1,
            taker_buy_volume=500,
            taker_buy_quote_volume=1000
        )
        ts = datetime.datetime(2024, 1, 1, 0, i)
        bar.update(bar_data, ts)
        tseries_graph.update_all(ts)

        if expected_sma[i] is None:
            assert ma.value is None
        else:
            assert ma.value == pytest.approx(expected_sma[i])
