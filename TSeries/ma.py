from collections import deque

from TSeries.tseries import TSeries

# Recommend MA periods: 3, 5, 8, 13, 21, 34, 55, ...
class MA(TSeries):
    def __init__(self, input_series, period, name=None):
        assert isinstance(period, int) and period > 0
        name = name or f"MA({input_series.name,period})"
        super().__init__(name)
        self.set_inputs(input_series)
        self.period = period
        self.buffer = deque(maxlen=period)

    def update(self, timestamp):
        input_val = self.inputs[0].value
        if input_val is not None:
            self.buffer.append(input_val)
        if len(self.buffer) == self.period:
            self.value = sum(self.buffer) / self.period
        else:
            self.value = None
        self.timestamp = timestamp