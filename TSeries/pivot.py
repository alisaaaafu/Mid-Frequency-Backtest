from collections import deque
from TSeries.tseries import TSeries


class PivotHigh(TSeries):
    def __init__(self, input_series: TSeries, size: int, max_num: int = 20, name=None):
        name = name or f"PivotHigh({input_series.name}, size={size})"
        super().__init__(name)
        self.set_inputs(input_series)
        self.size = size
        self.max_num = max_num
        self.buffer = deque(maxlen=2 * size + 1)
        self.pivots = deque(maxlen=max_num)  # will automatically drop oldest

    def update(self, timestamp):
        val = self.inputs[0].value
        self.timestamp = timestamp

        if val is None:
            self.value = list(self.pivots)
            return

        self.buffer.append((timestamp, val))

        if len(self.buffer) == self.buffer.maxlen:
            mid_idx = self.size
            mid_time, mid_val = self.buffer[mid_idx]
            left = [v for _, v in list(self.buffer)[:mid_idx]]
            right = [v for _, v in list(self.buffer)[mid_idx + 1:]]

            if all(mid_val > x for x in left) and all(mid_val > x for x in right):
                self.pivots.append((mid_time, mid_val))

        self.value = list(self.pivots)


class PivotLow(TSeries):
    def __init__(self, input_series: TSeries, size: int, max_num: int = 20, name=None):
        name = name or f"PivotLow({input_series.name}, size={size})"
        super().__init__(name)
        self.set_inputs(input_series)
        self.size = size
        self.max_num = max_num
        self.buffer = deque(maxlen=2 * size + 1)
        self.pivots = deque(maxlen=max_num)  # store (timestamp, value)

    def update(self, timestamp):
        val = self.inputs[0].value
        self.timestamp = timestamp

        if val is None:
            self.value = list(self.pivots)
            return

        self.buffer.append((timestamp, val))

        if len(self.buffer) == self.buffer.maxlen:
            mid_idx = self.size
            mid_time, mid_val = self.buffer[mid_idx]
            left = [v for _, v in list(self.buffer)[:mid_idx]]
            right = [v for _, v in list(self.buffer)[mid_idx + 1:]]

            if all(mid_val < x for x in left) and all(mid_val < x for x in right):
                self.pivots.append((mid_time, mid_val))

        self.value = list(self.pivots)