from TSeries.tseries import TSeries

from Utils.DataStructure import BAR

class Bar(TSeries):
    def __init__(self, name="bar"):
        super().__init__(name)
        self.is_source = True

    def update(self, bar: BAR, timestamp):
        self.value = bar
        self.timestamp = timestamp


class Open(TSeries):
    def __init__(self, bar_series: Bar, name="open"):
        super().__init__(name)
        self.set_inputs(bar_series)

    def update(self, timestamp):
        self.value = self.inputs[0].value.open
        self.timestamp = timestamp


class High(TSeries):
    def __init__(self, bar_series: Bar, name="high"):
        super().__init__(name)
        self.set_inputs(bar_series)

    def update(self, timestamp):
        self.value = self.inputs[0].value.high
        self.timestamp = timestamp


class Low(TSeries):
    def __init__(self, bar_series: Bar, name="low"):
        super().__init__(name)
        self.set_inputs(bar_series)

    def update(self, timestamp):
        self.value = self.inputs[0].value.low
        self.timestamp = timestamp


class Close(TSeries):
    def __init__(self, bar_series: Bar, name="close"):
        super().__init__(name)
        self.set_inputs(bar_series)

    def update(self, timestamp):
        self.value = self.inputs[0].value.close
        self.timestamp = timestamp


class Volume(TSeries):
    def __init__(self, bar_series: Bar, name="volume"):
        super().__init__(name)
        self.set_inputs(bar_series)

    def update(self, timestamp):
        self.value = self.inputs[0].value.volume
        self.timestamp = timestamp