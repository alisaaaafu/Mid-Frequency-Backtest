from TSeries.tseries_graph import tseries_graph

class TSeries:
    def __init__(self, name):
        self.name = name
        self.inputs = []
        self.value = None
        self.timestamp = None
        tseries_graph.register(self)
    
    def set_inputs(self, *tseries):
        self.inputs = tseries

    def update(self, timestamp):
        raise NotImplementedError(f"update() not implemented in {self.name}")

