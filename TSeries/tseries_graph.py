import networkx as nx

class TSeriesGraph:
    def __init__(self) -> None:
        self.graph = nx.DiGraph()
        self.series = {}
        self._cached_update_order = None
        self._graph_dirty = True

    def register(self, tseries):
        self.series[tseries.name] = tseries
        self.graph.add_node(tseries.name)
        for input_ts in getattr(tseries, 'inputs', []):
            self.graph.add_edge(input_ts.name, tseries.name)
        self._graph_dirty = True
    
    def update_all(self, timestamp):
        if self._graph_dirty or self._cached_update_order is None:
            try:
                self._cached_update_order = list(nx.topological_sort(self.graph))
            except nx.NetworkXUnfeasible:
                raise RuntimeError("Cycle detected in TSeriesGraph")
            self._graph_dirty = False

        for name in self._cached_update_order:
            tseries = self.series[name]
            if getattr(tseries, 'is_source', False):
                continue
            tseries.update(timestamp)


# Global singleton instance
tseries_graph = TSeriesGraph()