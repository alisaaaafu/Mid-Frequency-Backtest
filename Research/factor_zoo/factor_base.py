import pandas as pd
import numpy as np

class FactorBase:
    def __init__(self, name, df):
        self.name = name
        self.signal_df = df.copy()
        self.signal_df.index = pd.to_datetime(self.signal_df['timestamp'])
        self.signal_df = self.signal_df.resample('1min').last().fillna(method='ffill')
        self.signal_df.time = self.signal_df.index
        self.signal_df = self.signal_df.reset_index(drop=True)