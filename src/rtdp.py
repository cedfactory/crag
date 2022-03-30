from abc import ABCMeta, abstractmethod
import pandas as pd

class RealTimeDataProvider(metaclass = ABCMeta):
    
    def __init__(self, params = None):
        pass

    @abstractmethod
    def next(self):
        pass

    @abstractmethod
    def get_current_data(self):
        pass
