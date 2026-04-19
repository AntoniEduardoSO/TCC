from abc import ABC, abstractmethod
import pandas as pd

class BaseHandler(ABC):
    
    @abstractmethod
    def evaluate_school(self, school_data: pd.Series) -> list:
        pass

    @abstractmethod
    def evaluate_municipality(self, municipality_data: pd.DataFrame) -> list:
        pass

    @abstractmethod
    def evaluate_microregion(self, microregion_data: pd.DataFrame) -> list:
        pass

    @abstractmethod
    def evaluate_mesoregion(self, mesoregion_data: pd.DataFrame) -> list:
        pass

    @abstractmethod
    def evaluate_state(self, state_data: pd.DataFrame) -> list: pass