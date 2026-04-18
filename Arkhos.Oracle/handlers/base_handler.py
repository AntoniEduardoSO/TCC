from abc import ABC, abstractmethod
import pandas as pd

class BaseHandler(ABC):
    
    @abstractmethod
    def evaluate_school(self, school_data: pd.Series) -> list:
        """Avalia microdados de uma única escola e retorna prescrições."""
        pass

    @abstractmethod
    def evaluate_municipality(self, municipality_data: pd.DataFrame) -> list:
        """Avalia dados agregados de um município e retorna prescrições."""
        pass

    @abstractmethod
    def evaluate_microregion(self, microregion_data: pd.DataFrame) -> list:
        """Avalia dados agregados de uma microrregião e retorna prescrições."""
        pass

    @abstractmethod
    def evaluate_mesoregion(self, mesoregion_data: pd.DataFrame) -> list:
        """Avalia dados agregados de uma mesorregião e retorna prescrições."""
        pass