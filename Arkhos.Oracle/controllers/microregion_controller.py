import pandas as pd
from handlers.infrastructure_handler import InfrastructureHandler
from handlers.management_handler import ManagementHandler
from handlers.performance_handler import PerformanceHandler
from handlers.financial_handler import FinancialHandler

class MicroregionController:
    def __init__(self, baselines: dict):
        self.handlers = [
            InfrastructureHandler(baselines),
            ManagementHandler(baselines),
            PerformanceHandler(baselines),
            FinancialHandler(baselines)
        ]
        
    def process_all_microregions(self, df: pd.DataFrame) -> list:
        all_prescriptions = []
        
        # Garante que não há dados nulos quebrando o agrupamento
        df_valid = df.dropna(subset=['id_microrregiao'])
        grouped = df_valid.groupby('id_microrregiao')
        
        for micro_id, micro_df in grouped:
            for handler in self.handlers:
                results = handler.evaluate_microregion(micro_df)
                
                for req in results:
                    req['id_alvo'] = micro_id
                    all_prescriptions.append(req)
                
        return all_prescriptions