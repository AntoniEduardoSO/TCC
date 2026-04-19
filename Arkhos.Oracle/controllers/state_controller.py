import pandas as pd
from handlers.infrastructure_handler import InfrastructureHandler
from handlers.management_handler import ManagementHandler
from handlers.performance_handler import PerformanceHandler
from handlers.financial_handler import FinancialHandler

class StateController:
    def __init__(self, baselines: dict):
        self.handlers = [
            InfrastructureHandler(baselines),
            ManagementHandler(baselines),
            PerformanceHandler(baselines),
            FinancialHandler(baselines)
        ]
        
    def process_state(self, df: pd.DataFrame) -> list:
        all_prescriptions = []
        
        state_id = 27.0 
        
        for handler in self.handlers:
            diagnosticos = handler.evaluate_state(df)
            for diag in diagnosticos:
                diag['id_alvo'] = state_id
                
                diag['titulo'] = diag['titulo'].replace('🔮 ', '').replace('🚨 ', '').replace('⚠️ ', '').replace('📉 ', '').replace('⚖️ ', '')
                
                all_prescriptions.append(diag)
                
        return all_prescriptions