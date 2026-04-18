import pandas as pd
from handlers.infrastructure_handler import InfrastructureHandler
from handlers.management_handler import ManagementHandler

class MicroregionController:
    def __init__(self):
        self.handlers = [
            InfrastructureHandler(),
            ManagementHandler()
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