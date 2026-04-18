import pandas as pd
from handlers.infrastructure_handler import InfrastructureHandler
from handlers.management_handler import ManagementHandler

class MesoregionController:
    def __init__(self, baselines: dict):
        self.handlers = [
            InfrastructureHandler(baselines),
            ManagementHandler(baselines)
        ]
        
    def process_all_mesoregions(self, df: pd.DataFrame) -> list:
        all_prescriptions = []
        
        # Garante que não há dados nulos quebrando o agrupamento
        df_valid = df.dropna(subset=['id_mesorregiao'])
        grouped = df_valid.groupby('id_mesorregiao')
        
        for meso_id, meso_df in grouped:
            for handler in self.handlers:
                results = handler.evaluate_mesoregion(meso_df)
                
                for req in results:
                    req['id_alvo'] = meso_id
                    all_prescriptions.append(req)
                
        return all_prescriptions