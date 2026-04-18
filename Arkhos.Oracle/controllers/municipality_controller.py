from handlers.infrastructure_handler import InfrastructureHandler
from handlers.management_handler import ManagementHandler

class MunicipalityController:
    def __init__(self):
        self.handlers = [
            InfrastructureHandler(),
            ManagementHandler()
        ]
        
    def process_all_municipalities(self, df):
        all_prescriptions = []

        for city_id, city_df in df.groupby('id_municipio'):
            for handler in self.handlers:
                results = handler.evaluate_municipality(city_df)
                
                for res in results:
                    res['id_alvo'] = city_id
                    all_prescriptions.append(res)
                    
        return all_prescriptions