from handlers.infrastructure_handler import InfrastructureHandler
from handlers.management_handler import ManagementHandler
import pandas as pd

class SchoolController:
    def __init__(self):
        self.handlers = [
            InfrastructureHandler(),
            ManagementHandler()
        ]
        
    def process_all_schools(self, df: pd.DataFrame) -> list:
        all_prescriptions = []
        
        for _, school_row in df.iterrows():
            school_id = school_row['id_escola_fk']
            
            for handler in self.handlers:
                results = handler.evaluate_school(school_row)
                
                for res in results:
                    res['id_alvo'] = school_id
                    all_prescriptions.append(res)
                
        return all_prescriptions