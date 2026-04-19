from handlers.infrastructure_handler import InfrastructureHandler
from handlers.management_handler import ManagementHandler
from handlers.performance_handler import PerformanceHandler
from handlers.financial_handler import FinancialHandler
import pandas as pd

class SchoolController:
    def __init__(self, baselines: dict):
        self.handlers = [
            InfrastructureHandler(baselines),
            ManagementHandler(baselines),
            PerformanceHandler(baselines),
            FinancialHandler(baselines)
        ]
        
    def process_all_schools(self, df: pd.DataFrame) -> list:
        all_prescriptions = []
        
        for _, school_row in df.iterrows():
            school_id = school_row['id_escola_fk']
            school_results = []
            
            # Coleta isolada (Silos)
            for handler in self.handlers:
                diagnosticos = handler.evaluate_school(school_row)
                for diag in diagnosticos:
                    diag['id_alvo'] = school_id
                    
                    # Limpeza de caracteres especiais visuais (Emojis) para delegar renderização ao Front-end
                    diag['titulo'] = diag['titulo'].replace('🔮 ', '').replace('🚨 ', '').replace('⚠️ ', '').replace('📉 ', '').replace('⚖️ ', '')
                    
                    school_results.append(diag)
            
            # Pós-Processamento: Fusão de Diagnósticos (Cross-Axis)
            if school_results:
                school_results = self._gerar_alertas_cruzados(school_results, school_id)
            
            all_prescriptions.extend(school_results)
            
        return all_prescriptions

    def _gerar_alertas_cruzados(self, diagnosticos: list, school_id: float) -> list:
        titulos = [d['titulo'].lower() for d in diagnosticos]
        
        has_evasao_severa = any('evasão' in t or 'abandono' in t for t in titulos)
        has_rh_fragil = any('docente' in t or 'vínculo' in t for t in titulos)
        has_infra_ruim = any('acessibilidade' in t or 'estrutural' in t for t in titulos)

        # --- Regra de Cruzamento 1: Colapso Pedagógico (Desempenho + Gestão de RH) ---
        if has_evasao_severa and has_rh_fragil:
            diagnosticos.append({
                "axis": "Cross-Axis", 
                "level": "School", 
                "tipo_insight": "Prescritivo",
                "titulo": "ALERTA CRUZADO: Evasão tracionada por instabilidade docente.",
                "valor_destaque": "CRÍTICO",
                "descricao": "A unidade apresenta sobreposição crítica: alto índice de abandono escolar aliado a um quadro de professores majoritariamente provisório. O vínculo do aluno com a escola encontra-se rompido.",
                "recomendacao": "Deflagrar intervenção prioritária. Congelar transferências de professores efetivos desta unidade e alocar equipe multidisciplinar de retenção.",
                "valor_baseline": 0.0, 
                "id_alvo": school_id
            })

        # --- Regra de Cruzamento 2: Falência Estrutural e Pedagógica (Desempenho + Infraestrutura) ---
        if has_evasao_severa and has_infra_ruim:
            diagnosticos.append({
                "axis": "Cross-Axis", 
                "level": "School", 
                "tipo_insight": "Prescritivo",
                "titulo": "ALERTA CRUZADO: Ambiente inóspito agravando abandono.",
                "valor_destaque": "CRÍTICO",
                "descricao": "Há correlação direta nesta unidade entre a degradação da infraestrutura física e a alta taxa de evasão de alunos.",
                "recomendacao": "Vincular a aprovação de reformas emergenciais (Plano de Obras) ao compromisso de Busca Ativa Escolar assinado pela direção.",
                "valor_baseline": 0.0, 
                "id_alvo": school_id
            })
            
        return diagnosticos