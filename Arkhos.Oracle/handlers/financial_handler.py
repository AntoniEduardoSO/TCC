import pandas as pd
from handlers.base_handler import BaseHandler

class FinancialHandler(BaseHandler):
    
    def __init__(self, baselines: dict):

        # Baselines (Médias Estaduais)
        self.media_custo = baselines.get('custo_aluno_ano', 6500.00)
        self.media_infra = baselines.get('acessibilidade_mean', 0.5)
        self.media_evasao = baselines.get('evasao_mean', 0.05)

        # Dispersão (Desvio Padrão) para cálculo do Z-Score dinâmico
        self.std_custo = baselines.get('custo_aluno_std', 1200.00)
        self.std_infra = baselines.get('acessibilidade_std', 0.15)
        self.std_evasao = baselines.get('evasao_std', 0.02)
    
    def _calcular_z_score(self, valor: float, media: float, std: float) -> float:
        if std == 0: return 0.0
        return (valor - media) / std

    def evaluate_school(self, school_data: pd.Series) -> list:
        return []

    def evaluate_municipality(self, municipality_data: pd.DataFrame) -> list:
        prescriptions = []
        
        # Métricas reais do município
        custo_aluno_rede = municipality_data['spending_per_student'].mean()
        infra_rede = municipality_data['acessibility_rating'].mean()
        evasao_rede = municipality_data['dropout_rate'].mean()
        
        # Padronização Estatística (Z-Scores)
        z_custo = self._calcular_z_score(custo_aluno_rede, self.media_custo, self.std_custo)
        z_infra = self._calcular_z_score(infra_rede, self.media_infra, self.std_infra)
        z_evasao = self._calcular_z_score(evasao_rede, self.media_evasao, self.std_evasao)
        

        if z_custo > 1.0 and (z_infra < -1.0 or z_evasao > 1.0):
            req = {
                "axis": "Financial", "level": "Municipality", "tipo_insight": "Prescritivo",
                "titulo": "Alerta Crítico: Ineficiência Alocativa Severa.",
                "valor_destaque": f"R$ {custo_aluno_rede:,.2f}",
                "descricao": f"é o custo por aluno. O município apresenta despesa estatisticamente discrepante (+{z_custo:.1f} desvios padrão acima da média), contrastando com indicadores pedagógicos e estruturais em colapso crítico.",
                "recomendacao": "Acionar os órgãos de controlo (TCE) para auditoria especial em contratos de serviços contínuos. Requer bloqueio preventivo de novos empenhos não essenciais.",
                "valor_baseline": self.media_custo
            }
            prescriptions.append(req)
            
        elif z_custo > 0.5 and (z_infra < -0.5 or z_evasao > 0.5):
            req = {
                "axis": "Financial", "level": "Municipality", "tipo_insight": "Prescritivo",
                "titulo": "Atenção: Ineficiência Alocativa Moderada.",
                "valor_destaque": f"R$ {custo_aluno_rede:,.2f}",
                "descricao": f"é o custo por aluno. A rede apresenta gastos superiores à mediana do Estado, mas não converte esse investimento em melhoria de infraestrutura ou retenção de alunos.",
                "recomendacao": "Revisar imediatamente a matriz de alocação de recursos e repactuar contratos de transporte e alimentação para otimizar o fluxo de caixa.",
                "valor_baseline": self.media_custo
            }
            prescriptions.append(req)

        # Quadrante 3: Subfinanciamento Crítico
        # Condição: Gasto precário (< -1 Desvio Padrão) E Retorno em colapso
        elif z_custo < -1.0 and (z_infra < -1.0 or z_evasao > 1.0):
            req = {
                "axis": "Financial", "level": "Municipality", "tipo_insight": "Prescritivo",
                "titulo": "Alerta Crítico: Subfinanciamento Crônico da Rede.",
                "valor_destaque": f"R$ {custo_aluno_rede:,.2f}",
                "descricao": f"é o investimento por aluno. A rede opera em grave déficit financeiro ({abs(z_custo):.1f} desvios padrão abaixo da média estadual), o que justifica matematicamente a precariedade estrutural observada.",
                "recomendacao": "Decretar estado de emergência educacional para captação prioritária de emendas parlamentares e adesão massiva a editais do FNDE/PAR.",
                "valor_baseline": self.media_custo
            }
            prescriptions.append(req)
            
        return prescriptions

    def evaluate_microregion(self, microregion_data: pd.DataFrame) -> list:
        prescriptions = []
        
        std_custo_micro = microregion_data.groupby('id_municipio')['spending_per_student'].mean().std()
        media_custo_micro = microregion_data['spending_per_student'].mean()
        
        # Otimização Científica: Substituição do limite fixo de "1500" pelo Coeficiente de Variação (CV)
        if pd.notna(std_custo_micro) and media_custo_micro > 0:
            cv = std_custo_micro / media_custo_micro
            
            # Um CV > 0.20 (20%) indica altíssima dispersão / desigualdade em uma mesma região geográfica
            if cv > 0.20: 
                req = {
                    "axis": "Financial", "level": "Microregion", "tipo_insight": "Prescritivo",
                    "titulo": "Diretriz Regional: Assimetria Extrema no Custo-Aluno.",
                    "valor_destaque": f"{cv * 100:.1f}%",
                    "descricao": "é o Coeficiente de Variação (CV) dos gastos entre prefeituras vizinhas. A alta assimetria aponta que municípios próximos operam com eficiências financeiras drasticamente desiguais no mesmo território.",
                    "recomendacao": "Fomentar a criação de Consórcios Públicos Intermunicipais para ganho de escala em licitações conjuntas (Atas de Registo de Preços) de insumos escolares.",
                    "valor_baseline": 0.10 # CV tolerável em torno de 10%
                }
                prescriptions.append(req)
                
        return prescriptions

    def evaluate_mesoregion(self, mesoregion_data: pd.DataFrame) -> list:
        prescriptions = []
        
        custo_aluno_meso = mesoregion_data['spending_per_student'].mean()
        infra_meso = mesoregion_data['acessibility_rating'].mean()

        z_custo_meso = self._calcular_z_score(custo_aluno_meso, self.media_custo, self.std_custo)
        z_infra_meso = self._calcular_z_score(infra_meso, self.media_infra, self.std_infra)
        
        if z_custo_meso < -0.5 and z_infra_meso < -0.5:
            req = {
                "axis": "Financial", "level": "Mesoregion", "tipo_insight": "Prescritivo",
                "titulo": "Diretriz Macrorregional: Subfinanciamento Histórico.",
                "valor_destaque": f"R$ {custo_aluno_meso:,.2f}",
                "descricao": f"é a média de investimento por aluno. Este território opera financeiramente abaixo da média do Estado, o que consolida o abismo de infraestrutura escolar nesta macrorregião.",
                "recomendacao": "Desenhar política de repasse compensatório no orçamento estadual (LOA) e ICMS Educacional visando a equidade territorial.",
                "valor_baseline": self.media_custo
            }
            prescriptions.append(req)
            
        return prescriptions
    
    def evaluate_state(self, state_data: pd.DataFrame) -> list:
        prescriptions = []
        
        media_custo_estado = state_data['spending_per_student'].mean()
        media_infra_estado = state_data['acessibility_rating'].mean()
        
        if media_custo_estado > self.media_custo and media_infra_estado < self.media_infra:
            req = {
                "axis": "Financial", "level": "State", "tipo_insight": "Prescritivo",
                "titulo": "Diretriz Estadual: Ineficiência Alocativa Global.",
                "valor_destaque": f"R$ {media_custo_estado:,.2f}",
                "descricao": "é a média de custo-aluno do Estado. O indicador aponta que, no panorama geral, o volume financeiro investido na educação alagoana não está refletindo em avanço da infraestrutura física da rede.",
                "recomendacao": "Revisar o Plano Estadual de Educação (PEE) e propor reforma imediata nos critérios de repasse do Tesouro Estadual e FUNDEB.",
                "valor_baseline": self.media_custo
            }
            prescriptions.append(req)
            
        return prescriptions