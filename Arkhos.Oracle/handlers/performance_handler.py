import pandas as pd
from handlers.base_handler import BaseHandler

class PerformanceHandler(BaseHandler):
    
    def __init__(self, baselines: dict):
        self.baseline_evasao = baselines.get('evasao_mean', 0.05)

        self.margem_evasao = baselines.get('evasao_std', 0.02)
        if pd.isna(self.margem_evasao): self.margem_evasao = 0.02

        self.limite_tolerancia = self.baseline_evasao + self.margem_evasao

        self.CRITICO_EVASAO = 0.10
        
    def evaluate_school(self, school_data: pd.Series) -> list:
        prescriptions = []
        evasao = school_data.get('dropout_rate', 0)
        
        if evasao >= self.CRITICO_EVASAO:
            req = {
                "axis": "Performance",
                "level": "School",
                "titulo": "Alerta Crítico: Evasão Escolar Severa.",
                "valor_destaque": f"{evasao * 100:.1f}%",
                "descricao": f"dos alunos abandonaram. A unidade rompeu o teto crítico (10%) e opera fora da margem de tolerância do Estado ({self.limite_tolerancia*100:.1f}%).",
                "recomendacao": "Acionar imediatamente o Conselho Tutelar e deflagrar protocolo local de Busca Ativa Escolar para reintegração urgente.",
                "valor_baseline": self.baseline_evasao
            }
            prescriptions.append(req)
            
        # Regra da Margem: Pegamos os vetores que "escaparam" do tubo de variância normal
        elif evasao > self.limite_tolerancia:
            req = {
                "axis": "Performance",
                "level": "School",
                "titulo": "Atenção: Risco de Desengajamento Escolar.",
                "valor_destaque": f"{evasao * 100:.1f}%",
                "descricao": f"de abandono. A unidade ultrapassou o limite superior da margem estatística do Estado ({self.limite_tolerancia*100:.1f}%).",
                "recomendacao": "Monitorar diários de classe para identificar alunos com faltas consecutivas (infrequência) e convocar os responsáveis.",
                "valor_baseline": self.baseline_evasao
            }
            prescriptions.append(req)
            
        return prescriptions

    def evaluate_municipality(self, municipality_data: pd.DataFrame) -> list:
        prescriptions = []
        evasao_rede = municipality_data['dropout_rate'].mean()
        
        if evasao_rede >= self.CRITICO_EVASAO:
            req = {
                "axis": "Performance",
                "level": "Municipality",
                "titulo": "Ruptura de Vínculo: Colapso na retenção de alunos.",
                "valor_destaque": f"{evasao_rede * 100:.1f}%",
                "descricao": f"é a taxa média de abandono da rede municipal, o dobro da tolerância aceitável.",
                "recomendacao": "Instituir comitê intersetorial (Educação, Saúde e Assistência Social) para mitigar os fatores socioeconômicos causadores da evasão.",
                "valor_baseline": self.baseline_evasao
            }
            prescriptions.append(req)
            
        elif evasao_rede > self.limite_tolerancia:
            req = {
                "axis": "Performance",
                "level": "Municipality",
                "titulo": "ZONA DE RISCO: Evasão sistêmica fora da curva.",
                "valor_destaque": f"{evasao_rede * 100:.1f}%",
                "descricao": f"de abandono médio municipal. A rede operou em desvio padrão acima da linha do Estado ({self.limite_tolerancia*100:.1f}%).",
                "recomendacao": "Fomentar campanhas municipais de conscientização e cruzar dados com o Cadastro Único para mapear famílias vulneráveis.",
                "valor_baseline": self.baseline_evasao
            }
            prescriptions.append(req)
            
        return prescriptions

    def evaluate_microregion(self, microregion_data: pd.DataFrame) -> list:
        prescriptions = []
        evasao_micro = microregion_data['dropout_rate'].mean()
        
        if evasao_micro > self.baseline_evasao:
            req = {
                "axis": "Performance",
                "level": "Microregion",
                "titulo": "Alerta Regional: Dificuldade de fixação do aluno.",
                "valor_destaque": f"{evasao_micro * 100:.1f}%",
                "descricao": f"dos alunos desta microrregião evadem. O indicador aponta para causas territoriais (como transporte escolar rural ou trabalho infantil sazonal).",
                "recomendacao": "Avaliar rotas de transporte escolar intermunicipal e alinhar calendários letivos com ciclos econômicos locais (ex: época de colheita).",
                "valor_baseline": self.baseline_evasao
            }
            prescriptions.append(req)
            
        return prescriptions

    def evaluate_mesoregion(self, mesoregion_data: pd.DataFrame) -> list:
        prescriptions = []
        evasao_meso = mesoregion_data['dropout_rate'].mean()
        
        if evasao_meso > self.baseline_evasao:
            req = {
                "axis": "Performance",
                "level": "Mesoregion",
                "titulo": "Diretriz Macrorregional: Déficit Crítico na Permanência Escolar.",
                "valor_destaque": f"{evasao_meso * 100:.1f}%",
                "descricao": f"da rede nesta mesorregião não conclui o ano letivo, puxando a média estadual para baixo.",
                "recomendacao": "Recomenda-se à SEDUC a expansão focalizada de programas de fomento à permanência (como o Cartão Escola 10) e expansão do Ensino Integral nas escolas desta macrorregião.",
                "valor_baseline": self.baseline_evasao
            }
            prescriptions.append(req)
            
        return prescriptions
    
    def evaluate_state(self, state_data: pd.DataFrame) -> list:
        prescriptions = []
        evasao_estado = state_data['dropout_rate'].mean()
        
        if evasao_estado > self.baseline_evasao:
            req = {
                "axis": "Performance", "level": "State", "tipo_insight": "Prescritivo",
                "titulo": "Diretriz Estadual: Crise de Retenção Escolar.",
                "valor_destaque": f"{evasao_estado * 100:.1f}%",
                "descricao": "é a taxa média de evasão do Estado. O volume de abandono requer uma resposta governamental unificada, superando a margem histórica tolerável.",
                "recomendacao": "Transformar programas de transferência de renda vinculados à educação (ex: poupança ensino médio) em políticas permanentes de Estado.",
                "valor_baseline": self.baseline_evasao
            }
            prescriptions.append(req)
            
        return prescriptions