import pandas as pd
from handlers.base_handler import BaseHandler

class ManagementHandler(BaseHandler):

    def __init__(self, baselines: dict):

        self.baseline_instabilidade = baselines.get('instabilidade', 0.35)
        self.baseline_admin = baselines.get('admin', 0.12)
        
        # Constantes de Compliance (Limites Legais/Graves - Fixos)
        self.TETO_INSTABILIDADE_REDE = 0.40
        self.TETO_ADMIN_REDE = 0.15
        self.CRITICO_ESCOLA = 0.50
    
    def evaluate_school(self, school_data: pd.Series) -> list:
        prescriptions = []
        instabilidade = school_data.get('teacher_instability_rating', 0)
        
        if instabilidade > self.baseline_instabilidade or instabilidade >= self.CRITICO_ESCOLA:
            etapas_ativas = []
            if school_data.get('QT_DOC_INF', 0) > 0: etapas_ativas.append("Educação Infantil")
            if school_data.get('QT_DOC_FUND_AI', 0) > 0: etapas_ativas.append("Ensino Fundamental (Anos Iniciais)")
            if school_data.get('QT_DOC_FUND_AF', 0) > 0: etapas_ativas.append("Ensino Fundamental (Anos Finais)")
            if school_data.get('QT_DOC_MED', 0) > 0: etapas_ativas.append("Ensino Médio")

            if len(etapas_ativas) > 1:
                texto_etapas = "para as etapas de " + ", ".join(etapas_ativas[:-1]) + " e " + etapas_ativas[-1]
            elif len(etapas_ativas) == 1:
                texto_etapas = "para a etapa de " + etapas_ativas[0]
            else:
                texto_etapas = "para as disciplinas nucleares"
                
            if instabilidade == 1.0:
                req = {
                    "axis": "Management",
                    "level": "School",
                    "titulo": "ANOMALIA CRÍTICA: Ausência total de docentes efetivos.",
                    "valor_destaque": "100%",
                    "descricao": "do corpo docente desta unidade é temporário ou terceirizado. A escola opera sem nenhum servidor de carreira.",
                    "recomendacao": f"Intervenção imediata. Exigir a alocação de professores efetivos {texto_etapas} para compor o núcleo pedagógico.",
                    "valor_baseline": self.baseline_instabilidade
                }
                prescriptions.append(req)
                
            elif instabilidade >= self.CRITICO_ESCOLA:
                req = {
                    "axis": "Management",
                    "level": "School",
                    "titulo": "Alerta Crítico: Maioria Docente com Vínculo Provisório.",
                    "valor_destaque": f"{instabilidade * 100:.1f}%",
                    "descricao": "do quadro não possui vínculo efetivo, inviabilizando a continuidade do Projeto Político Pedagógico (PPP).",
                    "recomendacao": f"Solicitar substituição gradativa por servidores concursados, com prioridade {texto_etapas}.",
                    "valor_baseline": self.baseline_instabilidade
                }
                prescriptions.append(req)
                
            else:
                req = {
                    "axis": "Management",
                    "level": "School",
                    "titulo": "Atenção: Fragilidade no Vínculo Pedagógico.",
                    "valor_destaque": f"{instabilidade * 100:.1f}%",
                    "descricao": f"do corpo docente atua sob contratos precários, operando acima da mediana estadual ({self.baseline_instabilidade*100:.1f}%).",
                    "recomendacao": f"Monitorar renovações e priorizar a lotação de efetivos {texto_etapas} nas próximas janelas de remoção da rede.",
                    "valor_baseline": self.baseline_instabilidade
                }
                prescriptions.append(req)
            
        return prescriptions

    def evaluate_municipality(self, municipality_data: pd.DataFrame) -> list:
        prescriptions = []
        instabilidade_rede = municipality_data['teacher_instability_rating'].mean()
        carga_admin_rede = municipality_data['administrative_burden_rating'].mean()
        

        if instabilidade_rede >= self.TETO_INSTABILIDADE_REDE:
            req = {
                "axis": "Management",
                "level": "Municipality",
                "titulo": "Alerta Crítico: Rompimento do Teto de Contratos Temporários.",
                "valor_destaque": f"{instabilidade_rede * 100:.1f}%",
                "descricao": f"da folha docente é provisória. O município ultrapassou o teto legal de {self.TETO_INSTABILIDADE_REDE*100:.0f}%.",
                "recomendacao": "Bloqueio imediato de contratações temporárias. Deflagrar planejamento urgente para Concurso Público visando o provimento efetivo da rede.",
                "valor_baseline": self.baseline_instabilidade
            }
            prescriptions.append(req)
        elif instabilidade_rede > self.baseline_instabilidade:
            req = {
                "axis": "Management",
                "level": "Municipality",
                "titulo": "ZONA DE RISCO: Instabilidade de RH acima da média.",
                "valor_destaque": f"{instabilidade_rede * 100:.1f}%",
                "descricao": f"de contratos temporários. A rede está pior que a mediana do Estado ({self.baseline_instabilidade*100:.1f}%) e aproxima-se de limites críticos.",
                "recomendacao": "Congelar renovações de contratos não-essenciais e iniciar mapeamento de vacâncias para convocação de concursados de cadastro reserva.",
                "valor_baseline": self.baseline_instabilidade
            }
            prescriptions.append(req)

        if carga_admin_rede >= self.TETO_ADMIN_REDE:
            req = {
                "axis": "Management",
                "level": "Municipality",
                "titulo": "PENALIDADE MÁXIMA: Inchaço da Carga Administrativa.",
                "valor_destaque": f"{carga_admin_rede * 100:.1f}%",
                "descricao": f"dos gastos estão em funções burocráticas, rompendo o teto de compliance de {self.TETO_ADMIN_REDE*100:.0f}%.",
                "recomendacao": "Realizar auditoria imediata na folha de pagamento para identificar desvios de função e realocar servidores.",
                "valor_baseline": self.baseline_admin
            }
            prescriptions.append(req)


        elif carga_admin_rede > self.baseline_admin:
            req = {
                "axis": "Management",
                "level": "Municipality",
                "titulo": "ZONA DE RISCO: Alta Carga Administrativa.",
                "valor_destaque": f"{carga_admin_rede * 100:.1f}%",
                "descricao": f"da estrutura de custos é burocrática, operando acima da linha de base do Estado ({self.baseline_admin*100:.1f}%).",
                "recomendacao": "Suspender gratificações administrativas e otimizar processos para frear despesas não-pedagógicas.",
                "valor_baseline": self.baseline_admin
            }
            prescriptions.append(req)
            
        return prescriptions

    def evaluate_microregion(self, microregion_data: pd.DataFrame) -> list:
        prescriptions = []
        instabilidade_micro = microregion_data['teacher_instability_rating'].mean()
        carga_admin_micro = microregion_data['administrative_burden_rating'].mean()
        
        if instabilidade_micro > self.baseline_instabilidade:
            req = {
                "axis": "Management",
                "level": "Microregion",
                "titulo": "Alerta Regional: Precariedade do Vínculo Docente.",
                "valor_destaque": f"{instabilidade_micro * 100:.1f}%",
                "descricao": f"é a média provisória na microrregião, pior que a linha base do Estado ({self.baseline_instabilidade*100:.1f}%), indicando dificuldade territorial de fixação de profissionais.",
                "recomendacao": "Fomentar a criação de um Consórcio Intermunicipal para a realização de Concurso Público Unificado, reduzindo custos de certame para prefeituras menores.",
                "valor_baseline": self.baseline_instabilidade
            }
            prescriptions.append(req)
            
        if carga_admin_micro > self.baseline_admin:
            req = {
                "axis": "Management",
                "level": "Microregion",
                "titulo": "Alerta Regional: Ineficiência da Máquina Administrativa.",
                "valor_destaque": f"{carga_admin_micro * 100:.1f}%",
                "descricao": f"da folha regional está alocada em burocracia (superior aos {self.baseline_admin*100:.1f}% do Estado). O inchaço é um padrão territorial.",
                "recomendacao": "Propor pacto regional de otimização de recursos, centralizando serviços contábeis e administrativos via consórcio para desonerar a folha da educação.",
                "valor_baseline": self.baseline_admin
            }
            prescriptions.append(req)
            
        return prescriptions

    def evaluate_mesoregion(self, mesoregion_data: pd.DataFrame) -> list:
        prescriptions = []
        instabilidade_meso = mesoregion_data['teacher_instability_rating'].mean()
        carga_admin_meso = mesoregion_data['administrative_burden_rating'].mean()
        
        if instabilidade_meso > self.baseline_instabilidade:
            req = {
                "axis": "Management",
                "level": "Mesoregion",
                "titulo": "Diretriz Macrorregional: Colapso no Plano de Carreira Docente.",
                "valor_destaque": f"{instabilidade_meso * 100:.1f}%",
                "descricao": f"da rede atua sem estabilidade, distorcendo a linha do Estado ({self.baseline_instabilidade*100:.1f}%). Aponta falha crítica na política de valorização do magistério.",
                "recomendacao": "Articular com a SEDUC e o Tribunal de Contas (TCE/AL) um plano de regularização de vínculos, incluindo previsão no Plano Plurianual (PPA).",
                "valor_baseline": self.baseline_instabilidade
            }
            prescriptions.append(req)
            
        if carga_admin_meso > self.baseline_admin:
            req = {
                "axis": "Management",
                "level": "Mesoregion",
                "titulo": "Diretriz Macrorregional: Risco Sistêmico de Compliance Administrativo.",
                "valor_destaque": f"{carga_admin_meso * 100:.1f}%",
                "descricao": "da despesa educacional está concentrada em atividades meio, desviando recursos massivos das atividades fim (pedagógicas).",
                "recomendacao": "Recomenda-se auditoria programática e orientativa pelo TCE/AL nos municípios desta macrorregião para combater o desvio de finalidade.",
                "valor_baseline": self.baseline_admin
            }
            prescriptions.append(req)
            
        return prescriptions
    

    def evaluate_state(self, state_data: pd.DataFrame) -> list:
        prescriptions = []
        instabilidade_estado = state_data['teacher_instability_rating'].mean()
        
        if instabilidade_estado > self.baseline_instabilidade:
            req = {
                "axis": "Management", "level": "State", "tipo_insight": "Prescritivo",
                "titulo": "Diretriz Estadual: Precarização do Vínculo Docente.",
                "valor_destaque": f"{instabilidade_estado * 100:.1f}%",
                "descricao": f"da rede pública estadual atua sob contratos temporários ou terceirizados. A precarização generalizada compromete a continuidade pedagógica.",
                "recomendacao": "Constituir comissão conjunta (Governo, Assembleia Legislativa e Sindicatos) para o planejamento e homologação de um novo Concurso Público Estadual.",
                "valor_baseline": self.baseline_instabilidade
            }
            prescriptions.append(req)
            
        return prescriptions