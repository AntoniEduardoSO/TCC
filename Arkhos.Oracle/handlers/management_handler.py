import pandas as pd
from handlers.base_handler import BaseHandler

class ManagementHandler(BaseHandler):
    
    def evaluate_school(self, school_data: pd.Series) -> list:
        prescriptions = []
        instabilidade = school_data.get('teacher_instability_rating', 0)
        
        if instabilidade > 0.30:
            etapas_ativas = []
            if school_data.get('QT_DOC_INF', 0) > 0:
                etapas_ativas.append("Educação Infantil")
            if school_data.get('QT_DOC_FUND_AI', 0) > 0:
                etapas_ativas.append("Ensino Fundamental (Anos Iniciais)")
            if school_data.get('QT_DOC_FUND_AF', 0) > 0:
                etapas_ativas.append("Ensino Fundamental (Anos Finais)")
            if school_data.get('QT_DOC_MED', 0) > 0:
                etapas_ativas.append("Ensino Médio")

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
                    "recomendacao": f"Intervenção imediata. Exigir a alocação de professores efetivos {texto_etapas} para compor o núcleo pedagógico."
                }
                prescriptions.append(req)
                
            elif instabilidade >= 0.50:
                req = {
                    "axis": "Management",
                    "level": "School",
                    "titulo": "ALERTA GRAVE: Maioria docente provisória.",
                    "valor_destaque": f"{instabilidade * 100:.1f}%",
                    "descricao": "do quadro não possui vínculo efetivo, inviabilizando a continuidade do Projeto Político Pedagógico (PPP).",
                    "recomendacao": f"Solicitar substituição gradativa por servidores concursados, com prioridade {texto_etapas}."
                }
                prescriptions.append(req)
                
            else:
                req = {
                    "axis": "Management",
                    "level": "School",
                    "titulo": "Fragilidade no vínculo pedagógico.",
                    "valor_destaque": f"{instabilidade * 100:.1f}%",
                    "descricao": "do corpo docente atua sob contratos precários, gerando rotatividade que impacta o rendimento escolar.",
                    "recomendacao": f"Monitorar renovações e priorizar a lotação de efetivos {texto_etapas} nas próximas janelas de remoção da rede."
                }
                prescriptions.append(req)
            
        return prescriptions

    def evaluate_municipality(self, municipality_data: pd.DataFrame) -> list:
        prescriptions = []
        instabilidade_rede = municipality_data['teacher_instability_rating'].mean()
        carga_admin_rede = municipality_data['administrative_burden_rating'].mean()
        
        TETO_INSTABILIDADE = 0.40
        ALERTA_INSTABILIDADE = 0.35 
        
        if instabilidade_rede >= TETO_INSTABILIDADE:
            req = {
                "axis": "Management",
                "level": "Municipality",
                "titulo": "PENALIDADE MÁXIMA: Teto de Contratos Temporários Excedido.",
                "valor_destaque": f"{instabilidade_rede * 100:.1f}%",
                "descricao": f"da folha docente é provisória. O município ultrapassou o teto legal de {TETO_INSTABILIDADE*100:.0f}%, zerando o rating de estabilidade.",
                "recomendacao": "Bloqueio imediato de contratações temporárias. Deflagrar planejamento urgente para Concurso Público visando o provimento efetivo da rede."
            }
            prescriptions.append(req)
        elif instabilidade_rede >= ALERTA_INSTABILIDADE:
            req = {
                "axis": "Management",
                "level": "Municipality",
                "titulo": "ZONA DE RISCO: Aproximação do Teto de Instabilidade.",
                "valor_destaque": f"{instabilidade_rede * 100:.1f}%",
                "descricao": f"de contratos temporários, perigosamente próximo à penalidade máxima de {TETO_INSTABILIDADE*100:.0f}%.",
                "recomendacao": "Congelar renovações de contratos não-essenciais e iniciar mapeamento de vacâncias para convocação de concursados de cadastro reserva."
            }
            prescriptions.append(req)

        TETO_ADMIN = 0.15
        ALERTA_ADMIN = 0.12 
        
        if carga_admin_rede >= TETO_ADMIN:
            req = {
                "axis": "Management",
                "level": "Municipality",
                "titulo": "PENALIDADE MÁXIMA: Inchaço da Carga Administrativa.",
                "valor_destaque": f"{carga_admin_rede * 100:.1f}%",
                "descricao": f"dos gastos estão em funções burocráticas, rompendo o teto de {TETO_ADMIN*100:.0f}% e penalizando o município.",
                "recomendacao": "Realizar auditoria imediata na folha de pagamento para identificar desvios de função e realocar servidores."
            }
            prescriptions.append(req)
        elif carga_admin_rede >= ALERTA_ADMIN:
            req = {
                "axis": "Management",
                "level": "Municipality",
                "titulo": "ZONA DE RISCO: Alta Carga Administrativa.",
                "valor_destaque": f"{carga_admin_rede * 100:.1f}%",
                "descricao": f"da estrutura de custos é burocrática, aproximando-se do limite de {TETO_ADMIN*100:.0f}%.",
                "recomendacao": "Suspender gratificações administrativas e otimizar processos para frear despesas não-pedagógicas."
            }
            prescriptions.append(req)
            
        return prescriptions

    def evaluate_microregion(self, microregion_data: pd.DataFrame) -> list:
        prescriptions = []
        instabilidade_micro = microregion_data['teacher_instability_rating'].mean()
        carga_admin_micro = microregion_data['administrative_burden_rating'].mean()
        
        if instabilidade_micro >= 0.35:
            req = {
                "axis": "Management",
                "level": "Microregion",
                "titulo": "Alerta Regional: Precariedade do Vínculo Docente.",
                "valor_destaque": f"{instabilidade_micro * 100:.1f}%",
                "descricao": "é a média de contratos provisórios na microrregião, indicando dificuldade sistêmica de fixação de profissionais.",
                "recomendacao": "Fomentar a criação de um Consórcio Intermunicipal para a realização de Concurso Público Unificado, reduzindo custos de certame para prefeituras menores."
            }
            prescriptions.append(req)
            
        if carga_admin_micro >= 0.12:
            req = {
                "axis": "Management",
                "level": "Microregion",
                "titulo": "Alerta Regional: Ineficiência da Máquina Administrativa.",
                "valor_destaque": f"{carga_admin_micro * 100:.1f}%",
                "descricao": "da folha regional está alocada em burocracia. O inchaço administrativo é um padrão entre os municípios desta área.",
                "recomendacao": "Propor pacto regional de otimização de recursos, centralizando serviços contábeis e administrativos via consórcio para desonerar a folha da educação."
            }
            prescriptions.append(req)
            
        return prescriptions

    def evaluate_mesoregion(self, mesoregion_data: pd.DataFrame) -> list:
        prescriptions = []
        instabilidade_meso = mesoregion_data['teacher_instability_rating'].mean()
        carga_admin_meso = mesoregion_data['administrative_burden_rating'].mean()
        
        if instabilidade_meso >= 0.35:
            req = {
                "axis": "Management",
                "level": "Mesoregion",
                "titulo": "Diretriz de Macrorregião: Colapso do Plano de Carreira Docente.",
                "valor_destaque": f"{instabilidade_meso * 100:.1f}%",
                "descricao": "da rede atua sem estabilidade. Este índice macrorregional aponta para uma falha crítica na política de Estado de valorização do magistério.",
                "recomendacao": "Articular com a SEDUC e o Tribunal de Contas (TCE/AL) um plano de regularização de vínculos, incluindo previsão no Plano Plurianual (PPA) para fomento a concursos públicos."
            }
            prescriptions.append(req)
            
        if carga_admin_meso >= 0.12:
            req = {
                "axis": "Management",
                "level": "Mesoregion",
                "titulo": "Diretriz de Macrorregião: Risco Sistêmico de Compliance.",
                "valor_destaque": f"{carga_admin_meso * 100:.1f}%",
                "descricao": "da despesa educacional está concentrada em atividades meio, desviando recursos massivos das atividades fim (pedagógicas).",
                "recomendacao": "Recomenda-se auditoria programática e orientativa pelo TCE/AL nos municípios desta macrorregião para combater o desvio de finalidade na folha de pagamento da Educação."
            }
            prescriptions.append(req)
            
        return prescriptions