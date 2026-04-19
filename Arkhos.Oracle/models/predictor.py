import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
import shap  # pip install shap
import random # Adicionado para variação dinâmica de linguagem

class RiskPredictor:
    def __init__(self, baselines: dict):
        self.baselines = baselines
        
        self.features = [
            'acessibility_rating', 'recreation_rating', 'wellbeing_rating', 
            'human_support_rating', 'management_rating', 'age_grade_distortion_rating', 
            'pedagogical_rating', 'teacher_stress_rating', 'teacher_instability_rating', 
            'administrative_burden_rating', 'spending_per_student', 'spending_per_teacher', 
            'pedagogical_spending_per_student', 'infrastructure_spending_per_student', 
            'meal_spending_per_student', 'transport_spending_per_student'
        ]
        
        self.dicionario_viloes = {
            'acessibility_rating': 'precariedade na acessibilidade estrutural',
            'recreation_rating': 'falta de espaços adequados para recreação',
            'wellbeing_rating': 'baixo índice de bem-estar no ambiente escolar',
            'human_support_rating': 'déficit agudo no suporte humano e psicológico',
            'management_rating': 'fragilidade da gestão escolar participativa',
            'age_grade_distortion_rating': 'alta distorção idade-série (atraso escolar endêmico)',
            'pedagogical_rating': 'insuficiência severa de recursos pedagógicos',
            'teacher_stress_rating': 'nível crítico de sobrecarga e estresse docente',
            'teacher_instability_rating': 'alta rotatividade e precariedade dos vínculos de professores',
            'administrative_burden_rating': 'excesso de carga burocrática',
            'spending_per_student': 'déficit orçamentário no investimento geral por aluno',
            'spending_per_teacher': 'baixo investimento na valorização docente',
            'pedagogical_spending_per_student': 'escassez de investimento direto em pedagogia',
            'infrastructure_spending_per_student': 'falta de repasses para custeio de infraestrutura',
            'meal_spending_per_student': 'deficiência no custeio nutricional da merenda',
            'transport_spending_per_student': 'falhas logísticas de transporte escolar'
        }

        self.recomendacoes_especificas = {
            'acessibility_rating': 'Priorizar captação de recursos do PDDE Acessível para readequação arquitetônica (rampas, banheiros PNE).',
            'recreation_rating': 'Incluir construção ou reforma de espaços de convivência no próximo Plano de Ações Articuladas (PAR) do MEC.',
            'wellbeing_rating': 'Acionar a infraestrutura estadual para garantir saneamento básico e fornecimento regular de água potável.',
            'human_support_rating': 'Articular com a Secretaria de Saúde/Assistência Social parceria para disponibilizar psicólogos e assistentes sociais (Lei 13.935/2019).',
            'management_rating': 'Fomentar a implementação de grêmios estudantis e promover eleições diretas para o Conselho Escolar.',
            'age_grade_distortion_rating': 'Implementar turmas de aceleração de aprendizagem e programas intensivos de correção de fluxo.',
            'pedagogical_rating': 'Solicitar ampliação de acervo via PNLD e planejar a implantação de laboratórios móveis de informática.',
            'teacher_stress_rating': 'Revisar o dimensionamento de alunos por turma e promover ações de saúde mental ocupacional.',
            'teacher_instability_rating': 'Reduzir a dependência de contratos temporários (monitores/terceirizados) via planejamento para concurso público.',
            'administrative_burden_rating': 'Incentivar a adoção integral de diários de classe digitais para desburocratizar a rotina do docente.',
            'spending_per_student': 'Revisar a matriz de alocação do FUNDEB para garantir equidade na distribuição per capita inter-regional.',
            'spending_per_teacher': 'Assegurar o cumprimento do piso salarial e promover progressão de carreira com base em formação continuada.',
            'pedagogical_spending_per_student': 'Remanejar rubricas orçamentárias descentralizadas para focar estritamente na aquisição de material de apoio didático.',
            'infrastructure_spending_per_student': 'Realizar auditoria técnica nos repasses de manutenção e priorizar zeladoria escolar imediata.',
            'meal_spending_per_student': 'Revisar contratos do PNAE, garantindo o percentual mínimo de compras da agricultura familiar local.',
            'transport_spending_per_student': 'Otimizar rotas do PNATE e exigir manutenção preventiva da frota terceirizada.'
        }

        self.targets_config = [
            {
                "target_col": "dropout_rate",
                "axis": "Performance",
                "baseline_mean": baselines.get("evasao_mean", 0.05),
                "baseline_std": baselines.get("evasao_std", 0.02),
                "is_bad_when": "HIGH", 
                "titulo": "Alto risco de evasão futura.",
                "desc_base": "é a projeção do risco de abandono"
            },
            {
                "target_col": "acessibility_rating",
                "axis": "Infrastructure",
                "baseline_mean": baselines.get("acessibilidade_mean", 0.5),
                "baseline_std": baselines.get("acessibilidade_std", 0.1),
                "is_bad_when": "LOW", 
                "titulo": "Degradação projetada na infraestrutura.",
                "desc_base": "é a projeção do índice estrutural, caindo para níveis críticos"
            },
            {
                "target_col": "teacher_instability_rating",
                "axis": "Management",
                "baseline_mean": baselines.get("instabilidade_mean", 0.6),
                "baseline_std": baselines.get("instabilidade_std", 0.15),
                "is_bad_when": "LOW", 
                "titulo": "Risco de ruptura no quadro docente.",
                "desc_base": "é a nota projetada de estabilidade, apontando para grave rotatividade futura"
            }
        ]

    def _prepare_data(self, df_historico: pd.DataFrame):
        df = df_historico.copy()
        df = df.dropna(subset=['dropout_rate', 'approval_rate', 'failure_rate'])
        df = df.sort_values(by=['id_escola', 'ano'])
        df['target_evasao_futura'] = df.groupby('id_escola')['dropout_rate'].shift(-1)
        return df.dropna(subset=['target_evasao_futura']).copy()
    
    def _extrair_detalhe_granular(self, vilao: str, id_escola: float, df_detalhado: pd.DataFrame) -> str:
        dados_escola = df_detalhado[df_detalhado['id_escola_fk'] == id_escola]
        if dados_escola.empty: return ""
            
        linha = dados_escola.iloc[0]
        faltas = []
        
        if vilao == 'recreation_rating':
            if not linha.get('IN_QUADRA_ESPORTES', 1): faltas.append("quadra desportiva")
            if not linha.get('IN_PATIO_COBERTO', 1): faltas.append("pátio coberto")
            if not linha.get('IN_PARQUE_INFANTIL', 1): faltas.append("parque infantil")
            if faltas: return f" Especificamente, constata-se a ausência de {', '.join(faltas)} na unidade."

        elif vilao == 'wellbeing_rating':
            if not linha.get('IN_AGUA_POTAVEL', 1): faltas.append("acesso direto a água potável")
            if not linha.get('IN_REFEITORIO', 1): faltas.append("refeitório estruturado")
            if not linha.get('IN_ESGOTO_REDE_PUBLIC', 1): faltas.append("ligação à rede pública de esgoto")
            if faltas: return f" O cenário é agravado pela falta de {', '.join(faltas)}."

        elif vilao == 'human_support_rating':
            if not linha.get('QT_PROF_PSICOLOGO', 1): faltas.append("psicólogo escolar")
            if not linha.get('QT_PROF_ASSIST_SOCIAL', 1): faltas.append("assistente social")
            if faltas: return f" A unidade não conta com suporte de {', '.join(faltas)}."

        elif vilao == 'pedagogical_rating':
            if not linha.get('IN_BIBLIOTECA', 1): faltas.append("biblioteca")
            if not linha.get('IN_LABORATORIO_INFORMATICA', 1): faltas.append("laboratório de informática")
            if faltas: return f" O déficit inclui a falta de {', '.join(faltas)}."

        elif vilao == 'management_rating':
            if not linha.get('IN_ORGAO_CONSELHO_ESCOLAR', 1): faltas.append("conselho escolar ativo")
            if not linha.get('IN_ORGAO_ASS_PAIS_MESTRES', 1): faltas.append("associação de pais e mestres")
            if faltas: return f" Falta consolidação de gestão participativa com a comunidade ({', '.join(faltas)})."

        return ""

    def _extrair_detalhe_agregado(self, vilao: str, id_alvo: float, df_detalhado: pd.DataFrame, nivel_coluna: str) -> str:
        dados_rede = df_detalhado[df_detalhado[nivel_coluna] == id_alvo]
        if dados_rede.empty: return ""

        def maioria_carece(coluna):
            return dados_rede[coluna].mean() < 0.5 if coluna in dados_rede.columns else False
            
        faltas = []
        if vilao == 'recreation_rating':
            if maioria_carece('IN_QUADRA_ESPORTES'): faltas.append("quadras desportivas")
            if maioria_carece('IN_PATIO_COBERTO'): faltas.append("pátios cobertos")
            if faltas: return f" Especificamente, a maioria das escolas desta rede carece de {', '.join(faltas)}."
            
        elif vilao == 'management_rating':
            if maioria_carece('IN_ORGAO_CONSELHO_ESCOLAR'): faltas.append("conselhos escolares")
            if faltas: return f" Há baixa consolidação de gestão participativa territorial, faltando {', '.join(faltas)}."

        return ""

    def _gerar_texto_explicativo(self, viloes: list, acao_shap: str) -> tuple:
        """Motor NLG (Natural Language Generation) usando terminologia acadêmica estrita de ML"""
        if len(viloes) >= 2:
            vilao_1, vilao_2 = viloes[0], viloes[1]
            motivo_1 = self.dicionario_viloes.get(vilao_1, "fator estrutural")
            motivo_2 = self.dicionario_viloes.get(vilao_2, "fator secundário")
            
            templates = [
                f"O motor de predição detectou que a combinação de {motivo_1} com a {motivo_2} {acao_shap} severamente o cenário.",
                f"O algoritmo aponta que a {motivo_1} é o principal agravante, sendo potencializada pela {motivo_2}.",
                f"A análise multivariada revela que a {motivo_1}, aliada à {motivo_2}, é a força motriz que {acao_shap} este indicador."
            ]
            return random.choice(templates), vilao_1 
        
        elif len(viloes) == 1:
            vilao_1 = viloes[0]
            motivo_1 = self.dicionario_viloes.get(vilao_1, "fator primário")
            templates = [
                f"O modelo estatístico isolou a {motivo_1} como a variável primária que {acao_shap} a projeção.",
                f"O modelo preditivo detectou forte correlação apontando que a {motivo_1} isoladamente {acao_shap} o cenário."
            ]
            return random.choice(templates), vilao_1
            
        return "", None

    def generate_shap_prescriptions(self, df_completo: pd.DataFrame, df_detalhado: pd.DataFrame, ano_atual: int = 2024) -> list:
        todas_prescricoes = []
        
        for config in self.targets_config:
            target = config["target_col"]
            print(f"\n[ML] Iniciando modelagem preditiva para: {target}")
            
            df = df_completo.copy()
            df = df.dropna(subset=[target])
            df = df.sort_values(by=['id_escola', 'ano'])
            df[f'target_futuro'] = df.groupby('id_escola')[target].shift(-1)
            
            df_treino = df.dropna(subset=[f'target_futuro']).copy()
            df_2024 = df[df['ano'] == ano_atual].dropna(subset=[target]).copy()
            
            X_train = df_treino[self.features].fillna(0)
            y_train = df_treino['target_futuro']
            X_2024 = df_2024[self.features].fillna(0)
            
            if X_2024.empty or X_train.empty: continue

            # Random Forest e SHAP
            model = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1)
            model.fit(X_train, y_train)
            predicoes_futuras = model.predict(X_2024)
            
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X_2024)
            
            df_2024 = df_2024.reset_index(drop=True)
            df_2024['risco_predito'] = predicoes_futuras
            for i, feat in enumerate(self.features): df_2024[f'shap_{feat}'] = shap_values[:, i]
                
            # --- 1. AVISOS ESCOLAS ---
            for idx, row in df_2024.iterrows():
                predicao = row['risco_predito']
                escola_id = row.get('id_escola', row.get('id_escola_fk'))
                pesos = {feat: row[f'shap_{feat}'] for feat in self.features}
                
                alerta_disparado = False
                viloes_ativos = []
                
                # Nova lógica: Capturando os Top Vilões com base na direção do prejuízo
                if config["is_bad_when"] == "HIGH":
                    if predicao > (config["baseline_mean"] + config["baseline_std"]):
                        alerta_disparado = True
                        sorted_feats = sorted(pesos.items(), key=lambda item: item[1], reverse=True)
                        viloes_ativos = [f for f, v in sorted_feats if v > 0] # Pega os que puxam pra cima
                else:
                    if predicao < (config["baseline_mean"] - config["baseline_std"]):
                        alerta_disparado = True
                        sorted_feats = sorted(pesos.items(), key=lambda item: item[1]) # Ascendente
                        viloes_ativos = [f for f, v in sorted_feats if v < 0] # Pega os que empurram pra baixo
                
                if alerta_disparado and viloes_ativos:
                    acao_shap = "agrava" if config["is_bad_when"] == "HIGH" else "puxa para baixo"
                    texto_ia, vilao_principal = self._gerar_texto_explicativo(viloes_ativos, acao_shap)
                    detalhe_micro = self._extrair_detalhe_granular(vilao_principal, escola_id, df_detalhado) if vilao_principal else ""
                    
                    # Recupera a recomendação dinâmica para o nível da escola
                    rec_direcionada = self.recomendacoes_especificas.get(vilao_principal, "Realizar auditoria técnica para mapear a raiz estrutural do déficit projetado.")

                    req = {
                        "axis": config["axis"], "level": "School", "ano": ano_atual, "tipo_insight": "Preditivo",
                        "titulo": f"ALERTA PREDITIVO: {config['titulo']}",
                        "valor_destaque": f"{predicao * 100:.1f}%" if "rate" in target else f"{predicao:.2f}",
                        "descricao": f"{config['desc_base']}. {texto_ia}{detalhe_micro}",
                        "recomendacao": rec_direcionada,
                        "valor_baseline": config["baseline_mean"], "id_alvo": escola_id
                    }
                    todas_prescricoes.append(req)

            # --- 2. AVISOS MACRO (Agregações) ---
            def processar_nivel_agregado(nivel_id_col, nome_level, titulo_prefixo, extrator_detalhe):
                if nivel_id_col not in df_2024.columns: return
                for alvo_id, alvo_df in df_2024.groupby(nivel_id_col):
                    predicao_media = alvo_df['risco_predito'].mean()
                    alerta_disparado = False
                    viloes_ativos = []
                    
                    mean_shaps = alvo_df[[f'shap_{f}' for f in self.features]].mean()
                    
                    if config["is_bad_when"] == "HIGH":
                        if predicao_media > (config["baseline_mean"] + (config["baseline_std"] * 0.5)):
                            alerta_disparado = True
                            sorted_shaps = mean_shaps.sort_values(ascending=False)
                            viloes_ativos = [f.replace('shap_', '') for f, v in sorted_shaps.items() if v > 0]
                    else:
                        if predicao_media < (config["baseline_mean"] - (config["baseline_std"] * 0.5)):
                            alerta_disparado = True
                            sorted_shaps = mean_shaps.sort_values(ascending=True)
                            viloes_ativos = [f.replace('shap_', '') for f, v in sorted_shaps.items() if v < 0]

                    if alerta_disparado and viloes_ativos:
                        acao_shap = "agrava sistemicamente" if config["is_bad_when"] == "HIGH" else "corrói o índice regional"
                        texto_ia, vilao_principal = self._gerar_texto_explicativo(viloes_ativos, acao_shap)
                        detalhe_rede = self._extrair_detalhe_agregado(vilao_principal, alvo_id, df_detalhado, nivel_id_col) if vilao_principal else ""
                        
                        # Recupera a recomendação dinâmica e adapta para uma visão mais sistêmica/macrorregional
                        fallback_macro = "Articular força-tarefa intersetorial e prever suplementação orçamentária para o gargalo apontado."
                        rec_direcionada_macro = self.recomendacoes_especificas.get(vilao_principal, fallback_macro)
                        
                        req = {
                            "axis": config["axis"], "level": nome_level, "ano": ano_atual, "tipo_insight": "Preditivo",
                            "titulo": f"{titulo_prefixo}: {config['titulo']}",
                            "valor_destaque": f"{predicao_media * 100:.1f}%" if "rate" in target else f"{predicao_media:.2f}",
                            "descricao": f"é a projeção média para a rede. {texto_ia}{detalhe_rede}",
                            "recomendacao": rec_direcionada_macro, # <-- Alterado aqui para usar a dinâmica no Macro também!
                            "valor_baseline": config["baseline_mean"], "id_alvo": alvo_id
                        }
                        todas_prescricoes.append(req)

            processar_nivel_agregado('id_municipio', 'Municipality', 'ALERTA SISTÊMICO', 'id_municipio')
            processar_nivel_agregado('id_microrregiao', 'Microregion', 'ALERTA REGIONAL', 'id_microrregiao')
            processar_nivel_agregado('id_mesorregiao', 'Mesoregion', 'DIRETRIZ MACRO', 'id_mesorregiao')

            predicao_media_estado = df_2024['risco_predito'].mean()
            alerta_disparado = False
            viloes_ativos = []
            
            mean_shaps = df_2024[[f'shap_{f}' for f in self.features]].mean()
            
            if config["is_bad_when"] == "HIGH":
                if predicao_media_estado > (config["baseline_mean"] + (config["baseline_std"] * 0.25)): # Margem de tolerância menor para o Estado
                    alerta_disparado = True
                    sorted_shaps = mean_shaps.sort_values(ascending=False)
                    viloes_ativos = [f.replace('shap_', '') for f, v in sorted_shaps.items() if v > 0]
            else:
                if predicao_media_estado < (config["baseline_mean"] - (config["baseline_std"] * 0.25)):
                    alerta_disparado = True
                    sorted_shaps = mean_shaps.sort_values(ascending=True)
                    viloes_ativos = [f.replace('shap_', '') for f, v in sorted_shaps.items() if v < 0]

            if alerta_disparado and viloes_ativos:
                acao_shap = "agrava sistemicamente" if config["is_bad_when"] == "HIGH" else "corrói o índice global"
                texto_ia, vilao_principal = self._gerar_texto_explicativo(viloes_ativos, acao_shap)
                
                # Para o estado, o detalhe é gerado em cima do df inteiro (podemos passar id_alvo como None ou 27)
                detalhe_rede = " O impacto deste fator já se reflete na maioria das mesorregiões." 
                
                rec_direcionada_macro = self.recomendacoes_especificas.get(vilao_principal, "Articular com a casa civil plano de mitigação global.")
                
                req = {
                    "axis": config["axis"], "level": "State", "ano": ano_atual, "tipo_insight": "Preditivo",
                    "titulo": f"Diretriz Estadual: {config['titulo']}",
                    "valor_destaque": f"{predicao_media_estado * 100:.1f}%" if "rate" in target else f"{predicao_media_estado:.2f}",
                    "descricao": f"é a projeção média de risco para todo o Estado. {texto_ia}{detalhe_rede}",
                    "recomendacao": f"Pauta Governamental Urgente: {rec_direcionada_macro}",
                    "valor_baseline": config["baseline_mean"], "id_alvo": 27.0
                }
                todas_prescricoes.append(req)

        return todas_prescricoes