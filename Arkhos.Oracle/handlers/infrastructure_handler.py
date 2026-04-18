import pandas as pd
from handlers.base_handler import BaseHandler

class InfrastructureHandler(BaseHandler):
    
    def evaluate_school(self, school_data: pd.Series) -> list:
        prescriptions = []
        rating = school_data.get('acessibility_rating', 1.0)
        
        if rating < 0.5:
            missing_items = []
            if not school_data.get('IN_SALA_ATENDIMENTO_ESPECIAL', 1):
                missing_items.append("Salas de AEE")
            if not school_data.get('IN_BANHEIRO_PNE', 1):
                missing_items.append("banheiros acessíveis (PNE)")
            if not school_data.get('IN_ACESSIBILIDADE_RAMPAS', 1):
                missing_items.append("rampas de acesso")
            if not school_data.get('IN_ACESSIBILIDADE_VAO_LIVRE', 1):
                missing_items.append("portas com vão livre adequado")
            if not school_data.get('IN_ACESSIBILIDADE_CORRIMAO', 1):
                missing_items.append("corrimãos")
            if not school_data.get('IN_ACESSIBILIDADE_PISOS_TATEIS', 1):
                missing_items.append("pisos táteis")
            if not school_data.get('IN_ACESSIBILIDADE_SINAL_TATIL', 1):
                missing_items.append("sinalização tátil")
            if not school_data.get('IN_ACESSIBILIDADE_SINAL_SONORO', 1):
                missing_items.append("sinalização sonora")
                
            if missing_items:
                if len(missing_items) > 3:
                    top_itens = missing_items[:3]
                    outros_count = len(missing_items) - 3
                    
                    itens_principais = ", ".join(top_itens[:-1]) + f" e {top_itens[-1]}"
                    
                    recomendacao = (f"Priorizar a implementação de adequações críticas, como {itens_principais}, "
                                    f"além de realizar outras {outros_count} intervenções complementares na unidade escolar.")
                                    
                elif len(missing_items) > 1:
                    itens_formatados = ", ".join(missing_items[:-1]) + " e " + missing_items[-1]
                    recomendacao = f"Priorizar a implementação de {itens_formatados} na unidade escolar."
                    
                else:
                    itens_formatados = missing_items[0]
                    recomendacao = f"Concentrar esforços na implementação de {itens_formatados} na unidade escolar."


                valor_destaque = f"{rating * 100:.1f}%"
                
                req = {
                    "axis": "Infrastructure",
                    "level": "School",
                    "titulo": "Defasagem na acessibilidade estrutural.",
                    "valor_destaque": valor_destaque,
                    "descricao": "é o índice atual de acessibilidade desta unidade, operando severamente abaixo da linha de base de inclusão.",
                    "recomendacao": recomendacao
                }
                prescriptions.append(req)
                
        return prescriptions

    def evaluate_municipality(self, municipality_data: pd.DataFrame) -> list:
        prescriptions = []
        
        # Verifica a média geral do município
        avg_acessibility = municipality_data['acessibility_rating'].mean()
        
        if avg_acessibility < 0.5:
            total_escolas = len(municipality_data)
            
            # Calcula o percentual de escolas defasadas (nota < 0.5)
            escolas_defasadas = len(municipality_data[municipality_data['acessibility_rating'] < 0.5])
            percentual = (escolas_defasadas / total_escolas) * 100
            
            # Dicionário para mapear as colunas do banco para nomes amigáveis
            colunas_amigaveis = {
                'IN_SALA_ATENDIMENTO_ESPECIAL': 'Salas de AEE',
                'IN_BANHEIRO_PNE': 'banheiros acessíveis (PNE)',
                'IN_ACESSIBILIDADE_RAMPAS': 'rampas de acesso',
                'IN_ACESSIBILIDADE_VAO_LIVRE': 'portas com vão livre',
                'IN_ACESSIBILIDADE_CORRIMAO': 'corrimãos',
                'IN_ACESSIBILIDADE_PISOS_TATEIS': 'pisos táteis',
                'IN_ACESSIBILIDADE_SINAL_TATIL': 'sinalização tátil',
                'IN_ACESSIBILIDADE_SINAL_SONORO': 'sinalização sonora'
            }

            # Conta quantas escolas NÃO possuem (valor == 0) cada item na rede
            faltas_por_item = {}
            for col, nome_amigavel in colunas_amigaveis.items():
                if col in municipality_data.columns:
                    qtd_falta = total_escolas - municipality_data[col].sum()
                    if qtd_falta > 0:
                        faltas_por_item[nome_amigavel] = qtd_falta

            # Ordena para pegar os problemas mais comuns (maior número de faltas)
            itens_ordenados = sorted(faltas_por_item.items(), key=lambda x: x[1], reverse=True)
            
            # Pega apenas os nomes dos 3 itens mais críticos no município
            top_3_nomes = [item[0] for item in itens_ordenados[:3]]

            # Monta a recomendação dinâmica para o Gestor Municipal
            if len(top_3_nomes) > 1:
                itens_formatados = ", ".join(top_3_nomes[:-1]) + f" e {top_3_nomes[-1]}"
                recomendacao = f"Articular edital unificado focado nas carências sistêmicas da rede, como a implementação de {itens_formatados}."
            elif len(top_3_nomes) == 1:
                recomendacao = f"Articular edital de adequação focado na principal carência da rede: implementação de {top_3_nomes[0]}."
            else:
                recomendacao = "Realizar auditoria estrutural para mapear necessidades de acessibilidade nas unidades escolares."
            
            req = {
                "axis": "Infrastructure",
                "level": "Municipality",
                "titulo": "Defasagem sistêmica na acessibilidade estrutural.",
                "valor_destaque": f"{percentual:.1f}%",
                "descricao": "das escolas da rede municipal operam severamente abaixo da linha de base de inclusão.",
                "recomendacao": recomendacao
            }
            prescriptions.append(req)
            
        return prescriptions
    

    def evaluate_microregion(self, microregion_data: pd.DataFrame) -> list:
        prescriptions = []
        
        avg_acessibility = microregion_data['acessibility_rating'].mean()
        
        if avg_acessibility < 0.5:
            total_escolas = len(microregion_data)
            escolas_defasadas = len(microregion_data[microregion_data['acessibility_rating'] < 0.5])
            percentual = (escolas_defasadas / total_escolas) * 100
            
            colunas_amigaveis = {
                'IN_SALA_ATENDIMENTO_ESPECIAL': 'Salas de AEE',
                'IN_BANHEIRO_PNE': 'banheiros acessíveis (PNE)',
                'IN_ACESSIBILIDADE_RAMPAS': 'rampas de acesso',
                'IN_ACESSIBILIDADE_VAO_LIVRE': 'portas com vão livre',
                'IN_ACESSIBILIDADE_CORRIMAO': 'corrimãos',
                'IN_ACESSIBILIDADE_PISOS_TATEIS': 'pisos táteis',
                'IN_ACESSIBILIDADE_SINAL_TATIL': 'sinalização tátil',
                'IN_ACESSIBILIDADE_SINAL_SONORO': 'sinalização sonora'
            }

            faltas_por_item = {}
            for col, nome_amigavel in colunas_amigaveis.items():
                if col in microregion_data.columns:
                    qtd_falta = total_escolas - microregion_data[col].sum()
                    if qtd_falta > 0:
                        faltas_por_item[nome_amigavel] = qtd_falta

            itens_ordenados = sorted(faltas_por_item.items(), key=lambda x: x[1], reverse=True)
            top_3_nomes = [item[0] for item in itens_ordenados[:3]]

            if len(top_3_nomes) > 1:
                itens_formatados = ", ".join(top_3_nomes[:-1]) + f" e {top_3_nomes[-1]}"
                recomendacao = f"Fomentar a criação de consórcios intermunicipais ou buscar fomento estadual para mitigar o déficit regional de {itens_formatados}."
            elif len(top_3_nomes) == 1:
                recomendacao = f"Buscar apoio estadual para fomento estrutural focado na principal carência regional: {top_3_nomes[0]}."
            else:
                recomendacao = "Mapear assimetrias de acessibilidade entre os municípios da microrregião para ações conjuntas."
            
            req = {
                "axis": "Infrastructure",
                "level": "Microregion",
                "titulo": "Alerta Regional: Fragilidade na infraestrutura inclusiva.",
                "valor_destaque": f"{percentual:.1f}%",
                "descricao": "das escolas nesta microrregião operam abaixo da linha de base, indicando um déficit de escala territorial.",
                "recomendacao": recomendacao
            }
            prescriptions.append(req)
            
        return prescriptions
    

    def evaluate_mesoregion(self, mesoregion_data: pd.DataFrame) -> list:
        prescriptions = []
        
        avg_acessibility = mesoregion_data['acessibility_rating'].mean()
        
        if avg_acessibility < 0.5:
            total_escolas = len(mesoregion_data)
            escolas_defasadas = len(mesoregion_data[mesoregion_data['acessibility_rating'] < 0.5])
            percentual = (escolas_defasadas / total_escolas) * 100
            
            colunas_amigaveis = {
                'IN_SALA_ATENDIMENTO_ESPECIAL': 'Salas de AEE',
                'IN_BANHEIRO_PNE': 'banheiros acessíveis (PNE)',
                'IN_ACESSIBILIDADE_RAMPAS': 'rampas de acesso',
                'IN_ACESSIBILIDADE_VAO_LIVRE': 'portas com vão livre',
                'IN_ACESSIBILIDADE_CORRIMAO': 'corrimãos',
                'IN_ACESSIBILIDADE_PISOS_TATEIS': 'pisos táteis',
                'IN_ACESSIBILIDADE_SINAL_TATIL': 'sinalização tátil',
                'IN_ACESSIBILIDADE_SINAL_SONORO': 'sinalização sonora'
            }

            faltas_por_item = {}
            for col, nome_amigavel in colunas_amigaveis.items():
                if col in mesoregion_data.columns:
                    qtd_falta = total_escolas - mesoregion_data[col].sum()
                    if qtd_falta > 0:
                        faltas_por_item[nome_amigavel] = qtd_falta

            itens_ordenados = sorted(faltas_por_item.items(), key=lambda x: x[1], reverse=True)
            top_3_nomes = [item[0] for item in itens_ordenados[:3]]


            if len(top_3_nomes) > 1:
                itens_formatados = ", ".join(top_3_nomes[:-1]) + f" e {top_3_nomes[-1]}"
                recomendacao = (f"Instituir diretriz estadual de reestruturação. Recomenda-se incluir no "
                                f"orçamento da Secretaria de Estado da Educação (SEDUC) um plano de fomento focado "
                                f"na implementação de {itens_formatados} para esta macrorregião.")
            elif len(top_3_nomes) == 1:
                recomendacao = (f"Priorizar no Plano Plurianual (PPA) o repasse de verbas extraordinárias "
                                f"voltadas exclusivamente para a implementação de {top_3_nomes[0]} nas "
                                f"escolas desta macrorregião.")
            else:
                recomendacao = "Criar grupo de trabalho estadual para avaliar os indicadores críticos de acessibilidade."
            
            req = {
                "axis": "Infrastructure",
                "level": "Mesoregion",
                "titulo": "Diretriz de Macrorregião: Déficit estrutural sistêmico.",
                "valor_destaque": f"{percentual:.1f}%",
                "descricao": "das escolas desta mesorregião operam abaixo da linha de base, indicando a necessidade urgente de políticas públicas de Estado.",
                "recomendacao": recomendacao
            }
            prescriptions.append(req)
            
        return prescriptions