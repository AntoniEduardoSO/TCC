import pandas as pd
from handlers.base_handler import BaseHandler

class InfrastructureHandler(BaseHandler):

    COLUNAS_AMIGAVEIS = {
        'IN_SALA_ATENDIMENTO_ESPECIAL': 'Salas de AEE',
        'IN_BANHEIRO_PNE': 'banheiros acessíveis (PNE)',
        'IN_ACESSIBILIDADE_RAMPAS': 'rampas de acesso',
        'IN_ACESSIBILIDADE_VAO_LIVRE': 'portas com vão livre',
        'IN_ACESSIBILIDADE_CORRIMAO': 'corrimãos',
        'IN_ACESSIBILIDADE_PISOS_TATEIS': 'pisos táteis',
        'IN_ACESSIBILIDADE_SINAL_TATIL': 'sinalização tátil',
        'IN_ACESSIBILIDADE_SINAL_SONORO': 'sinalização sonora'
    }

    def __init__(self, baselines: dict):
        """
        :param baseline_rating: A mediana ou média de acessibilidade do estado no ano base.
        Deve ser calculada no orquestrador (ex: df['acessibility_rating'].median()) e repassada aqui.
        """
        self.baseline_rating = baselines.get('acessibilidade', 0.5)

    def _format_lista(self, itens: list) -> str:
        """Centraliza a lógica de humanização de listas (", " e " e ")"""
        if not itens: return ""
        if len(itens) == 1: return itens[0]
        return ", ".join(itens[:-1]) + f" e {itens[-1]}"

    def _get_regional_metrics(self, df: pd.DataFrame):
        avg_acessibility = df['acessibility_rating'].mean()
        
        # Só dispara alerta se a unidade de agregação estiver ABAIXO da mediana estadual
        if avg_acessibility >= self.baseline_rating:
            return None
            
        total_escolas = len(df)
        
        percentual = (df['acessibility_rating'] < self.baseline_rating).mean() * 100
        
        cols_presentes = [c for c in self.COLUNAS_AMIGAVEIS.keys() if c in df.columns]
        somas = df[cols_presentes].sum()
        
        # Encontra a quantidade de "ausências" por item (total de escolas - soma do item)
        faltas = total_escolas - somas
        
        # Pega as top 3 colunas com mais problemas (ignora se não houver falta = > 0)
        top_3_cols = faltas[faltas > 0].nlargest(3).index.tolist()
        
        # Traduz as colunas para o nome amigável
        top_3_nomes = [self.COLUNAS_AMIGAVEIS[col] for col in top_3_cols]
        
        return percentual, top_3_nomes
    
    def evaluate_school(self, school_data: pd.Series) -> list:
        prescriptions = []
        rating = school_data.get('acessibility_rating', 1.0)
        
        # Compara dinamicamente com a baseline do Estado
        if rating < self.baseline_rating:
            missing_items = [
                nome for col, nome in self.COLUNAS_AMIGAVEIS.items()
                if not school_data.get(col, 1)
            ]
            
            if missing_items:
                if len(missing_items) > 3:
                    itens_principais = self._format_lista(missing_items[:3])
                    outros_count = len(missing_items) - 3
                    recomendacao = (f"Priorizar a implementação de adequações críticas, como {itens_principais}, "
                                    f"além de realizar outras {outros_count} intervenções complementares na unidade escolar.")
                else:
                    itens_formatados = self._format_lista(missing_items)
                    verbo = "Concentrar esforços na" if len(missing_items) == 1 else "Priorizar a"
                    recomendacao = f"{verbo} implementação de {itens_formatados} na unidade escolar."

                req = {
                    "axis": "Infrastructure",
                    "level": "School",
                    "titulo": "Defasagem na acessibilidade estrutural.",
                    "valor_destaque": f"{rating * 100:.1f}%",
                    "descricao": f"é o índice desta unidade, operando severamente abaixo da mediana inclusiva do Estado ({self.baseline_rating*100:.1f}%).",
                    "recomendacao": recomendacao,
                    "valor_baseline": self.baseline_rating
                }
                prescriptions.append(req)
                
        return prescriptions

    def evaluate_municipality(self, municipality_data: pd.DataFrame) -> list:
        metrics = self._get_regional_metrics(municipality_data)
        if not metrics: return []
        
        percentual, top_3_nomes = metrics
        
        if len(top_3_nomes) > 1:
            itens_formatados = self._format_lista(top_3_nomes)
            recomendacao = f"Articular edital unificado focado nas carências sistêmicas da rede, como a implementação de {itens_formatados}."
        elif len(top_3_nomes) == 1:
            recomendacao = f"Articular edital de adequação focado na principal carência da rede: implementação de {top_3_nomes[0]}."
        else:
            recomendacao = "Realizar auditoria estrutural para mapear necessidades de acessibilidade nas unidades escolares."
        
        return [{
            "axis": "Infrastructure",
            "level": "Municipality",
            "titulo": "Defasagem sistêmica na acessibilidade estrutural.",
            "valor_destaque": f"{percentual:.1f}%",
            "descricao": f"das escolas da rede municipal operam abaixo da linha de base do Estado ({self.baseline_rating*100:.1f}%).",
            "recomendacao": recomendacao,
            "valor_baseline": self.baseline_rating
        }]
    

    def evaluate_microregion(self, microregion_data: pd.DataFrame) -> list:
        metrics = self._get_regional_metrics(microregion_data)
        if not metrics: return []
        
        percentual, top_3_nomes = metrics
        
        if len(top_3_nomes) > 1:
            itens_formatados = self._format_lista(top_3_nomes)
            recomendacao = f"Fomentar a criação de consórcios intermunicipais ou buscar fomento estadual para mitigar o déficit regional de {itens_formatados}."
        elif len(top_3_nomes) == 1:
            recomendacao = f"Buscar apoio estadual para fomento estrutural focado na principal carência regional: {top_3_nomes[0]}."
        else:
            recomendacao = "Mapear assimetrias de acessibilidade entre os municípios da microrregião para ações conjuntas."
        
        return [{
            "axis": "Infrastructure",
            "level": "Microregion",
            "titulo": "Alerta Regional: Fragilidade na infraestrutura inclusiva.",
            "valor_destaque": f"{percentual:.1f}%",
            "descricao": f"das escolas operam abaixo da mediana do Estado, indicando um déficit de escala territorial.",
            "recomendacao": recomendacao,
            "valor_baseline": self.baseline_rating
        }]
    

    def evaluate_mesoregion(self, mesoregion_data: pd.DataFrame) -> list:
        metrics = self._get_regional_metrics(mesoregion_data)
        if not metrics: return []
        
        percentual, top_3_nomes = metrics
        
        if len(top_3_nomes) > 1:
            itens_formatados = self._format_lista(top_3_nomes)
            recomendacao = (f"Instituir diretriz estadual de reestruturação. Recomenda-se incluir no "
                            f"orçamento da SEDUC um plano de fomento focado "
                            f"na implementação de {itens_formatados} para esta macrorregião.")
        elif len(top_3_nomes) == 1:
            recomendacao = (f"Priorizar no Plano Plurianual (PPA) o repasse de verbas extraordinárias "
                            f"voltadas exclusivamente para a implementação de {top_3_nomes[0]} nas "
                            f"escolas desta macrorregião.")
        else:
            recomendacao = "Criar grupo de trabalho estadual para avaliar os indicadores críticos de acessibilidade."
        
        return [{
            "axis": "Infrastructure",
            "level": "Mesoregion",
            "titulo": "Diretriz de Macrorregião: Déficit estrutural sistêmico.",
            "valor_destaque": f"{percentual:.1f}%",
            "descricao": f"das escolas operam abaixo da mediana estadual, indicando a necessidade urgente de políticas públicas de Estado.",
            "recomendacao": recomendacao,
            "valor_baseline": self.baseline_rating
        }]