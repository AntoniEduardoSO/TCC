import pandas as pd
from .categories import categorize_cost

def process_education_data(df):
    if df is None or df.empty:
        return None

    # Normalização de nomes de colunas para facilitar a busca
    df.columns = [c.strip() for c in df.columns]

    word_filter = 'educa|ensino|fundeb|merenda|semed|ensino|fundo de educa'
    keyword_col = ['org', 'unid', 'secr', 'centr', 'dep', 'setor']

    # Identifica colunas relevantes
    col_to_verify = [col for col in df.columns if any(key in col.lower() for key in keyword_col)]

    if not col_to_verify:
        return None

    # Cria máscara booleana para filtrar linhas
    final_mask = pd.Series(False, index=df.index)
    for col in col_to_verify:
        col_mask = df[col].astype(str).str.contains(
            word_filter, case=False, na=False, regex=True
        )
        final_mask |= col_mask

    df_edu = df[final_mask].copy()
    if df_edu.empty:
        return None

    # Lógica de consolidação de Órgão
    col_orgao_orig = next((c for c in col_to_verify if 'org' in c.lower()), None)
    col_unidade_orig = next((c for c in col_to_verify if 'unid' in c.lower()), None)

    if col_orgao_orig and col_unidade_orig:
        df_edu['Orgao_Consolidado'] = df_edu[col_orgao_orig].astype(str) + " - " + df_edu[col_unidade_orig].astype(str)
    elif col_unidade_orig:
        df_edu['Orgao_Consolidado'] = df_edu[col_unidade_orig]
    elif col_orgao_orig:
        df_edu['Orgao_Consolidado'] = df_edu[col_orgao_orig]
    else:
        df_edu['Orgao_Consolidado'] = df_edu[col_to_verify[0]]

    # Lógica de Descrição
    col_empenho = 'DsEmpenho' if 'DsEmpenho' in df_edu.columns else None
    col_item = 'DsItemDespesa' if 'DsItemDespesa' in df_edu.columns else None

    if col_empenho and col_item:
        df_edu['Descricao_Final'] = df_edu[col_empenho].fillna(df_edu[col_item]).fillna('')
    elif col_empenho:
        df_edu['Descricao_Final'] = df_edu[col_empenho].fillna('')
    elif col_item:
        df_edu['Descricao_Final'] = df_edu[col_item].fillna('')
    else:
        df_edu['Descricao_Final'] = 'Sem Descrição'

    # Categorização (usa seu módulo existente)
    df_edu['Categoria'] = df_edu['Descricao_Final'].apply(categorize_cost)

    # Mapeamento final
    cols_map = {
        'Data': 'Data',
        'Empenho': 'Empenho',
        'Orgao_Consolidado': 'Órgão',
        'Credor': 'Credor',
        'Empenhado': 'Valor',
        'Descricao_Final': 'Descrição',
        'Categoria': 'Categoria',
        'ano_referencia': 'Ano',
        'municipio_nome': 'Municipio_nome',
        'municipio_id': 'municipio_id'
    }

    cols_finais = [c for c in cols_map.keys() if c in df_edu.columns]
    return df_edu[cols_finais].rename(columns=cols_map)