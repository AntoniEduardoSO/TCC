import pandas as pd
import os
import glob
import hashlib
from .categories import categorize_cost

import unicodedata

def generate_centralized_id(row):

    mid = str(row['municipio_id']) if pd.notna(row['municipio_id']) else ''
    
    dt = str(row['data']) if pd.notna(row['data']) else ''
    
    val = str(row['valor']) if pd.notna(row['valor']) else '0'
    
    det = str(row['detalhe']) if pd.notna(row['detalhe']) else ''
    cred = str(row['credor']) if pd.notna(row['credor']) else ''
    
    unique_str = f"{mid}-{dt}-{val}-{cred}-{det}"
    
    return hashlib.md5(unique_str.encode('utf-8')).hexdigest()

def normalize_column(col):
    col = str(col).strip().lower()
    col = unicodedata.normalize('NFKD', col)
    col = col.encode('ascii', 'ignore').decode('ascii')
    return col

def clean_money(value):
    if value is None:
        return 0.0

    value = str(value).strip()

    value = value.replace("R$", "").strip()

    if "," in value:
        value = value.replace(".", "")
        value = value.replace(",", ".")

    try:
        return float(value)
    except:
        return 0.0

def transform_files_type_1(current_dir):
    all_dataframes = []
    path_pattern = os.path.join(current_dir, '..', '..', 'data', 'Transparencia', '*_1.csv')
    
    files = glob.glob(path_pattern)

    for file in files:
        df = pd.read_csv(file, sep=';')

        rename_map = {
            'Data': 'data',
            'Valor': 'valor',
            'Projeto Atividade': 'elemento_despesa',
            'Credor': 'credor',
            'Histórico': 'detalhe',
            'municipio_nome': 'municipio_nome',
            'codigo_ibge': 'municipio_id'
        }
        
        cols_to_keep = [col for col in rename_map.keys() if col in df.columns]
        df = df[cols_to_keep]
        df = df.rename(columns=rename_map)
        
        df['portal_origem'] = '1'
        
        if 'valor' in df.columns:
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0.0)

        all_dataframes.append(df)

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        final_df['data'] = pd.to_datetime(final_df['data'], errors='coerce')
        
        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        cols_final = [c for c in cols_order if c in final_df.columns]
        final_df = final_df[cols_final]
        
        return final_df
    return pd.DataFrame()

def transform_files_type_2(current_dir):
    all_dataframes = []
    path_pattern = os.path.join(current_dir, '..', '..', 'data', 'Transparencia', '*_2.csv')

    files = glob.glob(path_pattern)

    if not files:
        print("Nenhum arquivo [2]")

    for file in files:
        df = pd.read_csv(file, sep=';')

        rename_map = {
            'Data': 'data',
            'Credor': 'credor',
            'Valor': 'valor',
            'Elemento': 'elemento_despesa',
            'Descrição': 'detalhe',
            'Municipio_nome': 'municipio_nome',
            'municipio_id': 'municipio_id'
        }

        cols_to_keep = [col for col in rename_map.keys() if col in df.columns]
        df = df[cols_to_keep]

        df = df.rename(columns=rename_map)

        df['portal_origem'] = '2'

        all_dataframes.append(df)

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)

        final_df['data'] = pd.to_datetime(final_df['data'], dayfirst=True, errors='coerce')

        final_df['valor'] = pd.to_numeric(final_df['valor'], errors='coerce').fillna(0.0)

        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        
        cols_final = [c for c in cols_order if c in final_df.columns]

        final_df = final_df[cols_final]

    return all_dataframes

def transform_files_type_3(current_dir):
    all_dataframes = []
    path_pattern = os.path.join(current_dir, '..', '..', 'data', 'Transparencia', '*_3.csv')
    
    files = glob.glob(path_pattern)

    for file in files:
        df = pd.read_csv(file, sep=';')

        if 'Valor Pago' in df.columns:
            df['Valor Pago'] = df['Valor Pago'].apply(clean_money)
        
        rename_map = {
            'Data': 'data',
            'Valor Pago': 'valor', 
            'Ação': 'elemento_despesa',
            'Despesa': 'detalhe',
            'municipio_nome': 'municipio_nome',
            'municipio_id': 'municipio_id'
        }

        cols_to_keep = [col for col in rename_map.keys() if col in df.columns]
        df = df[cols_to_keep]
        df = df.rename(columns=rename_map)
        
        if 'credor' not in df.columns:
            df['credor'] = None

        df['portal_origem'] = '3'
        all_dataframes.append(df)
            

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        final_df['data'] = pd.to_datetime(final_df['data'], dayfirst=True, errors='coerce')

        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        cols_final = [c for c in cols_order if c in final_df.columns]
        final_df = final_df[cols_final]

        return final_df
    return pd.DataFrame()

def transform_files_type_6(current_dir):
    all_dataframes = []
    path_pattern = os.path.join(current_dir, '..', '..', 'data', 'Transparencia', '*_6.csv')
    
    files = glob.glob(path_pattern)

    for file in files:
        df = pd.read_csv(file, sep=';')

        if 'Valor Pago' in df.columns:
            df['Valor Pago'] = df['Valor Pago'].apply(clean_money)

        rename_map = {
            'Data': 'data',
            'Valor Pago': 'valor',      
            'Ação': 'elemento_despesa',  
            'Despesa': 'detalhe',     
            'municipio_nome': 'municipio_nome',
            'municipio_id': 'municipio_id'
        }
        
        cols_to_keep = [col for col in rename_map.keys() if col in df.columns]
        df = df[cols_to_keep]
        df = df.rename(columns=rename_map)
        
        df['credor'] = None 
        df['portal_origem'] = '6'
        
        all_dataframes.append(df)     

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        final_df['data'] = pd.to_datetime(final_df['data'], dayfirst=True, errors='coerce')
        
        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        cols_final = [c for c in cols_order if c in final_df.columns]
        final_df = final_df[cols_final]
        
        return final_df
    return pd.DataFrame()

def transform_files_type_7(current_dir):
    all_dataframes = []
    path_pattern = os.path.join(current_dir, '..', '..', 'data', 'Transparencia', '*_7.csv')
    
    files = glob.glob(path_pattern)

    for file in files:
        df = pd.read_csv(file, sep=';')

        if 'Valor Pago' in df.columns:
            df['Valor Pago'] = df['Valor Pago'].apply(clean_money)

        if 'ano_referencia' in df.columns:
            df['data_construida'] = df['ano_referencia'].astype(str) + '-01-01'
        else:
            df['data_construida'] = pd.NaT

        rename_map = {
            'data_construida': 'data',  
            'Valor Pago': 'valor',
            'Descrição': 'detalhe',    
            'municipio_nome': 'municipio_nome',
            'municipio_id': 'municipio_id'
        }
        
        cols_to_keep = [col for col in rename_map.keys() if col in df.columns]
        df = df[cols_to_keep]
        df = df.rename(columns=rename_map)
        
        df['credor'] = None
        
        if 'detalhe' in df.columns:
            df['elemento_despesa'] = df['detalhe']
        else:
            df['elemento_despesa'] = None

        df['portal_origem'] = '7'
        
        all_dataframes.append(df)
            

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        final_df['data'] = pd.to_datetime(final_df['data'], errors='coerce')
        
        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        cols_final = [c for c in cols_order if c in final_df.columns]
        final_df = final_df[cols_final]
        
        return final_df
    return pd.DataFrame()

def transform_files_type_8(current_dir):
    all_dataframes = []
    

    path_pattern = os.path.join(current_dir, '..', '..', 'data', 'Transparencia', '*_8_*.csv')
    
    files = glob.glob(path_pattern)

    for file in files:
        df = pd.read_csv(file, sep=';')

        rename_map = {
            'data': 'data',
            'pago': 'valor',
            'acao': 'elemento_despesa', 
            'descricao': 'detalhe',     
            'credor': 'credor',        
            'municipio_nome': 'municipio_nome',
            'municipio_id': 'municipio_id'
        }
        
        cols_to_keep = [col for col in rename_map.keys() if col in df.columns]
        df = df[cols_to_keep]
        df = df.rename(columns=rename_map)
        
        if 'valor' in df.columns:
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0.0)

        df['portal_origem'] = '8'
        
        all_dataframes.append(df)

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        final_df['data'] = pd.to_datetime(final_df['data'], dayfirst=True, errors='coerce')
        
        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        cols_final = [c for c in cols_order if c in final_df.columns]
        final_df = final_df[cols_final]
        
        return final_df
    return pd.DataFrame()

def transform_files_type_9(current_dir):
    all_dataframes = []
    path_pattern = os.path.join(current_dir, '..', '..', 'data', 'Transparencia', '*_9.csv')
    
    files = glob.glob(path_pattern)

    for file in files:
        df = pd.read_csv(file, sep=';')

        if 'Ano' in df.columns:
            df['data_construida'] = df['Ano'].astype(str) + '-01-01'
        else:
            df['data_construida'] = pd.NaT

        rename_map = {
            'data_construida': 'data',
            'Valor_Atualizado': 'valor', 
            'Acao': 'elemento_despesa', 
            'Despesa': 'detalhe',        
            'municipio_nome': 'municipio_nome',
            'municipio_id': 'municipio_id'
        }
        
        cols_to_keep = [col for col in rename_map.keys() if col in df.columns]
        df = df[cols_to_keep]
        df = df.rename(columns=rename_map)
        
        if 'valor' in df.columns:
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0.0)

        df['credor'] = None 
        df['portal_origem'] = '9'
        
        all_dataframes.append(df)

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        final_df['data'] = pd.to_datetime(final_df['data'], errors='coerce')
        
        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        cols_final = [c for c in cols_order if c in final_df.columns]
        final_df = final_df[cols_final]

        return final_df
    return pd.DataFrame()

def transform_files_type_10(current_dir):
    all_dataframes = []
    path_pattern = os.path.join(current_dir, '..', '..', 'data', 'Transparencia', '*_10.csv')
    
    files = glob.glob(path_pattern)

    for file in files:
        df = pd.read_csv(file, sep=';')

        rename_map = {
            'data': 'data',
            'valor': 'valor',
            'acao': 'elemento_despesa', 
            'detalhes': 'detalhe', 
            'municipio_nome': 'municipio_nome',
            'municipio_id': 'municipio_id'
        }
        
        cols_to_keep = [col for col in rename_map.keys() if col in df.columns]
        df = df[cols_to_keep]
        df = df.rename(columns=rename_map)
        
        if 'valor' in df.columns:
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0.0)

        df['credor'] = None 
        df['portal_origem'] = '10'
        
        all_dataframes.append(df)


    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        final_df['data'] = pd.to_datetime(final_df['data'], dayfirst=True, errors='coerce')
        
        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        cols_final = [c for c in cols_order if c in final_df.columns]
        final_df = final_df[cols_final]
        
        return final_df
    return pd.DataFrame()


def save_all_files():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    all_dataframes = []

    funcs_to_run = [
        transform_files_type_1,
        transform_files_type_2,
        transform_files_type_3,
        transform_files_type_6,
        transform_files_type_7,
        transform_files_type_8,
        transform_files_type_9,
        transform_files_type_10
    ]

    for func in funcs_to_run:
        try:
            result = func(current_dir)
            
            if isinstance(result, list):
                valid_dfs = [d for d in result if isinstance(d, pd.DataFrame) and not d.empty]
                all_dataframes.extend(valid_dfs)
                
            elif isinstance(result, pd.DataFrame):
                if not result.empty:
                    all_dataframes.append(result)
                    
        except Exception as e:
            print(f"Erro ao processar função {func.__name__}: {e}")

    if all_dataframes:
        master_df = pd.concat(all_dataframes, ignore_index=True)

        cols_para_hash = ['municipio_id', 'data', 'valor', 'credor', 'detalhe']
        for col in cols_para_hash:
            if col not in master_df.columns:
                master_df[col] = None

        master_df['id'] = master_df.apply(generate_centralized_id, axis=1)
        
        if 'data' in master_df.columns:
            master_df['data'] = pd.to_datetime(master_df['data'], errors='coerce')
            master_df = master_df.sort_values(by=['municipio_nome', 'data'])
        
        if 'data' in master_df.columns:
            master_df['data'] = master_df['data'].dt.strftime('%d/%m/%Y')

        cols_order = ['id', 'municipio_id', 'municipio_nome', 'data', 'valor', 'credor', 'elemento_despesa', 'detalhe', 'portal_origem']
        cols_final = [c for c in cols_order if c in master_df.columns]
        master_df = master_df[cols_final]

        output_path = os.path.join(current_dir, '..', '..', 'data', 'CONSOLIDADO_GERAL_FINAL.csv')
        
        master_df.to_csv(output_path, index=False, encoding='utf-8-sig')



def process_portal_type2(df):
    if df is None or df.empty:
        return None

    df.columns = [c.strip() for c in df.columns]

    required_cols = [
        "Órgão",
        "Unidade",
        "Data",
        "Empenho",
        "Elemento",
        "Credor",
        "Empenhado"
    ]

    for col in required_cols:
        if col not in df.columns:
            print(f"Coluna ausente no Portal 2: {col}")
            return None

    mask_edu = df["Órgão"].astype(str).str.contains(
        "educ", case=False, na=False
    )

    df = df[mask_edu].copy()

    if df.empty:
        return None

    df["Órgão_Final"] = (
        df["Órgão"].astype(str).str.strip() +
        " - " +
        df["Unidade"].astype(str).str.strip()
    )

    df["Valor"] = (
        df["Empenhado"]
        .astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip()
    )

    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0)

    if "DsEmpenho" in df.columns:
        descricao = df["DsEmpenho"].astype(str)
    else:
        descricao = ""

    if "DsItemDespesa" in df.columns:
        descricao = descricao + " " + df["DsItemDespesa"].astype(str)

    df_final = pd.DataFrame({
        "Data": df["Data"],
        "Empenho": df["Empenho"],
        "Órgão": df["Órgão_Final"],
        "Credor": df["Credor"],
        "Valor": df["Valor"],
        "Elemento": df["Elemento"],
        "Descrição": descricao,
        "Ano": df.get("ano_referencia"),
        "Municipio_nome": df.get("municipio_nome"),
        "municipio_id": df.get("municipio_id")
    })

    df_final = df_final[
        [
            "Data",
            "Empenho",
            "Órgão",
            "Credor",
            "Valor",
            "Elemento",
            "Descrição",
            "Ano",
            "Municipio_nome",
            "municipio_id"
        ]
    ]

    return df_final
