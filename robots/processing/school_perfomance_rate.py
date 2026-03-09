import os
import time
import glob
import zipfile
import pandas as pd
import numpy as np

from pathlib import Path
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import  WebDriverWait

from ..core.driver_setup import get_driver
from ..core import io

current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
downloads_folder = project_root / "data" / "raw"
downloads_folder_str = str(downloads_folder)

def get_value_vars_type_2(df):
    sufixos_validos = [f"F0{i}" for i in range(1, 10)] + [f"M0{i}" for i in range(1, 4)]
    
    value_vars = []
    for col in df.columns:
        if str(col).startswith(('tap_', 'tre_', 'tab_')):
            partes = col.split('_')
            # Verifica se o sufixo (ex: 'F01') está na lista de sufixos válidos
            if len(partes) > 1 and partes[1] in sufixos_validos:
                value_vars.append(col)
    return value_vars

def get_value_vars_type_1(df):
    value_vars = []
    for col in df.columns:
        if str(col).startswith(('1_', '2_', '3_')):
            if col.endswith(('FUN', 'MED', 'AI', 'AF', 'NS')) or 'MED_04' in col: 
                continue
            value_vars.append(col)
    return value_vars

def format_school_perfomance_rate_type_2(df):

    ano_col = 'Ano' if 'Ano' in df.columns else 'NU_ANO_CENSO'
    dep_col = 'Dependad' if 'Dependad' in df.columns else 'NO_DEPENDENCIA'

    df['SG_UF'] = df['SG_UF'].astype(str).str.strip()
    df[dep_col] = df[dep_col].astype(str).str.strip()

    dependencias_desejadas = ['Municipal', 'Estadual', 'Federal']
    id_vars = ['CO_ENTIDADE', ano_col]

    df = df.query(f"SG_UF == 'AL' and {dep_col} in @dependencias_desejadas").copy()

    df = df.dropna(subset=['CO_ENTIDADE']).copy()
    df['CO_ENTIDADE'] = df['CO_ENTIDADE'].astype(int).astype(str)

    value_vars = get_value_vars_type_2(df)

    df_long = pd.melt(
        df, 
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='coluna_original',
        value_name='valor'
    )
    
    df_long['valor_str'] = df_long['valor'].astype(str).str.strip()
    df_long = df_long[df_long['valor_str'] != '--'].copy()
    df_long = df_long.dropna(subset=['valor'])
    
    partes = df_long['coluna_original'].str.split('_') 
    
    df_long['tipo'] = partes.str[0].map({'tap': 0, 'tre': 1, 'tab': 2}).fillna(-1).astype(int)
    
    sufixo = partes.str[1]
    df_long['modalidade'] = sufixo.str[0].map({'F': 0, 'M': 1}).fillna(-1).astype(int)
    
    df_long['ano'] = sufixo.str[1:].astype(int)

    df_long = df_long[(df_long['tipo'] != -1) & (df_long['modalidade'] != -1)].copy()

    df_final = df_long.rename(columns={
        'CO_ENTIDADE': 'school_id',
        ano_col: 'ano_recolhido'
    })

    df_final['valor'] = pd.to_numeric(df_final['valor_str'].str.replace(',', '.'), errors='coerce')
    df_final = df_final.dropna(subset=['valor'])

    colunas_sql = ['school_id', 'ano_recolhido', 'modalidade', 'ano', 'tipo', 'valor']
    return df_final[colunas_sql]

def format_school_perfomance_rate_type_1(df):
        
    df['SG_UF'] = df['SG_UF'].astype(str).str.strip()
    df['NO_DEPENDENCIA'] = df['NO_DEPENDENCIA'].astype(str).str.strip()

    dependencias_desejadas = ['Municipal', 'Estadual', 'Federal']
    id_vars = ['CO_ENTIDADE', 'NU_ANO_CENSO']

    df = df.query("SG_UF == 'AL' and NO_DEPENDENCIA in @dependencias_desejadas").copy()

    df = df.dropna(subset=['CO_ENTIDADE']).copy()
    df['CO_ENTIDADE'] = df['CO_ENTIDADE'].astype(int).astype(str)

    value_vars = get_value_vars_type_1(df)

    df_long = pd.melt(
        df, 
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='coluna_original',
        value_name='valor')
    
    df_long['valor_str'] = df_long['valor'].astype(str).str.strip()
    df_long = df_long[df_long['valor'].astype(str) != '--'].copy()
    df_long = df_long.dropna(subset=['valor'])
    

    partes = df_long['coluna_original'].str.split('_')

    df_long['tipo'] = partes.str[0].map({'1': 0, '2': 1, '3': 2}).fillna(-1).astype(int)

    df_long['modalidade'] = partes.str[2].map({'FUN': 0, 'MED': 1}).fillna(-1).astype(int)

    df_long['ano'] = partes.str[3].astype(int)

    df_long = df_long[(df_long['tipo'] != -1) & (df_long['modalidade'] != -1) & (df_long['ano'] != -1)].copy()

    df_final = df_long.rename(columns={
        'CO_ENTIDADE': 'school_id',
        'NU_ANO_CENSO': 'ano_recolhido'
    })

    df_final['valor'] = pd.to_numeric(df_final['valor_str'].str.replace(',', '.'), errors='coerce')
    df_final = df_final.dropna(subset=['valor'])

    colunas_sql = ['school_id', 'ano_recolhido', 'modalidade', 'ano', 'tipo', 'valor']
    df_final = df_final[colunas_sql]

    return df_final

def extract_excel_from_zip(zip_path, year):

    with zipfile.ZipFile(zip_path, 'r') as z:
        all_files = z.namelist()

        target_file = next((f for f in all_files if f"tx_rend_escolas_{year}".lower() in f.lower() and f.lower().endswith('.xlsx')), None)
        
        if target_file:
            with z.open(target_file) as f:
                df = pd.read_excel(f, engine='openpyxl', skiprows=8)

                return df

        else:
            raise FileNotFoundError(f"Arquivo Excel não encontrado dentro do ZIP. Arquivos disponíveis: {all_files[:5]}")

def wait_and_read_csv(folder_path, timeout = 300):
    start_time = time.time()

    while True:
        if (time.time() - start_time) > timeout:
            raise TimeoutError("O download demorou muito para concluir.")
        
        files = glob.glob(os.path.join(folder_path, "*"))

        if any(f.endswith(('.crdownload', '.part', '.tmp')) for f in files):
            time.sleep(1)

        zip_files = glob.glob(os.path.join(folder_path, "*.zip"))

        if zip_files:
            return max(zip_files, key=os.path.getctime)
        
        time.sleep(1)

def download_school_perfomance_rate(i, driver, wait):

    year = 2025 - i 

    driver.get("https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/taxas-de-rendimento-escolar")

    if(i > 1):
        btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, f"/html/body/div[5]/div/div/div/div/div[2]/button[2]"))
        )
        driver.execute_script("arguments[0].click();", btn)

    btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, f"//*[@id='content-core']/div[1]/div[1]/div[{i}]/a"))
    )
    driver.execute_script("arguments[0].click();", btn)

    # https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2017/TAXA_REND_2017_ESCOLAS.zip

    if(year <= 2018):
        locator = (By.CSS_SELECTOR, '#parent-fieldname-text > div > ul > li:nth-child(3) > a')    
    else:
        locator = (By.XPATH, f"//a[contains(@href, 'tx_rend_escolas_{year}.zip') or contains(@href, 'TX_REND_ESCOLAS_{year}.zip')]")

    btn = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(locator)
    )

    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)

    driver.execute_script("arguments[0].click();", btn)

def get_school_perfomance_rate_file(i):

    downloads_folder.mkdir(parents=True, exist_ok=True)

    driver, wait = get_driver(downloads_folder, True)
    
    download_school_perfomance_rate(i, driver, wait)

    zip_file = wait_and_read_csv(downloads_folder)
    
    df = extract_excel_from_zip(zip_file,  2025 - i)

    if( i <= 4):
        df = format_school_perfomance_rate_type_1(df)
    else:
        df = format_school_perfomance_rate_type_2(df)


    io.clean_tmp_folder(downloads_folder)

    driver.quit()

    return df