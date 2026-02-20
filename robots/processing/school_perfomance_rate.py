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

def get_value_vars(df):
    value_vars = []
    for col in df.columns:
        if str(col).startswith(('1_', '2_', '3_')):

            if col.endswith(('FUN', 'MED')): 
                continue

            if col.endswith(('AI', 'AF')): 
                continue

            if col.endswith('NS') or 'MED_04' in col: 
                continue

            value_vars.append(col)

    return value_vars


def format_school_perfomance_rate(df, year):

    df['SG_UF'] = df['SG_UF'].astype(str).str.strip()
    df['NO_DEPENDENCIA'] = df['NO_DEPENDENCIA'].astype(str).str.strip()

    dependencias_desejadas = ['Municipal', 'Estadual', 'Federal']
    id_vars = ['CO_ENTIDADE', 'NU_ANO_CENSO']

    df = df.query("SG_UF == 'AL' and NO_DEPENDENCIA in @dependencias_desejadas").copy()

    df = df.dropna(subset=['CO_ENTIDADE']).copy()
    df['CO_ENTIDADE'] = df['CO_ENTIDADE'].astype(int).astype(str)

    value_vars = get_value_vars(df)

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

    df_long['NU_ANO_CENSO'] = year

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
    internal_path = f"tx_rend_escolas_{year}/tx_rend_escolas_{year}.xlsx"

    with zipfile.ZipFile(zip_path, 'r') as z:
        all_files = z.namelist()

        target_file = next((f for f in all_files if "tx_rend_escolas" in f and f.endswith('.xlsx')), None)

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

def download_school_perfomance_rate(i):

    driver, wait = get_driver(downloads_folder, False)

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

    btn = wait.until(
        EC.presence_of_element_located((By.XPATH, "//*[@id='parent-fieldname-text']//a[contains(translate(text(), 'ESCOLAS', 'escolas'), 'escolas')]"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
    driver.execute_script("arguments[0].click();", btn)

def get_school_perfomance_rate_file(i):
    
    downloads_folder.mkdir(parents=True, exist_ok=True)

    download_school_perfomance_rate(i)

    zip_file = wait_and_read_csv(downloads_folder)
    
    df = extract_excel_from_zip(zip_file,  2025 - i)

    df = format_school_perfomance_rate(df, 2025 - i)

    io.clean_tmp_folder(downloads_folder)

    return df