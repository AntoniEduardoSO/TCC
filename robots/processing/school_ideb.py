import pandas as pd
import time
import glob
import os
import zipfile

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


def format_school_ideb(df, etapa_ensino ):
    df = df.query("SG_UF == 'AL' ").copy()

    df = df.dropna(subset=['ID_ESCOLA']).copy()
    df['ID_ESCOLA'] = df['ID_ESCOLA'].astype(float).astype(int).astype(str)

    anos_validos = ['2017', '2019', '2021', '2023']
    value_vars = [col for col in df.columns if str(col).endswith(tuple(f"_{ano}" for ano in anos_validos))]

    df_long = pd.melt(
        df, 
        id_vars=['ID_ESCOLA'], 
        value_vars=value_vars, 
        var_name='coluna_original', 
        value_name='valor'
    )

    df_long['valor'] = df_long['valor'].astype(str).str.strip()
    df_long = df_long[~df_long['valor'].isin(['-', 'ND', 'nan', ''])]

    df_long['valor'] = df_long['valor'].str.replace(',', '.')
    df_long['valor'] = pd.to_numeric(df_long['valor'], errors='coerce')
    df_long = df_long.dropna(subset=['valor'])

    df_long['ano_recolhido'] = df_long['coluna_original'].str[-4:].astype(int)
    df_long['metrica'] = df_long['coluna_original'].str[:-5] 

    df_pivot = df_long.pivot_table(
        index=['ID_ESCOLA', 'ano_recolhido'], 
        columns='metrica', 
        values='valor'
    ).reset_index()

    df_pivot['etapa_ensino'] = etapa_ensino

    colunas_saeb_existentes = [col for col in ['VL_NOTA_MATEMATICA', 'VL_NOTA_PORTUGUES', 'VL_NOTA_MEDIA'] if col in df_pivot.columns]
    
    df_saeb = df_pivot[['ID_ESCOLA', 'ano_recolhido', 'etapa_ensino'] + colunas_saeb_existentes].copy()

    df_saeb = df_saeb.dropna(subset=colunas_saeb_existentes, how='all')
    df_saeb = df_saeb.rename(columns={
        'ID_ESCOLA': 'school_id',
        'VL_NOTA_MATEMATICA': 'nota_saeb_matematica',
        'VL_NOTA_PORTUGUES': 'nota_saeb_portugues',
        'VL_NOTA_MEDIA': 'nota_padronizada_media'
    })

    colunas_ideb_existentes = [col for col in ['VL_OBSERVADO', 'VL_PROJECAO'] if col in df_pivot.columns]
    
    df_ideb = df_pivot[['ID_ESCOLA', 'ano_recolhido', 'etapa_ensino'] + colunas_ideb_existentes].copy()

    df_ideb = df_ideb.dropna(subset=colunas_ideb_existentes, how='all')
    df_ideb = df_ideb.rename(columns={
        'ID_ESCOLA': 'school_id',
        'VL_OBSERVADO': 'ideb_nota',
        'VL_PROJECAO': 'ideb_meta'
    })

    return df_saeb, df_ideb

def extract_excel_from_zip(zip_path, i):

    if(i == 0):
        target_name = "anos_iniciais"
    elif (i == 1):
        target_name = "anos_finais"
    elif (i == 2):
        target_name = "ensino_medio"

    with zipfile.ZipFile(zip_path, 'r') as z:
        all_files = z.namelist()

        target_file = next((f for f in all_files if f"divulgacao_{target_name}_escolas_2023".lower() in f.lower() and f.lower().endswith('.xlsx')), None)
        
        if target_file:
            with z.open(target_file) as f:
                df = pd.read_excel(f, engine='openpyxl', skiprows=9)

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

def download_school_ideb(driver, wait, i):
    # //*[@id="parent-fieldname-text"]/ul[6]/li[2]/a

    btn = wait.until(EC.presence_of_element_located((By.XPATH, f"//*[@id='parent-fieldname-text']/ul[6]/li[{i}]/a")))
    driver.execute_script("arguments[0].click();", btn)

def get_school_ideb_file():

    downloads_folder.mkdir(parents=True, exist_ok=True)
    driver, wait = get_driver(downloads_folder, True)
    driver.get("https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados")

    lista_saeb = []
    lista_ideb = []

    i = 0

    while (i <= 2):

        download_school_ideb(driver, wait, i + 1)

        zip_file = wait_and_read_csv(downloads_folder)

        df = extract_excel_from_zip(zip_file, i)

        df_saeb_etapa, df_ideb_etapa = format_school_ideb(df, i)
        lista_saeb.append(df_saeb_etapa)
        lista_ideb.append(df_ideb_etapa)

        io.clean_tmp_folder(downloads_folder)

        i+=1

    df_saeb_final = pd.concat(lista_saeb, ignore_index=True)
    df_ideb_final = pd.concat(lista_ideb, ignore_index=True)

    output_folder = project_root / "data" / "Matricula"
    output_folder.mkdir(parents=True, exist_ok=True)

    saeb_path = output_folder / "school_saeb.csv"
    ideb_path = output_folder / "school_ideb.csv"
    
    df_saeb_final.to_csv(saeb_path, index=False, encoding='utf-8')
    df_ideb_final.to_csv(ideb_path, index=False, encoding='utf-8')
        
    driver.quit()

    return df_saeb_final, df_ideb_final