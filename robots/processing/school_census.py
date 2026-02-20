import os
import time
import glob
import zipfile
import pandas as pd

from pathlib import Path
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from ..core.driver_setup import get_driver
from ..core import io

current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
downloads_folder = project_root / "data" / "raw"
downloads_folder_str = str(downloads_folder)

def process_inep_zip(zip_path, year, type):
    dfs = []

    with zipfile.ZipFile(zip_path, 'r') as z:

        all_files = z.namelist()

        target_file = next(
            (f for f in all_files if f"microdados_ed_basica_{year}" in f and f.endswith(".csv")),
            None
        )
        
        if year == 2020:
            target_file = next(
                (f for f in all_files if f"microdados_ed_basica_{year}" in f and f.endswith(".CSV")),
                None
            )

        if target_file:
            
            with z.open(target_file) as f:

                df = pd.read_csv(
                    f,
                    sep=';',
                    encoding='latin-1',
                    low_memory=False
                )
                return df
        
        else:
            return None

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

def download_files_type_2(driver, wait, downloads_folder, i):
    year = 2021 - i

    btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, f"//*[@id='parent-fieldname-text']/div/ul[{i}]/li/a"))
    )
    driver.execute_script("arguments[0].click();", btn)

    zip_file = wait_and_read_csv(downloads_folder)

    df_ano = process_inep_zip(zip_file, year, 2)

    if df_ano is not None:

        io.clean_tmp_folder(downloads_folder)
        return df_ano

def download_files_type_1(driver, wait, downloads_folder, i):
    year = 2025 - i

    btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, f"//*[@id='parent-fieldname-text']/ul[{i}]/li/a"))
    )
    driver.execute_script("arguments[0].click();", btn)

    zip_file = wait_and_read_csv(downloads_folder)

    df_ano = process_inep_zip(zip_file, year, 1)

    if df_ano is not None:

        io.clean_tmp_folder(downloads_folder)
        return df_ano


def get_school_census_file(i):
    df = []
    
    downloads_folder.mkdir(parents=True, exist_ok=True)
    driver, wait = get_driver(downloads_folder, True)

    driver.get("https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar")

    if i <= 4:
        df = download_files_type_1(driver, wait, downloads_folder, i)
    elif i > 4:
        i -= 4
        df = download_files_type_2(driver, wait, downloads_folder, i)
    
    return df