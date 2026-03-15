import os
import shutil
import pandas as pd

from tqdm import tqdm

from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from .core.driver_setup import get_driver
from .core import io
from .core import transform


def _wait_loading(driver):
    try:
        WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.ID, "loading")))
        WebDriverWait(driver, 6000).until(EC.invisibility_of_element_located((By.ID, "loading")))
    except:
        pass

def _select_calendar(driver, element_id, text):
    field = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, element_id)))
    driver.execute_script("arguments[0].value = '';", field)
    field.send_keys(text)

def _select_dropdown_year(driver, element_id, text):
    element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, element_id)))
    Select(element).select_by_visible_text(str(text))

def download_action(driver, wait):
    btn_csv = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "#dataTables-Empenhos_wrapper a.buttons-csv"))
    )
    driver.execute_script("arguments[0].click();", btn_csv)


def exec_single_year(city, year, driver, wait, downloads_folder, state):
    df_city = []
    
    driver.get(city['url'])
    driver.delete_all_cookies()
    

    tab_empenhos = wait.until(EC.element_to_be_clickable((By.ID, "lnkEmpenhos")))
    driver.execute_script("arguments[0].click();", tab_empenhos)
    
    _wait_loading(driver)
    _select_dropdown_year(driver, "ddlAnoEmpenhos", year)

    periods = [("01/01", "30/06"), ("01/07", "31/12")]

    for idx, (start, end) in enumerate(periods, 1):
        periodo_label = f"P{idx}"

        if state.is_ok(city["nome"], year, periodo_label):
            continue

        _select_calendar(driver, "txtDtInicioEmpenhos", f"{start}/{year}")
        _select_calendar(driver, "txtDtFimEmpenhos", f"{end}/{year}")
        
        btn = driver.find_element(By.ID, "btnFiltrarEmpenhos")
        driver.execute_script("arguments[0].click();", btn)
        _wait_loading(driver)

        io.clean_tmp_folder(downloads_folder)
        
        try:
            download_action(driver, wait) # Clica no botão
            df = io.wait_and_read_csv(downloads_folder) # Espera e lê
        except Exception as e:
            df = None

        if df is None or df.empty:
            state.add(city["nome"], year, periodo_label, status="NO_DATA", portal_type="2", motivo="Sem dados ou erro download")
            continue


        df['ano_referencia'] = year
        df['municipio_nome'] = city["nome"]
        df['municipio_id'] = city["codigo_ibge"]

        anos_encontrados = df['Data'].astype(str).str.split("/").str[2].dropna().unique()
        if len(anos_encontrados) > 1 or str(year) not in anos_encontrados:
            state.add(city["nome"], year, periodo_label, status="YEAR_MISMATCH", portal_type="2", motivo=f"Anos {anos_encontrados}")
            continue

        df_processed = transform.process_portal_type2(df)

        if df_processed is not None and not df_processed.empty:
            df_city.append(df_processed)
            state.add(city["nome"], year, periodo_label, status="OK", portal_type="2", detalhe=f"{len(df_processed)} regs")
        else:
             state.add(city["nome"], year, periodo_label, status="FILTERED_EMPTY", portal_type="2", detalhe=f"Zero registros após filtro educação. len dos processados = {len(df_processed)}, len do df em si {len(df)}")

    return df_city

def process_city(city, downloads_folder, state):
    
    output_dir = os.path.join("data", "Transparencia")
    os.makedirs(output_dir, exist_ok=True)
    city_download_folder = os.path.join(downloads_folder, city['nome'])
    
    city_df_list = []

    for year in city["years_list"]:
        driver, wait = get_driver(city_download_folder, True)
        try:
            dfs = exec_single_year(city, year, driver, wait, city_download_folder, state)
            if dfs:
                city_df_list.extend(dfs)
        finally:
            driver.quit()
            
    if city_df_list:
        df_final = pd.concat(city_df_list, ignore_index=True)

        io.save_consolidated_df(
            df=df_final,
            output_folder=output_dir,
            filename=f"{city['nome']}_CONSOLIDADO_2.csv")
    
    io.clean_tmp_folder(downloads_folder)
    

    
    try:
        shutil.rmtree(city_download_folder)
    except:
        pass

def exec2(cities, downloads_folder, state, progress_callback=None):
    MAX_WORKERS = 3
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        future_to_city = {
            executor.submit(process_city, city, downloads_folder, state): city 
            for city in cities
        }

        for future in as_completed(future_to_city):
            city = future_to_city[future]
            try:
                future.result()
            except Exception as e:
                tqdm.write(f"Erro em {city['nome']}: {e}")
            
            if progress_callback:
                progress_callback()