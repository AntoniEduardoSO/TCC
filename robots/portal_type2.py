import time
import os
import glob
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

def limpar_pasta_temp(folder):
    files = glob.glob(os.path.join(folder, "*"))
    for f in files:
        try: os.remove(f)
        except: pass

def download_and_read_csv(driver, wait, downloads_folder):
    limpar_pasta_temp(downloads_folder)
    try:
        btn_csv = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#dataTables-Empenhos_wrapper a.buttons-csv")))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_csv)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", btn_csv)
    except Exception:
        return None

    max_tempo_espera = 60
    inicio = time.time()
    while (time.time() - inicio) < max_tempo_espera:
        arquivos = [f for f in os.listdir(downloads_folder) if not f.startswith('.') and f != '']
        if not arquivos:
            time.sleep(1)
            continue
        
        path_completo = os.path.join(downloads_folder, arquivos[0])
        if path_completo.endswith('.crdownload') or path_completo.endswith('.tmp'):
            time.sleep(2)
            continue
            
        if os.path.getsize(path_completo) > 0:
            time.sleep(3)
            try:
                df = pd.read_csv(path_completo, sep=None, engine='python', encoding='utf-8')
            except:
                df = pd.read_csv(path_completo, sep=None, engine='python', encoding='latin1')
            os.remove(path_completo)
            return df
        time.sleep(1)
    return None

def exec2(cities_config, driver, wait, downloads_folder):
    for city in cities_config:
        df_city_years = []
        years_to_process = city.get("years_list", [])

        for year in years_to_process:
            driver.get(city['url'])
            # time.sleep(2)
        
            # Ativa aba Empenhos
            tab = wait.until(EC.element_to_be_clickable((By.ID, "lnkEmpenhos")))
            tab.click()
            driver.execute_script("arguments[0].click();", tab)
            time.sleep(1)
            
            sel = Select(wait.until(EC.presence_of_element_located((By.ID, "ddlAnoEmpenhos"))))
            sel.select_by_value(str(year))
            time.sleep(1)

            for campo_id in ["txtDtInicioEmpenhos", "txtDtFimEmpenhos"]:
                    campo = wait.until(EC.element_to_be_clickable((By.ID, campo_id)))
                    driver.execute_script("arguments[0].value = '';", campo)
                    data_str = f"01/01/{year}" if "Inicio" in campo_id else f"31/12/{year}"
                    campo.send_keys(data_str)
            

            btn_pesquisar = driver.find_element(By.ID, "btnFiltrarEmpenhos")
            driver.execute_script("arguments[0].click();", btn_pesquisar)

            try:
                wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "loading-mask")))
            except:
                pass

            df_year = download_and_read_csv(driver, wait, downloads_folder)

            if df_year is not None:
                df_year['ano_referencia'] = year
                df_year['municipio_nome'] = city["nome"]
                df_year['codigo_ibge'] = city["codigo_ibge"]
                df_city_years.append(df_year)
                print(f" {len(df_year)} registros capturados do {city["nome"]}, do ano {year}")
            else:
                print(f"      [!] Aviso: Nenhum dado retornado para {year}.")

        # Consolidação Final
        if df_city_years:
            final_folder = os.path.join(os.getcwd(), "data", "Transparencia")
            os.makedirs(final_folder, exist_ok=True)
            df_final = pd.concat(df_city_years, ignore_index=True)
            path_final = os.path.join(final_folder, f"{city['nome']}_CONSOLIDADO.csv.gz")
            df_final.to_csv(path_final, index=False, sep=';', compression='gzip', encoding='utf-8-sig')