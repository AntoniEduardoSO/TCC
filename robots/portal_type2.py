import time
import os
import glob
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

def limpar_pasta_temp(folder):
    files = glob.glob(os.path.join(folder, "*"))
    for f in files:
        try: os.remove(f)
        except: pass

def download_and_read_csv(driver, wait, downloads_folder):
    limpar_pasta_temp(downloads_folder)

    try:
        btn_csv = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.buttons-csv")))
        
        driver.execute_script("arguments[0].scrollIntoView(true);", btn_csv)
        time.sleep(2) 
        
        driver.execute_script("arguments[0].click();", btn_csv)
        
    except Exception as e:
        return None

    max_tempo_espera = 60
    inicio = time.time()
    arquivo_final = None

    while (time.time() - inicio) < max_tempo_espera:
        arquivos = os.listdir(downloads_folder)
        
        arquivos_reais = [f for f in arquivos if not f.startswith('.') and f != '']
        
        if not arquivos_reais:
            time.sleep(1)
            continue
        
        nome_arquivo = arquivos_reais[0]
        path_completo = os.path.join(downloads_folder, nome_arquivo)
        
        if nome_arquivo.endswith('.crdownload') or nome_arquivo.endswith('.tmp'):
            print(f"      [⏳] Baixando... ({nome_arquivo})")
            time.sleep(2)
            continue
            
        if os.path.getsize(path_completo) > 0:
            tamanho_t1 = os.path.getsize(path_completo)
            time.sleep(3)
            tamanho_t2 = os.path.getsize(path_completo)
            
            if tamanho_t1 == tamanho_t2:
                arquivo_final = path_completo
                break
            else:
                print("Arquivo ainda crescendo.")
        else:
            time.sleep(1)

    if arquivo_final:
        try:
            try:
                df = pd.read_csv(arquivo_final, sep=None, engine='python', encoding='utf-8')
            except:
                df = pd.read_csv(arquivo_final, sep=None, engine='python', encoding='latin1')
            
            os.remove(arquivo_final)
            return df
        except Exception as e:
            print(f"Arquivo corrompido ou erro de leitura: {e}")
            return None
    else:
        return None

def exec2(cities_config, driver, wait, downloads_folder):
    for city in cities_config:
        df_city_years = []
        years_to_process = city.get("years_list", [])

        
        driver.get(city['url'])

            
        tab = wait.until(EC.element_to_be_clickable((By.ID, "lnkEmpenhos")))
        driver.execute_script("arguments[0].click();", tab)
        time.sleep(2)

        for year in years_to_process:
                
            sel = Select(wait.until(EC.presence_of_element_located((By.ID, "ddlAnoEmpenhos"))))
            if sel.first_selected_option.get_attribute("value") != str(year):
                sel.select_by_value(str(year))
                time.sleep(3)

            dt_ini = wait.until(EC.element_to_be_clickable((By.ID, "txtDtInicioEmpenhos")))
            driver.execute_script("arguments[0].value = '';", dt_ini)
            dt_ini.send_keys(f"01/01/{year}")
            
            dt_fim = wait.until(EC.element_to_be_clickable((By.ID, "txtDtFimEmpenhos")))
            driver.execute_script("arguments[0].value = '';", dt_fim)
            dt_fim.send_keys(f"31/12/{year}")

            btn = driver.find_element(By.ID, "btnFiltrarEmpenhos")
            
            try:
                wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "loading-mask")))
            except: pass
            
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1)

            df_year = download_and_read_csv(driver, wait, downloads_folder)

            if df_year is not None:
                df_year['ano_referencia'] = year
                df_year['municipio_nome'] = city["nome"]
                df_year['codigo_ibge'] = city["codigo_ibge"]
                df_city_years.append(df_year)
            else:
                driver.get(city['url'])
                try:
                    t = wait.until(EC.element_to_be_clickable((By.ID, "lnkEmpenhos")))
                    driver.execute_script("arguments[0].click();", t)
                except: pass

        if df_city_years:
            final_folder = os.path.join(os.getcwd(), "data", "Transparencia")
            if not os.path.exists(final_folder): os.makedirs(final_folder)
            
            df_final = pd.concat(df_city_years, ignore_index=True)
            path_final = os.path.join(final_folder, f"{city['nome']}_CONSOLIDADO.csv.gz")
            df_final.to_csv(path_final, index=False, sep=';', compression='gzip', encoding='utf-8-sig')