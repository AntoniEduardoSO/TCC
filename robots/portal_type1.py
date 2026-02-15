import time
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from .core import io

from .core.driver_setup import get_driver

diretory_project = os.getcwd()
downloads_folder = os.path.join(diretory_project, "data/raw")
final_folder = os.path.join(diretory_project, "data/Transparencia")

for f in [downloads_folder, final_folder]:
    if not os.path.exists(f): os.makedirs(f)

chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory": downloads_folder, "download.prompt_for_download": False}
chrome_options.add_experimental_option("prefs", prefs)

def search_education_value(select_element):
    for option in select_element.options:
        if "EDUCA" in option.text.upper(): 
            return option.get_attribute("value")
    return None

def download_and_read_csv(driver, wait, downloads_folder):
    """Baixa o arquivo, lê e retorna o DataFrame"""
    arquivos_antes = os.listdir(downloads_folder)
    
    # Clica no CSV
    link_csv = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, 'export/csv')]"))) # procura um elemento <a> que tem o link final export/csv
    driver.execute_script("arguments[0].click();", link_csv)
    
    # Aguarda o arquivo real
    for _ in range(30):
        novos = [f for f in os.listdir(downloads_folder) if f not in arquivos_antes and not f.startswith('.') and not f.endswith('.crdownload')]
        if novos:
            path = os.path.join(downloads_folder, novos[0])
            if os.path.getsize(path) > 0:
                try:
                    df = pd.read_csv(path, sep=None, engine='python', encoding='utf-8')
                except:
                    df = pd.read_csv(path, sep=None, engine='python', encoding='latin1')
                
                os.remove(path) # Limpa imediatamente
                return df
        time.sleep(1)
    return None



def exec1(cities_config, downloads_folder, state, progress_callback=None):
    
    df_cities_year = []
    
    for city_config in cities_config:
        driver, wait = get_driver(downloads_folder, True)
        
        try:
            
            driver.get(city_config["url"]) # Vá ate o url.
            
            element_year = wait.until(EC.presence_of_element_located((By.ID, "exercicio"))) # Procure o elemento pelo id exercicio.
            
            # Pegue as listas de anos disponiveis pelo site.
            years_list = [opt.get_attribute("value") for opt in Select(element_year).options if opt.get_attribute("value") != ""]

            for year in years_list:
                # Volta todo url do site para evitar que trave.
                driver.get(city_config["url"])
                
                select_year = Select(wait.until(EC.presence_of_element_located((By.ID, "exercicio"))))
                select_year.select_by_value(year)
                time.sleep(2) # espera o site retornar os valores.
                
                elemento_orgao = wait.until(EC.presence_of_element_located((By.ID, "orgao")))
                id_educa = search_education_value(Select(elemento_orgao))
                
                if id_educa:
                    Select(elemento_orgao).select_by_value(id_educa)
                    
                    btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-primary")))
                    driver.execute_script("arguments[0].click();", btn) # Clica o botao via JS

                    df_year = download_and_read_csv(driver, wait, downloads_folder)
                    
                    if df_year is not None:
                        df_year['ano_referencia'] = year
                        df_year['municipio_nome'] = city_config["nome"]
                        df_year['codigo_ibge'] = city_config["codigo_ibge"]
                        df_cities_year.append(df_year)
                        state.add(city_config["nome"], year, "P0", status="OK", portal_type="1", detalhe=f"{len(df_final)} regs" )
                else:
                    state.add(city_config["nome"], year, "P0", status="NO_DATA", portal_type="1", motivo="Sem dados ou erro download")


            if df_cities_year:
                df_final = pd.concat(df_cities_year, ignore_index=True)

                output_dir = os.path.join("data", "Transparencia")
                os.makedirs(output_dir, exist_ok=True)

                io.save_consolidated_df(
                    df=df_final,
                    output_folder=output_dir,
                    filename=f"{city_config['nome']}_CONSOLIDADO_1.csv"
                )
                
            io.clean_tmp_folder(downloads_folder)

            if progress_callback:
                progress_callback()
    


        except Exception as e:
            state.add(city_config["nome"], year, "P0", status="NO_DATA", portal_type="1", motivo="Sem dados ou erro download" )
    
    driver.quit()