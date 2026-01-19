from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import glob
import pandas as pd

def limpar_pasta_temp(folder):
    files = glob.glob(os.path.join(folder, "*"))
    for f in files:
        try: os.remove(f)
        except: pass

def download_and_read_csv(driver, wait, downloads_folder):
    limpar_pasta_temp(downloads_folder)

    try:
        
        wait.until(EC.presence_of_element_located((By.ID, "btnExportToCSV_CD")))
            
        btn_csv = driver.find_element(By.ID, "btnExportToCSV_I")

        driver.execute_script("arguments[0].scrollIntoView(true);", btn_csv)
        time.sleep(1)
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


def exec3(cities_config, driver, wait, downloads_folder):
    for city in cities_config:
        df_city_years = []

        url = city['url']
        termos_busca = ["Educa", "Fundeb", "FUNDEB"]
        driver.get(url)

        try:
            # O ID do botão é geralmente o ID do input + "_B-1"
            dropdown_button_id = "EntidadeId_B-1"
            
            btn_expandir = wait.until(EC.element_to_be_clickable((By.ID, dropdown_button_id)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_expandir)
            time.sleep(0.5)
            btn_expandir.click()
            
            # O ID da tabela que contém a lista é "EntidadeId_DDD_L_LBT" baseado no seu HTML
            lista_container_id = "EntidadeId_DDD_L_LBT"
            wait.until(EC.visibility_of_element_located((By.ID, lista_container_id)))
            time.sleep(0.5) 

            opcoes = driver.find_elements(By.CSS_SELECTOR, f"#{lista_container_id} td.dxeListBoxItem_Mulberry")
            entidade_encontrada = False

            for opcao in opcoes:
                texto_opcao = opcao.text
                
                if any(termo in texto_opcao for termo in termos_busca):
                    opcao.click()
                    entidade_encontrada = True
                    break 
            
            if not entidade_encontrada:
                print(f"Nenhuma entidade com os termos {termos_busca} foi encontrada na lista.")

            years = city.get("years_list", [])
            for year in years:
                
                btn_ano = wait.until(EC.element_to_be_clickable((By.ID, "AnoId_B-1")))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_ano)
                time.sleep(0.5)
                btn_ano.click()

                wait.until(EC.visibility_of_element_located((By.ID, "AnoId_DDD_L_LBT")))
                time.sleep(0.5)

                xpath_ano = f"//*[@id='AnoId_DDD_L_LBT']//td[contains(@class, 'dxeListBoxItem_Mulberry') and contains(text(), '{year}')]"
                ano_opcao = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_ano)))
                ano_opcao.click()

                time.sleep(0.5)

                btn_pesquisar = driver.find_element(By.CSS_SELECTOR, "button[type='submit'].btn-success")
                driver.execute_script("arguments[0].click();", btn_pesquisar)

                time.sleep(2)

                df_year = download_and_read_csv(driver, wait, downloads_folder)


                if df_year is not None:
                    df_year['ano_referencia'] = year
                    df_year['municipio_nome'] = city["nome"]
                    df_year['codigo_ibge'] = city["codigo_ibge"]
                    df_city_years.append(df_year)

                
            
            if df_city_years:
                final_folder = os.path.join(os.getcwd(), "data", "Transparencia")
                if not os.path.exists(final_folder): os.makedirs(final_folder)
                
                df_final = pd.concat(df_city_years, ignore_index=True)
                path_final = os.path.join(final_folder, f"{city['nome']}_CONSOLIDADO.csv.gz")
                df_final.to_csv(path_final, index=False, sep=';', compression='gzip', encoding='utf-8-sig')

        except Exception as e:
            print(f"Erro ao selecionar a entidade: {e}")
            raise e

