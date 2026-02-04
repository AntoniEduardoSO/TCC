from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException

from .core import io
from .driver_setup import get_driver

import time
import os
import pandas as pd

def safe_click_csv_in_iframe(driver, wait):
    """
    Tenta clicar no botão CSV lidando com atualizações de página (StaleElement).
    Tenta até 3 vezes.
    """
    attempts = 0
    max_attempts = 3
    
    while attempts < max_attempts:
        try:
            # 1. SEMPRE volte para o contexto principal antes de buscar o iframe
            driver.switch_to.default_content()
            
            # 2. Aguarda e muda para o Iframe (Isso pega a REFERÊNCIA NOVA)
            print(f" [Tentativa {attempts+1}] Entrando no Iframe 'frmPaginaAspx'...")
            wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "frmPaginaAspx")))
            
            # 3. Busca o botão lá dentro
            btn = wait.until(EC.presence_of_element_located((By.ID, "btnExportarCSV")))
            
            # 4. Scroll e Clique (JS é mais seguro aqui)
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            time.sleep(0.5) # Estabilidade visual
            driver.execute_script("arguments[0].click();", btn)
            
            print(" [Sucesso] Clique realizado!")
            
            # Importante: Voltar para o contexto padrão imediatamente após o sucesso
            driver.switch_to.default_content()
            return True

        except StaleElementReferenceException:
            # AQUI É O PULO DO GATO
            print(" [Stale] O Iframe ou Botão atualizou na nossa cara. Tentando de novo...")
            attempts += 1
            time.sleep(1) # Dá um tempo para o DOM assentar
            
        except Exception as e:
            print(f" [Erro] Falha genérica ao clicar no iframe: {e}")
            driver.switch_to.default_content() # Garante saída em caso de erro
            return False
            
    print(" [Falha] Não foi possível clicar após 3 tentativas.")
    return False

def select_entity(driver):
    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//*[contains(@id, 'cmbEntidadeContabil_B-1')]"))
    )
    btn.click()

    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//td[normalize-space()='FUNDO MUNICIPAL DE EDUCAÇÃO DE JACUÍPE']"))
    )
    btn.click()

def wait_loading(driver):
    WebDriverWait(driver, 10).until(
        EC.invisibility_of_element_located((By.ID, "divModalLoader"))
    )

def select_year(driver, year):
    # clicar no "select" do ano
    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//*[contains(@id, 'cmbExercicio_B-1')]"))
    )
    btn.click()

    # clicar no ano
    btn = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, f"//table[@id='cmbExercicio_DDD_L_LBT']//td[normalize-space()='{str(year)}']"))
    )
    btn.click()

def exec7(cities_config, driver, wait, downloads_folder, state):
    driver.quit()

    for city in cities_config:
        df_city = []
        for year in city["years_list"]:
            driver, wait = get_driver(downloads_folder)
            df_city_per_year = []

            driver.get(city["url"])

            time.sleep(1)

            select_year(driver, year)
            wait_loading(driver)

            WebDriverWait(driver, 20).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, "#divModalLoader, #_divModalLoader"))
            )

            select_entity(driver)
            wait_loading(driver)

            time.sleep(2) # Pequena pausa de segurança

            

            success = safe_click_csv_in_iframe(driver, WebDriverWait(driver, 15))

            if not success:
                print(f"Pular {city['nome']} {year} por falha no clique.")
                state.add(city["nome"], year, "P0", status="ERROR", portal_type="7", motivo="Falha Click Iframe")
                continue 
                # Segue para o próximo ano/cidade
            
            # --- ESPERA O DOWNLOAD ---
            df_city_per_year = io.wait_and_read_csv(downloads_folder)
            

            if df_city_per_year is None or df_city_per_year.empty:
                state.add(city["nome"], year, "P0", status="NO_DATA", portal_type="7", motivo="Sem dados ou erro download")
                continue
            
            df_city_per_year['ano_referencia'] = year
            df_city_per_year['municipio_nome'] = city["nome"]
            df_city_per_year['municipio_id'] = city["codigo_ibge"]

            df_city.append(df_city_per_year)
            state.add(city["nome"], year, "P0", status="OK", portal_type="7", motivo="Sem dados ou erro download")
            driver.quit()

        output_dir = os.path.join("data", "Transparencia")
        os.makedirs(output_dir, exist_ok=True)

        if df_city and len(df_city) > 0:
            final_df = pd.concat(df_city, ignore_index=True)
            
            io.save_consolidated_df(
                df=final_df,
                output_folder=output_dir,
                filename=f"{city['nome']}_CONSOLIDADO_7.csv"
            )