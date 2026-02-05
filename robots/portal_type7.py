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

def click_export_csv(driver):
    WebDriverWait(driver, 20).until(
        EC.frame_to_be_available_and_switch_to_it((By.ID, "frmPaginaAspx"))
    )

    try:
        btn_csv = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "btnExportarCSV"))
        )
        btn_csv.click()
        print(f"Clique no CSV realizado para {city['nome']} - {year}")
    except Exception as e:
        print(f"Erro ao clicar no CSV: {e}")
        try:
            csv_element = driver.find_element(By.ID, "btnExportarCSV")
            driver.execute_script("arguments[0].click();", csv_element)
        except:
            pass

    driver.switch_to.default_content()


def select_entity(driver):
    WebDriverWait(driver, 20).until(
        EC.invisibility_of_element_located((By.CSS_SELECTOR, "#divModalLoader, #_divModalLoader"))
    )

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

    time.sleep(1)

    btn = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, f"//table[@id='cmbExercicio_DDD_L_LBT']//td[normalize-space()='{str(year)}']"))
    )
    btn.click()
    driver.execute_script("arguments[0].click();", btn)
    
    

def exec7(cities_config, driver, wait, downloads_folder, state):
    driver.quit()

    for city in cities_config:
        df_city = []
        for year in city["years_list"]:
            if state.is_ok(city["nome"], year, "P0"):
                continue


            driver, wait = get_driver(downloads_folder)
            df_city_per_year = []


            driver.get(city["url"])

            time.sleep(2)

            select_year(driver, year)
            wait_loading(driver)

            select_entity(driver)
            wait_loading(driver)

            time.sleep(2)

            click_export_csv(driver)

            
            df_city_per_year = io.wait_and_read_csv(downloads_folder)
            

            if df_city_per_year is None or df_city_per_year.empty:
                state.add(city["nome"], year, "P0", status="NO_DATA", portal_type="7", motivo="Sem dados ou erro download")
                continue
            
            df_city_per_year['ano_referencia'] = year
            df_city_per_year['municipio_nome'] = city["nome"]
            df_city_per_year['municipio_id'] = city["codigo_ibge"]

            df_city.append(df_city_per_year)
            state.add(city["nome"], year, "P0", status="OK", portal_type="7", detalhe = f"{len(df_city_per_year)} regs")
            driver.delete_all_cookies()
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