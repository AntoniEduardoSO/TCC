from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from .core.driver_setup import get_driver

from .core import io

import time
import os

def wait_loading(driver):
    WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.XPATH, "/html/body/div[190]/div")))

def click_export(driver):
    time.sleep(5)
    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//*[@id='dropdownDownload']"))
    )
    driver.execute_script("arguments[0].click();", btn)

    time.sleep(2)

    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//*[@id='card-consulta']/div/div/div/div/div[1]/span/div/div/div/div/div/div[3]/div/ul/li[5]/a"))
    )
    driver.execute_script("arguments[0].click();", btn)
    
def click_filter(driver):
    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[190]/div/div[3]/button[1]"))
    )
    btn.click()

def click_entity(driver):
    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[190]/div/div[2]/div/div/div/div/div[1]/div[2]/ul/li[3]/div/label"))
    )
    btn.click()

def click_years(driver, i):
    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, f"//label[@for='anoExercicio-term-{i}']"))
    )
    btn.click()

def exec6(cities_config, downloads_folder, state, progress_callback=None):

    for city in cities_config:
        try:
            driver, wait = get_driver(downloads_folder, True)

            if state.is_ok(city["nome"], 0000, "P0"):
                continue

            driver.get(city["url"])

            wait_loading(driver)

            for i in range(5):
                click_years(driver, i)
            
            click_entity(driver)

            click_filter(driver)

            click_export(driver)

            df_city = io.wait_and_read_csv(downloads_folder)

            df_city['municipio_nome'] = city["nome"]
            df_city['municipio_id'] = city["codigo_ibge"]

            output_dir = os.path.join("data", "Transparencia")
            os.makedirs(output_dir, exist_ok=True)

            io.save_consolidated_df(
                df=df_city,
                output_folder=output_dir,
                filename=f"{city['nome']}_CONSOLIDADO_6.csv"
            )

            if progress_callback:
                progress_callback()

            io.clean_tmp_folder(downloads_folder)

            state.add(city["nome"], 0000, "P0", status="OK", portal_type="6", detalhe=f"{len(df_city)} regs")
        
        except:
            state.add(city["nome"], 0000, "P0", status="NO_DATA", portal_type="6", motivo="Sem dados ou erro download")