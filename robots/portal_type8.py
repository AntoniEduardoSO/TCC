from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor

import time
import re
import os
import pandas as pd

from robots.core import io
from robots.core.driver_setup import get_driver
from .core import transform

campos_relativos = {
    "descricao": ".//label[text()='Descrição:']/parent::div/following-sibling::div[1]/label",
    "credor": ".//label[text()='Credor:']/parent::div/following-sibling::div[1]/label",
    "acao": ".//label[text()='Ação:']/parent::div/following-sibling::div[1]/label",
    "pago": ".//label[text()='Pago(R$):']/parent::div/following-sibling::div[1]/label",
    "data": ".//label[text()='Data:']/parent::div/following-sibling::div[1]/label",
}

def has_next_page(driver, wait):
    xpath = "//*[@id='form-pago:tabela-empenho-pago_paginator_bottom']//a[contains(@class,'ui-paginator-next')]"

    for tentativa in range(3):
        try:
            btn = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            classes = btn.get_attribute("class")
            return "ui-state-disabled" not in classes
        except Exception as e:
            time.sleep(0.3)

    return False

def go_next_page(driver, wait):
    xpath = "//*[@id='form-pago:tabela-empenho-pago_paginator_bottom']//a[contains(@class,'ui-paginator-next')]"

    for tentativa in range(3):
        try:
            btn = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", btn)
            wait_loading(driver)
            return
        except Exception as e:
            time.sleep(0.3)

    raise RuntimeError("Não conseguiu avançar página")

def obter_total_registros(driver):
    texto = driver.find_element(By.XPATH, "//*[@id='form-pago:tabela-empenho-pago_paginator_bottom']/span[1]").text
    total = re.search(r'de\s+(\d+)', texto).group(1)
    return int(total)

def get_label_ids(driver):
    return [
        e.text for e in driver.find_elements(
            By.XPATH,
            "//*[@id='form-pago:tabela-empenho-pago_data']/tr/td[5]//label"
        )
    ]

def click_close_button(driver, wait):
    xpath_btn = (
        "//span[normalize-space()='Detalhe do Empenho']"
        "/ancestor::div[contains(@class,'ui-dialog')]"
        "//a[contains(@class,'ui-dialog-titlebar-close')]"
    )

    btn = wait.until(EC.presence_of_element_located((By.XPATH, xpath_btn)))

    driver.execute_script("""
    document.querySelectorAll('.ui-dialog-mask,.ui-widget-overlay')
    .forEach(e => e.remove());
    """)

    driver.execute_script("arguments[0].click();", btn)

    wait.until(EC.invisibility_of_element_located((
        By.XPATH,
        "//span[normalize-space()='Detalhe do Empenho']"
        "/ancestor::div[contains(@class,'ui-dialog')]"
    )))

def click_detail_empenho(driver, wait, label_id, tentativas=3):
    for _ in range(tentativas):
        try:
            dados = {"empenho": label_id}

            xpath_btn = (
                f"//label[normalize-space()='{label_id}']"
                "/ancestor::tr//a"
            )

            btn = wait.until(EC.presence_of_element_located((By.XPATH, xpath_btn)))
            driver.execute_script("arguments[0].click();", btn)

            modal = wait.until(EC.visibility_of_element_located((By.ID, "form-detalhes-empenho")))

            for chave, xpath in campos_relativos.items():
                try:
                    dados[chave] = modal.find_element(By.XPATH, xpath).text
                except:
                    dados[chave] = None

            click_close_button(driver, wait)
            return dados

        except Exception as e:
            time.sleep(0.5)

    return {"empenho": label_id}

def extract_data(driver, wait):
    data_buffer = []
    id_process = set()

    while True:

        label_ids = get_label_ids(driver)

        for label_id in label_ids:
            if label_id in id_process:
                continue

            registro = click_detail_empenho(driver, wait, label_id)
            data_buffer.append(registro)
            id_process.add(label_id)

        if not has_next_page(driver, wait):
            break
            
        go_next_page(driver, wait)    

    return data_buffer

def click_empenho_paid(driver, wait):
    btn = wait.until(
        EC.element_to_be_clickable((By.ID, "tabs:form-despesas:tabela-despesas:0:detalhesPago"))
    )
    driver.execute_script("arguments[0].click();", btn)

def wait_loading(driver):
    try:
        WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.ID, "j_idt422")))
        WebDriverWait(driver, 60000).until(EC.invisibility_of_element_located((By.ID, "j_idt422")))
    except:
        pass

def click_search_button(driver, wait):
    btn = wait.until(
        EC.element_to_be_clickable((By.ID, "tabs:form-despesas:j_idt86"))
    )
    driver.execute_script("arguments[0].click();", btn) 

def select_entity(driver):
    element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "tabs:form-despesas:buscarListagemEmpenhoPagoPorDespesaGruposelectOrgaoDespesas")))
    Select(element).select_by_value("12000")

def select_calendar(driver, year):
    data_ini = driver.find_element(By.ID, "tabs:form-despesas:j_idt66_input")
    data_ini.clear()
    data_ini.send_keys(f"01/01/{year}")

    time.sleep(3)

    data_final = driver.find_element(By.ID, "tabs:form-despesas:j_idt68_input")
    data_final.clear()
    data_final.send_keys(f"31/12/{year}")

def run_robot_for_year(year, city, downloads_folder, driver, wait, state):
    if state.is_ok(city["nome"], year, "P0"):
        return

    data_final = []
    driver.get(city["url"])

    select_calendar(driver, year)
    
    select_entity(driver)

    click_search_button(driver, wait)
    wait_loading(driver)

    click_empenho_paid(driver,wait)
    wait_loading(driver)


    data_final = extract_data(driver, wait)

    if not data_final:
        state.add(city["nome"], year, "P0", status="NO_DATA", portal_type="8", motivo="Tabela vazia ou erro na extração")
        return


    df = pd.DataFrame(data_final)
    
    if 'pago' in df.columns:
        df['pago'] = df['pago'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df['pago'] = pd.to_numeric(df['pago'], errors='coerce')
    
    df['ano_referencia'] = year
    df['municipio_nome'] = city["nome"]
    df['municipio_id'] = city["codigo_ibge"]


    if df is not None and not df.empty:
        output_dir = os.path.join("data", "Transparencia")
        os.makedirs(output_dir, exist_ok=True)

        io.save_consolidated_df(
            df=df,
            output_folder=output_dir,
            filename=f"MACEIO_CONSOLIDADO_8_{year}.csv"
        )
        
        state.add(city["nome"], year, "P0", status="OK", portal_type="8", detalhe=f"{len(df)} regs")
    else:
        state.add(city["nome"], year, "P0", status="FILTERED_EMPTY", portal_type="8", motivo="Sem registros de educação")
    

    io.clean_tmp_folder(downloads_folder)
    
    if driver:
        driver.quit()

def run(args):
    year, city, downloads_folder, state = args

    driver, wait = get_driver(downloads_folder, True)

    try:
        return run_robot_for_year(
            year=year,
            city=city,
            downloads_folder = downloads_folder,
            driver=driver,
            wait=wait,
            state=state)
    finally:
        driver.quit()

def exec8(cities_config, downloads_folder, state, progress_callback=None):
    MAX_WORKERS = 5
    
    for city in cities_config:
        jobs = [
            (year, city, downloads_folder, state)
            for year in city["years_list"]
        ]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(run, job) for job in jobs]

            for f in futures:
                try:
                    f.result()
                except Exception as e:
                    state.add(city["nome"], "0000", "P0", status="NO_DATA", portal_type="8", motivo=f"Erro: {e}")

        if progress_callback:
            progress_callback()