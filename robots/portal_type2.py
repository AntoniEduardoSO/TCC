import pandas as pd
import glob
import os
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys 

from .core.categories import categorize_cost

def imprimir_estado_atual(driver):
    print("\n--- [DEBUG] ESTADO ATUAL DA PÁGINA ---")
    
    # 1. O que está selecionado no Dropdown de Ano?
    try:
        select_ano = Select(driver.find_element(By.ID, "ddlAnoEmpenhos"))
        ano_selecionado = select_ano.first_selected_option.text
        print(f" > Dropdown 'Ano' mostra: {ano_selecionado}")
    except Exception as e:
        print(f" > Não consegui ler o dropdown: {e}")

    # 2. O que tem na primeira linha da tabela? (Isso é o mais importante!)
    try:
        # Pega a primeira linha do corpo da tabela
        primeira_linha = driver.find_element(By.CSS_SELECTOR, "#dataTables-Empenhos tbody tr")
        texto_linha = primeira_linha.text.strip()
        print(f" > Primeira linha de dados: {texto_linha[:100]}...") # Mostra os primeiros 100 caracteres
    except:
        print(" > Tabela vazia ou não encontrada.")

    # 3. Como está o botão CSV no HTML agora?
    try:
        # Pega o HTML exato do botão para ver se tem alguma classe 'disabled' ou atributo estranho
        btn = driver.find_element(By.CSS_SELECTOR, "a.buttons-csv")
        html_btn = btn.get_attribute('outerHTML')
        print(f" > HTML do botão CSV: {html_btn}")
    except:
        print(" > Botão CSV não encontrado no DOM.")
        
    print("----------------------------------------\n")

def _clean_tmp_folder(folder):
    files = glob.glob(os.path.join(folder, "*"))
    for f in files:
        try: os.remove(f)
        except: pass

def process_education_data(df):
    if df is None or df.empty:
        return None

    word_filter = 'educa|ensino|fundeb|merenda|semed|ensino|fundo de educa'

    keyword_col = ['org', 'unid', 'secr', 'centr', 'dep', 'setor']

    col_to_verify = []

    for col in df.columns:
        if any(key in col.lower() for key in keyword_col):
            col_to_verify.append(col)
    
    if not col_to_verify:
        col_to_verify = [c for c in df.columns if df[c].dtype == 'object'][5]

    if not col_to_verify:
        return None

    final_mask = pd.Series(False, index=df.index)

    for col in col_to_verify:
        col_mask = df[col].astype(str).str.contains(word_filter, case=False, na=False, regex=True)
        final_mask = final_mask | col_mask
    
    df_edu = df[final_mask].copy()

    if df_edu.empty:
        return None
    
    col_orgao_orig = next((c for c in col_to_verify if 'org' in c.lower()), None)
    col_unidade_orig = next((c for c in col_to_verify if 'unid' in c.lower()), None)
    
    if col_orgao_orig and col_unidade_orig:
        df_edu['Orgao_Consolidado'] = df_edu[col_orgao_orig].astype(str) + " - " + df_edu[col_unidade_orig].astype(str)
    elif col_unidade_orig:
        df_edu['Orgao_Consolidado'] = df_edu[col_unidade_orig]
    elif col_orgao_orig:
        df_edu['Orgao_Consolidado'] = df_edu[col_orgao_orig]
    else:
        df_edu['Orgao_Consolidado'] = df_edu[col_to_verify[0]]

    col_empenho = 'DsEmpenho' if 'DsEmpenho' in df_edu.columns else None
    col_item = 'DsItemDespesa' if 'DsItemDespesa' in df_edu.columns else None

    if col_empenho and col_item:
        df_edu['Descricao_Final'] = df_edu[col_empenho].fillna(df_edu[col_item]).fillna('')
    elif col_empenho:
        df_edu['Descricao_Final'] = df_edu[col_empenho].fillna('')
    elif col_item:
        df_edu['Descricao_Final'] = df_edu[col_item].fillna('')
    else:
        df_edu['Descricao_Final'] = 'Sem Descrição'

    df_edu['Categoria'] = df_edu['Descricao_Final'].apply(categorize_cost)

    cols_map = {
        'Data': 'Data',
        'Empenho': 'Empenho',
        'Orgao_Consolidado': 'Órgão', # Usa a nossa coluna tratada
        'Credor': 'Credor',
        'Empenhado': 'Valor',
        'Descricao_Final': 'Descrição',
        'Categoria': 'Categoria',
        'ano_referencia': 'Ano',
        'municipio_nome': 'Municipio_nome',
        'codigo_ibge': 'municipio_id'
    }

    cols_finais = [c for c in cols_map.keys() if c in df_edu.columns]
    df_final = df_edu[cols_finais].rename(columns=cols_map)
    
    return df_final


def transform(driver, wait, downloads_folder, year):
    _clean_tmp_folder(downloads_folder)

    btn_csv = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#dataTables-Empenhos_wrapper a.buttons-csv")))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_csv)
    driver.execute_script("arguments[0].click();", btn_csv)
    print(f"Cliquei no csv! do ano {year}")

    max_wait_time = 60

    start = time.time()
    while(time.time() - start) < max_wait_time:
        files = [
            f for f in os.listdir(downloads_folder) 
            if not f.startswith('.') 
            and f != '' 
            and os.path.isfile(os.path.join(downloads_folder, f)) # Garante que é arquivo
        ]

        if not files:
            time.sleep(1)
            continue

        absolut_path = os.path.join(downloads_folder, files[0])

        if absolut_path.endswith('.crdownload') or absolut_path.endswith('.tmp'):
            time.sleep(2)
            continue

        if os.path.getsize(absolut_path) > 0:
            time.sleep(3)

        encondings = ['utf-8', 'latin-1', 'cp1252']

        separates = [';', ',', '\t']

        for encoding in encondings:
            for sep in separates:
                df = pd.read_csv(
                    absolut_path,
                    sep = sep,
                    encoding = encoding,
                    engine='python',
                    on_bad_lines='skip'
                )
                if df.shape[1] < 2:
                    continue

                os.remove(absolut_path)
                return df
        
        os.remove(absolut_path)
        return df
    time.sleep(1)

    return None

def _select_calendar(driver, element_id, text, timeout=10):
    wait = WebDriverWait(driver, timeout)
    field = wait.until(EC.element_to_be_clickable((By.ID, element_id)))

    driver.execute_script("arguments[0].value = '';", field)

    field.send_keys(text)
    

def _select_dropdown_year(driver, element_id, text):
    element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, element_id)))
    Select(element).select_by_visible_text(str(text))

def _wait_loading(driver):
    WebDriverWait(driver, 5).until(
        EC.visibility_of_element_located((By.ID, "loading"))
    )

    WebDriverWait(driver, 6000).until(
        EC.invisibility_of_element_located((By.ID, "loading"))
    )

def exec2(cities, driver, wait, downloads_folder):

    for city in cities:
        df_city = []
        years = city.get("years_list", [])

        for year in years:
            driver.get(city['url'])

            tab_empenhos = wait.until(EC.element_to_be_clickable((By.ID, "lnkEmpenhos")))
            driver.execute_script("arguments[0].click();", tab_empenhos)
            print(f"Fui para aba de empenhos! do ano {year}")

            _wait_loading(driver)
            print(f"Esperei o primeiro loading! do ano {year}")
            imprimir_estado_atual(driver)

            _select_dropdown_year(driver, "ddlAnoEmpenhos", year)
            print(f"Coloquei os ano desejado! do ano {year}")

            element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "ddlMesEmpenhos")))
            Select(element).select_by_visible_text(str("Selecione"))

            start_date = f"01/01/{year}"
            end_date = f"31/12/{year}"

            _select_calendar(driver, "txtDtInicioEmpenhos", start_date)
            _select_calendar(driver, "txtDtFimEmpenhos", end_date)
            print(f"coloquei as datas do calendario! do ano {year}")

            tabela_velha = driver.find_element(By.CSS_SELECTOR, "#dataTables-Empenhos tbody")

            time.sleep(3)
            search_btn = driver.find_element(By.ID, "btnFiltrarEmpenhos")
            search_btn.click()
            driver.execute_script("arguments[0].click();", search_btn)
            
            print(f"Cliquei em buscar os dados! do ano {year}")

            _wait_loading(driver)
            print(f"Esperei o segundo loading! do ano {year}")

            imprimir_estado_atual(driver)

            if tabela_velha:
                try:
                    wait.until(EC.staleness_of(tabela_velha))
                except TimeoutException:
                    print(" [!] A tabela não atualizou! Talvez o filtro tenha falhado ou a internet caiu.")
                    continue

            try:
                primeira_linha = driver.find_element(By.CSS_SELECTOR, "#dataTables-Empenhos tbody tr").text
                if str(year) not in primeira_linha and "Nenhum registro" not in primeira_linha:
                    # Se eu pedi 2017, mas na tabela não tem "2017", algo deu errado.
                    # Nota: Ajuste essa lógica se a data não aparecer na primeira linha da tabela visualmente
                    print(f" [ALERT] Pedi {year} mas parece que a tabela ainda mostra dados errados. Texto: {primeira_linha[:50]}...")
                    # Aqui você pode decidir dar um 'continue' ou tentar esperar mais
            except:
                pass

            df_city_per_year = transform(driver, wait, downloads_folder, year)

            if df_city_per_year is not None:

                df_city_per_year['ano_referencia'] = year
                df_city_per_year['municipio_nome'] = city["nome"]
                df_city_per_year['municipio_id'] = city["codigo_ibge"]

                df_city_per_year = process_education_data(df_city_per_year)

                if df_city_per_year is not None and not df_city_per_year.empty:
                    df_city.append(df_city_per_year)
                    print(f" [V] {len(df_city_per_year)} registros de Educacao capturados do {city['nome']}, ano{year} do portal 2")

                else:
                    print(f" [!] Nenhum registro de Educação encontrado em {year} do municipio {city['nome']}, do portal 2")
            else:
                print(f" [!]  Nenhum dado retornado para {year} do municipio {city['nome']}, do portal 2.")



        if df_city:
            final_folder = os.path.join(os.getcwd(), "data", "Transparencia")
            os.makedirs(final_folder, exist_ok=True)
            df_final = pd.concat(df_city, ignore_index=True)
            path_final = os.path.join(final_folder, f"{city['nome']}_CONSOLIDADO.csv")
            df_final.to_csv(path_final, index=False, sep=';', encoding='utf-8-sig')