import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from .core import io

import pdfplumber
import pandas as pd
import os
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

terms = {"despesas orçadas", "despesas orcadas", "despesas_orcadas", "despesas_orçadas"}
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

def get_session():
    session = requests.Session()
    retry = Retry(
        total=5, 
        backoff_factor=1, 
        status_forcelist=[500, 502, 503, 504, 104], 
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(headers)
    return session

def etl_pdf(city, downloads_folder, state):

    df = []

    for year in city['years_list']:
        if state.is_ok(city["nome"], year, "P0"):
            continue

        pdf_path = f"{downloads_folder}/despesa_{year}.pdf"

        with pdfplumber.open(pdf_path) as pdf:
            
            for page in pdf.pages:
                achou = False
                df_per_year_len = 0
            
                dados_brutos = page.extract_table()

                if not dados_brutos:
                    continue            

                for linha in dados_brutos:
                    linha_tratada = [str(celula).replace('\n', ' ').strip() if celula else "" for celula in linha]

                    if not any(linha_tratada):
                        continue

                    if "Registro" in linha_tratada[0] or "Cód." in str(linha_tratada[2]):
                        continue
                        
                    if "TOTAL" in str(linha_tratada).upper() or "Registros:" in linha_tratada[0]:
                        continue
                    
                    if len(linha_tratada) >= 8:
                        df_per_year_len +=len(linha_tratada)
                        achou = True
                        df.append(linha_tratada[:8])

        if achou:
            state.add(city["nome"], year, "P0", status="OK", portal_type="9", detalhe=f"{df_per_year_len} regs")


    colunas = ["Registro", "Ano", "Cod_Acao", "Acao", "Cod_Despesa", "Despesa", "Valor_Orcado", "Valor_Atualizado"]
    df = pd.DataFrame(df[1:], columns=colunas)

    for col in ["Valor_Orcado", "Valor_Atualizado"]:
        df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')

    output_dir = os.path.join("data", "Transparencia")
    os.makedirs(output_dir, exist_ok=True)

    io.save_consolidated_df(
        df=df,
        output_folder=output_dir,
        filename=f"{city['nome']}_CONSOLIDADO_9.csv"
    )
    io.clean_tmp_folder(downloads_folder)

def find_and_download_files(links, city, downloads_folder, state, session):
    for link in links:
        href = link['href']
        titulo = link.get('title', '').lower()
        texto = link.get_text().lower()

        if '.pdf' in href and ('educação' in titulo or 'educacao' in titulo or 'educação' in texto or 'educacao' in texto):
            for term in terms:
                if term in titulo or term in texto:
                
                    url_pdf = urljoin(city['url'], href)

                    for year in city['years_list']:

                        if year in href:
                            nome_arquivo = f"despesa_{year}"
                    
                    if not nome_arquivo.lower().endswith('.pdf'):
                        nome_arquivo += ".pdf"
                    
                    caminho_arquivo = os.path.join(downloads_folder, nome_arquivo)
                    
                    try:
                        pdf_resp = session.get(url_pdf, timeout=60, verify=False)
                        pdf_resp.raise_for_status()

                        with open(caminho_arquivo, 'wb') as f:
                            f.write(pdf_resp.content)
                        break
                    except Exception as e:
                        state.add(city["nome"], year, "P0", status="NO_DATA", portal_type="9", motivo="Sem dados ou erro download")
                        break

def exec9(cities_config, downloads_folder, state, progress_callback=None):
    session = get_session()

    for city in cities_config:
        response = session.get(city["url"], timeout=30, verify=False)
        response.raise_for_status()

        if response.status_code != 200:
            state.add(city["nome"], 0000, "P0", status="NO_DATA", portal_type="9", motivo="Sem dados ou erro download")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=True)

        find_and_download_files(links, city, downloads_folder, state, session)

        etl_pdf(city,downloads_folder, state)

        io.clean_tmp_folder(downloads_folder)

        if progress_callback:
                progress_callback()
