import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import pdfplumber
import pandas as pd
import os

import pdfplumber
import pandas as pd
import os


terms = {"despesas orçadas", "despesas orcadas", "despesas_orcadas", "despesas_orçadas"}
years = {"2020", "2021", "2022", "2023"}

# URL da página de transparência que contém os arquivos
URL_FONTE = "https://saoluisdoquitunde.al.gov.br/receitas-e-despesas"
PASTA_DESTINO = "despesas_educacao"

def baixar_pdfs():
    # Cria a pasta se não existir
    if not os.path.exists(PASTA_DESTINO):
        os.makedirs(PASTA_DESTINO)

    print(f"Acessando {URL_FONTE}...")
    headers = {'User-Agent': 'Mozilla/5.0'} # Simula um navegador real
    response = requests.get(URL_FONTE, headers=headers)
    
    if response.status_code != 200:
        print("Erro ao acessar o site.")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Encontra todos os links da página
    links = soup.find_all('a', href=True)
    
    contador = 0
    for link in links:
        href = link['href']
        titulo = link.get('title', '').lower()
        texto = link.get_text().lower()
        
        # FILTRO: Verifica se é PDF e se tem palavras-chave de interesse
        # O site usa variações como "Despesas Orçadas", "Relatorio_despesas", etc.
        # Filtramos por "educação" (ou educacao) e "despesas" para garantir.
        if '.pdf' in href and ('educação' in titulo or 'educacao' in titulo or 'educação' in texto or 'educacao' in texto):
            for term in terms:

                if term in titulo or term in texto:
                
                    url_pdf = urljoin(URL_FONTE, href)

                    for year in years:
                        if year in href:
                            nome_arquivo = f"despesa_{year}"
                    
                    if not nome_arquivo.lower().endswith('.pdf'):
                        nome_arquivo += ".pdf"
                    
                    # Limpa caracteres inválidos do nome do arquivo se necessário, ou usa o nome original
                    caminho_arquivo = os.path.join(PASTA_DESTINO, nome_arquivo)
                    
                    
                    try:
                        pdf_resp = requests.get(url_pdf, headers=headers, timeout=30)
                        with open(caminho_arquivo, 'wb') as f:
                            f.write(pdf_resp.content)
                        contador += 1
                        break
                    except Exception as e:
                        print(f"Erro ao baixar {url_pdf}: {e}")
                        break

    print(f"\nConcluído! Total de arquivos baixados: {contador}")

    # Caminho para o seu arquivo PDF

    df = []
    for year in years:

        pdf_path = f"despesas_educacao/despesa_{year}.pdf"

        with pdfplumber.open(pdf_path) as pdf:
            
            for page in pdf.pages:
            
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
                        df.append(linha_tratada[:8])


    colunas = ["Registro", "Ano", "Cod_Acao", "Acao", "Cod_Despesa", "Despesa", "Valor_Orcado", "Valor_Atualizado"]
    df = pd.DataFrame(df[1:], columns=colunas)

    for col in ["Valor_Orcado", "Valor_Atualizado"]:
        df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df.to_csv('LUIS_DO_QUINTUDE_CONSOLIDADO_9.csv', index=False, encoding='utf-8-sig', sep=';')

if __name__ == "__main__":
    baixar_pdfs()