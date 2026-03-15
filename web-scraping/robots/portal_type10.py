import requests
import pandas as pd
import os

from .core import io
from .core import transform

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

terms = ['EDUCA', 'FUNDEB', 'PNAE', 'MERENDA', 'ALIMENTA', 'ESCOLA', 'CRECHE', 'ENSINO', 'PROFESSOR', 'PROFESSORA', "TRANSPORTE ESCOLAR", "FUNDEO DE EDUCA"]
colunas = ['municipio_nome', 'municipio_id', 'detalhes', 'data', 'valor', 'acao', 'id']

def exec10(cities_config, downloads_folder, state, progress_callback=None):
    for city in cities_config:
        if state.is_ok(city["nome"], 0000, "P0"):
            continue

        dados_processados = []

        response = requests.get(city["url"], timeout=120, headers=headers)

        if response.status_code == 200:
            dados = response.json()

            for empenho in dados:

                lista_liq = empenho.get('liquidacoes', [])

                for liq in lista_liq:
                    texto_justificativa = str(liq.get('Justificativa') or "")
                    acao = (empenho.get('acao') or {}).get('Descricao')
                    id = empenho.get('id')

                    for term in terms:
                        if term in texto_justificativa.upper():

                            row = [
                                city["nome"],
                                city["codigo_ibge"],
                                texto_justificativa,
                                empenho.get('DataEmissao', ''),
                                empenho.get('Valor', 0),
                                acao,
                                id
                            ]

                            dados_processados.append(row) 
                            break
            
            if not dados_processados:
                state.add(city["nome"], 0000, "P0", status="NO_DATA", portal_type="10", motivo="Sem dados da educação")
                continue
                
            

            df_city = pd.DataFrame(dados_processados, columns=colunas)

            if len(df_city) <= 100:
                state.add(city["nome"], 0000, "P0", status="NO_DATA", portal_type="10", motivo="Sem dados ou erro download")


            df_city['valor'] = pd.to_numeric(df_city['valor'], errors='coerce')
                
            df_city['data'] = pd.to_datetime(df_city['data'], errors='coerce')
            df_city['ano_referencia'] = df_city['data'].dt.year
            df_city['data'] = df_city['data'].dt.strftime('%d/%m/%Y')

            df_city['codigo_ibge'] = city["codigo_ibge"]

            if df_city is not None and not df_city.empty:
                output_dir = os.path.join("data", "Transparencia")
                os.makedirs(output_dir, exist_ok=True)

                colunas_identidade = [
                    'municipio_nome', 
                    'municipio_id', 
                    'detalhes', 
                    'data', 
                    'valor', 
                    'acao',
                    'ano_referencia',
                    'codigo_ibge'
                ]

                df_city = df_city.drop_duplicates(subset=colunas_identidade, keep='first')
                df_city = df_city.drop_duplicates(subset=['id'], keep='first')

                io.save_consolidated_df(
                    df=df_city,
                    output_folder=output_dir,
                    filename=f"{city['nome']}_CONSOLIDADO_10.csv"
                )

                state.add(city["nome"], 0000, "P0", status="OK", portal_type="10", detalhe=f"{len(df_city)} regs")

            else:
                state.add(city["nome"], 0000, "P0", status="FILTERED_EMPTY", portal_type="10", motivo="Nenhum dado sobrou após filtro")

            io.clean_tmp_folder(downloads_folder)

            if progress_callback:
                progress_callback()

        
        else:
            state.add(city["nome"], 0000, "P0", status="NO_DATA", portal_type="10", motivo="Sem dados ou erro download")