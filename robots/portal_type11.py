import requests
import pandas as pd
import re
import unidecode
import os

from .core import io

from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://www.fnde.gov.br/olinda-ide/servico/DADOS_ABERTOS_SIOPE/versao/v1/odata/Despesas_Siope"

cols_to_drop = [
    'IDN_EXIB_CODI',
    'IDN_CLAS',
    'NUM_ORDE',
    'COD_FONTE',
    'TIPO',
    'COD_UF',
    'SIG_UF',
    'NUM_NIVE',
    'COD_EXIB'
]

years = range(2017, 2025)
periods = ["01", "02", "03", "04", "05", "06"]
uf = "AL"

def classify(nom_pasta, nom_item):

    pasta = normalize(nom_pasta)
    item = normalize(nom_item)

    texto = f"{pasta} {item}"

    # --------------------
    # EXCLUSÕES / INDICADORES
    # --------------------

    if "total das despesas" in texto:
        return ("EXCLUIR", "FUNDEB", "Indicador")

    # --------------------
    # PESSOAL (REGRAS FORTES)
    # --------------------

    if any(k in texto for k in [
        "contratacao por tempo determinado",
        "contrato temporario",
        "tempo determinado",
        "substituicoes",
        "substituicao",
        "substituto",
        "contrato temporario",
        "tempo determinado",
        "temporario",
    ]):
        return ("Pessoal", "Magistério/Docentes", "Contrato Temporário")

    if any(k in texto for k in [
        "vencimentos",
        "salario",
        "gratificacao",
        "vantagens",
        "abono",
        "13",
        "adicional"
    ]):
        if "folha" in texto and any(k in texto for k in [
            "professor",
            "professores",
            "coordenador",
            "coordenadores"
            "ensino",
            "semed",
            "educacao",
        ]):
            return ("Pessoal", "Magistério/Docentes", "Remuneração")

    if "fundeb 60" in texto and "folha" in texto:
        return ("Pessoal", "Magistério/Docentes", "Remuneração")

    if any(k in texto for k in [
        "inss",
        "obrigacoes patronais",
        "previdenciarias",
        "contribuicoes previdenciarias",
        "previdenciaria",
        "guia de recolhimento",
        "inss",
        "previdenci",
        "iprev",
        "fupre",
        "parte patronal",
        "contribuicao patronal"
    ]):
        return ("Pessoal", "Magistério/Docentes", "Encargos")
    
    if any(k in texto for k in [
        "hora aula",
        "hora-aula",
        "complemento de hora",
        "carencia real"
    ]):
        return ("Pessoal", "Magistério/Docentes", "Remuneração")

    # --------------------
    # ALIMENTAÇÃO ESCOLAR
    # --------------------

    if any(k in texto for k in [
        "merenda",
        "alimentacao escolar",
        "generos alimenticios",
        "generos de alimentacao"
    ]):
        return ("Alimentação Escolar", "Consumo", "Alimentos")

    if "alimentacao" in item and "difusao cultural" in pasta:
        return ("Eventos Educacionais", "Eventos", "Alimentação em Eventos")
    
    if "evento" in texto and any(k in texto for k in [
        "lanche",
        "alimentacao",
        "coffee break"
    ]):
        return ("Eventos Educacionais", "Eventos", "Alimentação em Eventos")

    # --------------------
    # TRANSPORTE ESCOLAR
    # --------------------

    if "transporte escolar" in texto:
        return ("Transporte Escolar", "Operação", "Serviços de Transporte")

    if any(k in texto for k in [
        "combustivel",
        "lubrificante"
    ]):
        return ("Transporte Escolar", "Operação", "Combustível")

    if "veiculo" in texto:
        return ("Transporte Escolar", "Manutenção", "Veículos")

    if "seguro" in texto and "transporte" in texto:
        return ("Transporte Escolar", "Operação", "Seguros")
    
    if any(k in texto for k in [
        "motorista",
        "motoristas",
        "condutores de veiculos",
        "condutor de veiculo"
    ]):
        return ("Transporte Escolar", "Operação", "Motoristas")

    # --------------------
    # INFRAESTRUTURA ESCOLAR
    # --------------------

    if any(k in texto for k in [
        "manutencao",
        "conservacao"
    ]):
        return ("Infraestrutura Escolar", "Manutenção", "Manutenção Predial")

    if "limpeza" in texto:
        return ("Infraestrutura Escolar", "Manutenção", "Limpeza e Conservação")

    if any(k in texto for k in [
        "obras",
        "reforma",
        "construcao"
    ]):
        return ("Infraestrutura Escolar", "Obras", "Construção")

    if "mobiliario" in texto:
        return ("Infraestrutura Escolar", "Equipamentos", "Mobiliário")

    if "energia" in texto:
        return ("Infraestrutura Escolar", "Utilidades", "Energia")

    if any(k in texto for k in ["agua", "esgoto"]):
        return ("Infraestrutura Escolar", "Utilidades", "Água e Esgoto")

    if "gas" in texto:
        return ("Infraestrutura Escolar", "Utilidades", "Gás")

    if "instalac" in texto:
        return ("Infraestrutura Escolar", "Infraestrutura Física", "Instalações")
    
    if "locacao" in texto and any(k in texto for k in [
        "imovel",
        "escola",
        "predio"
    ]):
        return ("Infraestrutura Escolar", "Infraestrutura Física", "Locação de Imóveis")

    # --------------------
    # TECNOLOGIA
    # --------------------

    if "material de processamento de dados" in texto:
        return ("Infraestrutura Escolar", "Tecnologia Educacional", "Suprimentos de TI")

    if "processamento de dados" in texto:
        return ("Infraestrutura Escolar", "Tecnologia Educacional", "Manutenção de TI")

    if any(k in texto for k in ["informatica", "computador"]):
        return ("Infraestrutura Escolar", "Equipamentos", "Tecnologia da Informação")

    # --------------------
    # RECURSOS PEDAGÓGICOS
    # --------------------

    if any(k in texto for k in [
        "uniformes",
        "material escolar"
    ]):
        return ("Recursos Pedagógicos", "Material Escolar", "Insumos")

    if "esportivo" in texto:
        return ("Recursos Pedagógicos", "Material Educacional", "Material Esportivo")

    # --------------------
    # SERVIÇOS OPERACIONAIS
    # --------------------

    if "servicos bancarios" in texto or "tarifas bancarias" in texto:
        return ("Serviços e Operação", "Financeiro", "Serviços Bancários")

    if "servicos de terceiros" in texto:
        return ("Serviços e Operação", "Serviços Terceirizados", "PJ")

    if "locacao" in texto:
        return ("Serviços e Operação", "Locação", "Bens")

    # --------------------
    # ADMINISTRAÇÃO
    # --------------------

    if any(k in texto for k in [
        "material de expediente",
        "material de consumo"
    ]):
        return ("Gestão e Administração", "Materiais", "Materiais Administrativos")

    if any(k in texto for k in [
        "consultoria",
        "publicidade",
        "comunicacao",
        "telecomunicacoes",
        "copias",
        "reproducao",
        "servicos tecnicos",
        "servicos profissionais"
    ]):
        return ("Gestão e Administração", "Serviços", "Serviços Administrativos")

    if any(k in texto for k in [
        "sentenca",
        "precatorio",
        "judicial"
    ]):
        return ("Gestão e Administração", "Judicial", "Sentenças Judiciais")

    if any(k in texto for k in [
        "contribuicoes",
        "fgts",
        "taxas",
        "tributarias"
    ]):
        return ("Gestão e Administração", "Encargos", "Tributos e Contribuições")

    if any(k in texto for k in [
        "auxilio",
        "beneficio",
        "pensao"
    ]):
        return ("Gestão e Administração", "Transferências", "Benefícios")
    
    if "estagiario" in texto or "estagiarios" in texto:
        return ("Pessoal Administrativo", "Pessoal", "Estagiários")

    # --------------------
    # FALLBACK
    # --------------------

    return ("Gestão e Administração", "Administrativo", "Outros")

def normalize(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = unidecode.unidecode(text)
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def clean_and_optimize_df(data_list):

    if not data_list:
        return pd.DataFrame()

    df = pd.DataFrame(data_list)
    df = df.reset_index(drop=True)

    df = df.sort_values(by='NUM_NIVE', ascending=False)

    id_columns = ['NUM_ANO', 'NUM_PERI', 'COD_MUNI', 'COD_PAST']
    df = df.drop_duplicates(subset=id_columns, keep='first')

    df['VAL_DECL'] = pd.to_numeric(df['VAL_DECL'], errors='coerce').fillna(0)

    key_columns = ['COD_MUNI', 'NUM_ANO', 'NOM_PAST', 'TIP_PASTA', 'COD_EXIB_FORMATADO', 'COD_PAST']

    df = df.sort_values(by=key_columns + ['NUM_PERI'])

    df['VALOR_REAL_BIMESTRE'] = (
        df.groupby(key_columns)['VAL_DECL']
        .diff()
        .fillna(df['VAL_DECL'])
        .round(2)
    )


    df = df.drop(columns=cols_to_drop, errors="ignore")

    df = df.reset_index(drop=True)

    return df


def request_data(year, period, state):
    url = (
        f"{BASE_URL}"
        f"(Ano_Consulta=@Ano_Consulta,Num_Peri=@Num_Peri,Sig_UF=@Sig_UF)"
        f"?@Ano_Consulta={year}"
        f"&@Num_Peri={period}"
        f"&@Sig_UF='{uf}'"
        f"&$filter=TIPO eq 'Municipal' and NOM_COLU eq 'Desp. Liquidadas'"
        f"&$format=json"
    )

    try:
        with requests.Session() as session:
            response = session.get(url, timeout=30000)

            if response.status_code == 200:
                state.add("muni", year, period, status="OK", portal_type="11", detalhe=f"{len(response.json())} regs")
                return response.json().get("value", [])

            else:
                state.add("muni", year, period, status="ERROR", portal_type="11", motivo = "Fallback de error ou timeoutexception")
                return []

    except requests.RequestException as e:
        print(f"Request failed for {year}-{period}: {e}")
        return []


def exec11(cities_config, downloads_folder, state, progress_callback=None):
    data_list = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(request_data, year, period, state)
            for year in years
            for period in periods
        ]

        for future in as_completed(futures):

            result = future.result()
            if result:
                data_list.extend(result)
            
            if progress_callback:
                progress_callback()

    df = clean_and_optimize_df(data_list)

    if df.empty or 'NOM_PAST' not in df.columns or 'NOM_ITEM' not in df.columns:
        return 

    df_unique = df[['NOM_PAST','NOM_ITEM']].drop_duplicates()

    df_unique[['EIXO','MACRO','MICRO']] = df_unique.apply(
        lambda row: classify(row['NOM_PAST'],row['NOM_ITEM']),
        axis=1,
        result_type="expand"
    )

    df = df.merge(df_unique, on=['NOM_PAST','NOM_ITEM'], how='left')

    output_dir = os.path.join("data", "Transparencia")
    os.makedirs(output_dir, exist_ok=True)

    io.save_consolidated_df(
        df=df,
        output_folder=output_dir,
        filename=f"MUNICIPIOS_CONSOLIDADO_11.csv"
    )