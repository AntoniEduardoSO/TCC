import pandas as pd
import numpy as np
import os

from robots.core import io

from .ratings import create_rating_table, enrich_ratings_with_learning_metrics

from robots.processing.school_census import get_school_census_file
from robots.processing.school_perfomance_rate import get_school_perfomance_rate_file
from robots.processing.school_ideb import get_school_ideb_file

# Colunas essenciais para o merge e novas tabelas.
col_identify = ['NU_ANO_CENSO', 'CO_ENTIDADE']
col_dict = ['id_atributo', 'variavel', 'descricao', 'tipo', 'tamanho', 'grupo']

# Diretorio atual
dir_atual = os.path.dirname(os.path.abspath(__file__))

def fix_dtypes(df_dict, df):
    
    # Adicionando num dicionario os valores corretos de cada tipo.
    school_schema = {}

    for _, row in df_dict.iterrows():
        if row['tipo'] == 'Char':
            school_schema[row['variavel']] = 'str' # varchar normal.
        elif row['tipo'] == 'Num' and row['tamanho'] == 1:
            school_schema[row['variavel']] = 'Int8' # inteiro pequeno para Booleanos
        else:
            school_schema[row['variavel']] = 'Int64' # Quantidades
    
    # Renomeando os tipos de dados para o valor correto.
    df_dict.loc[(df_dict['tipo'] == 'Num') & (df_dict['tamanho'] == 1), 'tipo'] = 'Bool'

    data = df.query("SG_UF == 'AL' and TP_DEPENDENCIA < 4").copy()

    # Limpeza de strings para evitar espacoes e converter em nan.
    cols_object = data.select_dtypes(include=['object']).columns
    for col in cols_object:
        data.loc[:, col] = data[col].fillna('').str.strip()

    return data

def generate_optimized_tables(data, df_dict):
    # Pega as colunas que sao quantitativas.
    condicao_quant = (
        (df_dict['variavel'].str.startswith('QT')) | 
        ((df_dict['tipo'] == 'Num') & (df_dict['tamanho'] > 1))
    )
    col_quant = df_dict.loc[condicao_quant, 'variavel'].tolist()

    # Intersecao entre o dicionario df_dict e as colunas presentes do data original.

    col_present = [c for c in col_quant if c in data.columns]

    # Estamos descartando o 0 por nulo apenas para os valores quantitativos.
    data[col_present] = data[col_present].replace(0, np.nan)

    return data

def create_school_perfomance_rate(df):
    output_dir = os.path.join("..", "data", "Matricula")
    os.makedirs(output_dir, exist_ok=True)

    path = os.path.join(dir_atual, "..", "data/Matricula/school_perfomance_rate.csv")
    save_incremental(df, path)

def create_school_info(data, df_dict, year):

    col_adr = ['DS_ENDERECO', 'NU_ENDERECO', 'DS_COMPLEMENTO', 'NO_BAIRRO', 'CO_CEP']
    col_cellphone = ['NU_DDD', 'NU_TELEFONE']
    unwanted_col = ['NO_UF', 'SG_UF', 'CO_UF']


    filtro_geral = df_dict[df_dict['area'] == 'GERAL']
    school_info = data[filtro_geral['variavel']].copy()

    # Trocando '' para 'NaN'
    for col in col_adr:
        school_info[col] = school_info[col].astype(str).replace('NaN', '').str.strip()

    # Colocando o ano
    school_info['ano'] = year

    # Agrupando as colunas_endereco em uma so para endereco.
    school_info['endereco'] = (
        school_info['DS_ENDERECO'] + ", " + 
        school_info['NU_ENDERECO'] + ", " + 
        school_info['DS_COMPLEMENTO'] + ", " + 
        school_info['NO_BAIRRO'] + ", " + 
        school_info['CO_CEP']
    )
    
    school_info['telefone'] = (
        school_info['NU_DDD'] + school_info['NU_TELEFONE']
    ).astype(str)

    # Renomeando as colunas.
    school_info = school_info.rename(columns={
        'NO_ENTIDADE' : 'nome_escola',
        'CO_ENTIDADE' : 'id_escola',
        'NO_MUNICIPIO' : 'nome_municipio',
        'CO_MUNICIPIO' : 'municipio_id',
        'NO_MESORREGIAO' : 'nome_mesorregiao',
        'CO_MESORREGIAO' : 'id_mesorregiao',
        'NO_MICRORREGIAO': 'nome_microrregiao',
        'CO_MICRORREGIAO': 'id_microrregiao',
        'TP_DEPENDENCIA' : 'dependencia',
        'TP_LOCALIZACAO' : 'localizacao',
        'TP_SITUACAO_FUNCIONAMENTO' : 'funcionamento',
        'CO_ESCOLA_SEDE_VINCULADA' : 'sede',
        'IN_LOCAL_FUNC_PREDIO_ESCOLAR' : 'alocacao',
        'TP_OCUPACAO_PREDIO_ESCOLAR' : 'ocupacao'
    })

    school_info = school_info.drop(columns=col_adr)
    school_info = school_info.drop(columns=col_cellphone)
    school_info = school_info.drop(columns = unwanted_col)
    school_info['alocacao'] = school_info['alocacao'].astype(int)
    school_info['ocupacao'] = school_info['ocupacao'].astype(int)

    path = os.path.join(dir_atual, "..", "data/Geral/school_info.csv")
    save_incremental(school_info, path)
    
    return school_info

def create_infrastructure(data, df_dict):

    output_dir = os.path.join("data", "Infraestrutura")
    os.makedirs(output_dir, exist_ok=True)

    # Carregando o dicionario para infraestrutura.
    infra_dict_base = df_dict[df_dict['area'] == 'INFRAESTRUTURA'].copy()

    # Criando uma coluna para ter um id unico para cada coluna.
    infra_dict_base['id_atributo'] = range(1,len(infra_dict_base) + 1)

    # Organizando o csv dicionario de acordo com as colunas necessarias.

    infra_metadata = infra_dict_base[col_dict]
    infra_metadata.to_csv(os.path.join(dir_atual, "..", "data/Infraestrutura/infrastructure_dict.csv"), index=False, encoding='utf-8-sig')


    vars_infra = infra_dict_base['variavel'].tolist()

    valid_vars_infra = [var for var in vars_infra if var in data.columns]

    final_infra = data[col_identify + valid_vars_infra].melt(
        id_vars=col_identify, 
        value_vars=valid_vars_infra,
        var_name='variavel', 
        value_name='valor'
    ).dropna(subset=['valor']) # Removendo as linhas marcadas por nulo (funcao optimized_tables)

    # Fazendo o merge de colunas para linhas.

    final_infra = final_infra.merge(infra_metadata[['id_atributo', 'variavel', 'tipo']], on='variavel')

    # Alterando os nomes das colunas.
    final_infra = final_infra.rename(columns={'NU_ANO_CENSO': 'ano', 'CO_ENTIDADE': 'id_escola', 'tipo': 'tipo_atributo'})

    # Criando Id unico
    final_infra['id'] = range(1, len(final_infra) + 1)


    path = os.path.join(dir_atual, "..", "data/Infraestrutura/infrastructure_values.csv")

    path_dict = os.path.join(dir_atual, "..", "data/Infraestrutura/infrastructure_dict.csv")
    if not os.path.exists(path_dict):
        infra_metadata.to_csv(path_dict, index=False, encoding='utf-8-sig')

    cols_to_save = ['id', 'ano', 'id_escola', 'id_atributo', 'tipo_atributo', 'valor']
    save_incremental(final_infra[cols_to_save], path)

    return final_infra

def create_school_enrollment(data, df_dict):


    # Repeteco da funcao infraestructure, leia ele primeiro.

    enroll_dict_base = df_dict[df_dict['area'] == 'MATRICULA'].copy()

    enroll_dict_base['id_atributo'] = range(1,len(enroll_dict_base) + 1)

    enroll_metadata = enroll_dict_base[col_dict]
    enroll_metadata.to_csv(os.path.join(dir_atual, "..", "data/Matricula/enroll_dict.csv"), index=False, encoding='utf-8-sig')


    vars_enroll = enroll_dict_base['variavel'].tolist()

    valid_vars_enroll = [var for var in vars_enroll if var in data.columns]

    final_enroll = data[col_identify + valid_vars_enroll].melt(
        id_vars=col_identify, 
        value_vars=valid_vars_enroll,
        var_name='variavel', 
        value_name='valor'
    ).dropna(subset=['valor'])

    final_enroll = final_enroll.merge(enroll_metadata[['id_atributo', 'variavel', 'tipo']], on='variavel')

    final_enroll = final_enroll.rename(columns={'NU_ANO_CENSO': 'ano', 'CO_ENTIDADE': 'id_escola', 'tipo': 'tipo_atributo'})
   
    final_enroll['id'] = range(1, len(final_enroll) + 1)

    path = os.path.join(dir_atual, "..", "data/Matricula/enroll_values.csv")
    
    path_dict = os.path.join(dir_atual, "..", "data/Matricula/enroll_dict.csv")
    if not os.path.exists(path_dict):
        enroll_metadata.to_csv(path_dict, index=False, encoding='utf-8-sig')

    cols_to_save = ['id', 'ano', 'id_escola', 'id_atributo', 'tipo_atributo', 'valor']
    save_incremental(final_enroll[cols_to_save], path)
    
    return final_enroll

def save_incremental(df, filepath):
    file_exists = os.path.exists(filepath)

    df.to_csv(filepath, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')

def remove_files():
    output_files = [
        "data/Geral/school_info.csv",
        "data/Geral/school_ratings.csv",
        "data/Infraestrutura/infrastructure_values.csv",
        "data/Infraestrutura/infrastructure_dict.csv",
        "data/Matricula/enroll_values.csv",
        "data/Matricula/enroll_dict.csv"
    ]

    for f in output_files:
        full_path = os.path.join(dir_atual, "..", f)
        if os.path.exists(full_path):
            os.remove(full_path)

def exec_processing():

    base_dir = os.getcwd()

    downloads_folder = os.path.join(base_dir, "data", "raw")

    remove_files()
    io.clean_tmp_folder(downloads_folder)

    year = 2025
    i = 1

    # Carregando o dicionario.
    df_dict = pd.read_csv(os.path.join(dir_atual, "..", "dicionario.csv"))

    while i <= 8:
        current_year = year - i

        create_school_perfomance_rate(get_school_perfomance_rate_file(i))

        df = get_school_census_file(i)

        # Corrigindo os tipos de valores atraves do dicionario.csv e retornando o data limpo.
        data = fix_dtypes(df_dict, df)

        # Adicionando etiquetas para colunas quantitativas iguais a 0, evitando muitas linhas desnecessarias.
        data = generate_optimized_tables(data, df_dict)

        # Criando csv para school_info.csv
        df_info = create_school_info(data, df_dict, current_year)
        # Criando csv para infrastructure e dict_infraestructure
        df_infra_long = create_infrastructure(data, df_dict)

        # Criando csv para enrollment e dict_enrollment
        df_enroll_long = create_school_enrollment(data, df_dict)

        # Criando csv para tabelas de rating.
        create_rating_table(df_infra_long, df_enroll_long, df_info, df_dict, current_year, dir_atual)

        i += 1

        io.clean_tmp_folder(downloads_folder) 

    get_school_ideb_file()

    enrich_ratings_with_learning_metrics()
