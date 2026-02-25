import pandas as pd
import numpy as np
import os

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
    )

    # Renomeando as colunas.
    school_info = school_info.rename(columns={
        'NO_ENTIDADE' : 'nome_escola',
        'CO_ENTIDADE' : 'id_escola',
        'NO_MUNICIPIO' : 'nome_municipio',
        'CO_MUNICIPIO' : 'id_municipio',
        'NO_MESORREGIAO' : 'nome_mesorregiao',
        'CO_MESORREGIAO' : 'id_mesorregiao',
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

    path = os.path.join(dir_atual, "..", "data/Geral/school_info.csv")
    save_incremental(school_info, path)
    
    return school_info

def create_infrastructure(data, df_dict):

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

def get_acessible_rating(df_infra_wide, active_schools_ids):
    
    acessible_cols = [
        'QT_SALAS_UTILIZADAS', 'QT_SALAS_UTILIZADAS_ACESSIVEIS',
        'IN_BANHEIRO_PNE', 'IN_SALA_ATENDIMENTO_ESPECIAL',
        'IN_ACESSIBILIDADE_CORRIMAO', 'IN_ACESSIBILIDADE_PISOS_TATEIS',
        'IN_ACESSIBILIDADE_VAO_LIVRE', 'IN_ACESSIBILIDADE_RAMPAS',
        'IN_ACESSIBILIDADE_SINAL_TATIL'
    ]
    
    ratings_map = {}
    
    for school_id in active_schools_ids:
        if school_id not in df_infra_wide.index:
            continue

        school_data = df_infra_wide.loc[school_id]
        
        
        qnt_room = school_data.get(acessible_cols[0], 0)
        qnt_acessible_room = school_data.get(acessible_cols[1], 0)

        sum_acessibility = sum([school_data.get(col, 0) for col in acessible_cols[2:]])
        
        ratio_rooms = (qnt_acessible_room / qnt_room) if qnt_room > 0 else 0
        
        rating = round((ratio_rooms + sum_acessibility) / len(acessible_cols), 2)
        
        ratings_map[school_id] = rating
    
    return pd.Series(ratings_map)
    
def get_recreation_rating(df_infra_wide, active_schools_ids):
    recreation_cols = [
        'QT_SALAS_UTILIZADAS', 'QT_SALAS_UTILIZA_CLIMATIZADAS',
        'IN_TERREIRAO', 'IN_AREA_PLANTIO',
        'IN_PATIO_COBERTO', 'IN_PATIO_DESCOBERTO',
        'IN_PARQUE_INFANTIL', 'IN_PISCINA',
        'IN_QUADRA_ESPORTES', 'IN_TERREIRAO'
    ]
    
    ratings_map = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_infra_wide.index: 
            continue

        
        school_data = df_infra_wide.loc[school_id]
        
        qnt_room = school_data.get(recreation_cols[0], 0)
        qnt_air_conditioned_room = school_data.get(recreation_cols[1], 0)
        
        ration_room = (qnt_air_conditioned_room / qnt_room) if qnt_room > 0 else 0
        
        sum_recreation = sum([school_data.get(col, 0) for col in recreation_cols[2:]]) + ration_room
        
        rating = round(sum_recreation / len(recreation_cols), 2)
        
        ratings_map[school_id] = rating
    
    return pd.Series(ratings_map)

def get_wellbeing_rating(df_infra_wide, active_schools_ids):
    
    wellbeing_cols = [
        'IN_AGUA_POTAVEL', 'IN_ALIMENTACAO',
        'IN_COZINHA', 'IN_REFEITORIO', 
        'IN_ESGOTO_REDE_PUBLICA', 'IN_ENERGIA_REDE_PUBLICA',
        'IN_LIXO_SERVICO_COLETA'
    ]
    
    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_infra_wide.index: 
            continue


        school_data = df_infra_wide.loc[school_id]

        soma = sum([school_data.get(col, 0) for col in wellbeing_cols])
        
        rating = soma / len(wellbeing_cols)
        
        rating_maps[school_id] = rating.round(2)

    return pd.Series(rating_maps)
        
def get_human_support_rating(df_enroll_wide, active_schools_ids):
    support_staff_cols = [
        'QT_PROF_PSICOLOGO', 'QT_PROF_ASSIST_SOCIAL',
        'QT_PROF_FONAUDIOLOGO', 'QT_PROF_NUTRICIONISTA'
    ]
    
    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_enroll_wide.index: 
            continue


        school_data = df_enroll_wide.loc[school_id]

        soma = sum([school_data.get(col, 0) for col in support_staff_cols])
        
        rating = soma / len(support_staff_cols)
        
        rating_maps[school_id] = round(rating, 2)

    return pd.Series(rating_maps)

def get_management_rating(df_enroll_wide, active_schools_ids):
    management_cols = ['IN_ORGAO_ASS_PAIS', 'IN_ORGAO_CONSELHO_ESCOLAR', 'IN_ORGAO_GREMIO_ESTUDANTIL']
    
    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_enroll_wide.index: 
            continue


        school_data = df_enroll_wide.loc[school_id]

        soma = sum([school_data.get(col, 0) for col in management_cols])
        
        rating = soma / len(management_cols)
        
        rating_maps[school_id] = round(rating, 2)

    return pd.Series(rating_maps)

def get_age_grade_distortion(df_enroll_wide, active_schools_ids):
    distortion_cols = [
        'QT_MAT_BAS_15_17', 'QT_MAT_FUND_AF_6',
        'QT_MAT_FUND_AF_7', 'QT_MAT_FUND_AF_8',
        'QT_MAT_FUND_AF_9']
    
    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_enroll_wide.index: 
            continue


        school_data = df_enroll_wide.loc[school_id]
        
        total_15_17 = school_data.get(distortion_cols[0], 0)

        soma_distortion = sum([school_data.get(col, 0) for col in distortion_cols[1:]])

        if total_15_17 > 0:
            rating = soma_distortion / total_15_17
        else:
            rating = 0
        
        rating_maps[school_id] = round(rating, 2)

    return pd.Series(rating_maps)

def get_pedagogical_rating(df_infra_wide, active_schools_ids):
    pedagogical_cols = [
        'IN_BIBLIOTECA_SALA_LEITURA', 'IN_LABORATORIO_INFORMATICA',
        'IN_LABORATORIO_CIENCIAS', 'IN_BANDA_LARGA',
        'IN_INTERNET_ALUNOS', 'IN_MATERIAL_PED_JOGOS'
        ]
    
    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_infra_wide.index: 
            continue


        school_data = df_infra_wide.loc[school_id]

        soma = sum([school_data.get(col, 0) for col in pedagogical_cols])
        
        rating = soma / len(pedagogical_cols)
        
        rating_maps[school_id] = round(rating, 2)

    return pd.Series(rating_maps)

def save_incremental(df, filepath):
    file_exists = os.path.exists(filepath)

    df.to_csv(filepath, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')

def create_rating_table(df_infra_long, df_enroll_long, df_school_info, df_dict, year):

    df_active = df_school_info[df_school_info['funcionamento'] == 1].copy()
    
    df_school_ratings = pd.DataFrame(index=df_active['id_escola'])
    df_school_ratings['ano'] = year

    df_dict_infra = pd.read_csv(os.path.join(dir_atual, "..", "data/Infraestrutura/infrastructure_dict.csv"))
    df_dict_enroll = pd.read_csv(os.path.join(dir_atual, "..", "data/Matricula/enroll_dict.csv"))

    map_infra_names = dict(zip(df_dict_infra['id_atributo'], df_dict_infra['variavel']))

    df_infra_wide = df_infra_long.pivot(index='id_escola', columns='id_atributo', values='valor')
    df_infra_wide.columns = df_infra_wide.columns.map(map_infra_names)
    df_infra_wide = df_infra_wide.reindex(df_school_ratings.index).fillna(0)

    map_enroll_names = dict(zip(df_dict_enroll['id_atributo'], df_dict_enroll['variavel']))
    df_enroll_wide = df_enroll_long.pivot(index='id_escola', columns='id_atributo', values='valor')
    df_enroll_wide.columns = df_enroll_wide.columns.map(map_enroll_names)
    df_enroll_wide = df_enroll_wide.reindex(df_school_ratings.index).fillna(0)

    df_school_ratings['acessibility_rating'] = get_acessible_rating(df_infra_wide, df_school_ratings.index)
    df_school_ratings['recreation_rating'] = get_recreation_rating(df_infra_wide, df_school_ratings.index)
    df_school_ratings['wellbeing_rating'] = get_wellbeing_rating(df_infra_wide, df_school_ratings.index)
    df_school_ratings['human_support_rating'] = get_human_support_rating(df_enroll_wide, df_school_ratings.index)
    df_school_ratings['management_rating'] = get_management_rating(df_enroll_wide, df_school_ratings.index)
    df_school_ratings['age_grade_distortion_rating'] = get_age_grade_distortion(df_enroll_wide, df_school_ratings.index)
    df_school_ratings['pedagogical_rating'] = get_pedagogical_rating(df_infra_wide, df_school_ratings.index)

    path_ratings = os.path.join(dir_atual, "..", "data/Geral/school_ratings.csv")
    save_incremental(df_school_ratings.reset_index(), path_ratings)
    
    print(df_school_ratings.head(10))

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

    remove_files()

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
        create_rating_table(df_infra_long, df_enroll_long, df_info, df_dict, current_year)

        i += 1

    get_school_ideb_file()
