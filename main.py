import pandas as pd
import numpy as np


# Colunas essenciais para o merge e novas tabelas.
col_identify = ['NU_ANO_CENSO', 'CO_ENTIDADE']
col_dict = ['id_atributo', 'variavel', 'descricao', 'tipo', 'tamanho', 'grupo']

def fix_dtypes(df_dict):
    
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

    # Carregando o csv com os tipos corretos
    data = pd.read_csv(
        "microdados.csv", 
        encoding='ISO-8859-1', # encoding latin-1
        sep=';', 
        low_memory=False, 
        dtype=school_schema
    ).query("SG_UF == 'AL' and TP_DEPENDENCIA < 4").copy()  # Filtrando pela regiao de AL e escola não privadas.

    
    
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
    data.loc[:, col_present] = data[col_present].replace(0, pd.NA)

    return data
    
def create_school_info(data, df_dict):

    col_adr = ['DS_ENDERECO', 'NU_ENDERECO', 'DS_COMPLEMENTO', 'NO_BAIRRO', 'CO_CEP']
    col_cellphone = ['NU_DDD', 'NU_TELEFONE']


    filtro_geral = df_dict[df_dict['area'] == 'GERAL']
    school_info = data[filtro_geral['variavel']].copy()

    # Trocando '' para 'NaN'
    for col in col_adr:
        school_info[col] = school_info[col].astype(str).replace('NaN', '').str.strip()

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


    school_info.to_csv('Geral/school_info.csv', index=False, encoding='utf-8-sig')
    print("arquivo school_info criado com sucesso na pasta ""Geral/school_info.csv"" ! ")

def create_infrastructure(data, df_dict):

    # Carregando o dicionario para infraestrutura.
    infra_dict_base = df_dict[df_dict['area'] == 'INFRAESTRUTURA'].copy()

    # Criando uma coluna para ter um id unico para cada coluna.
    infra_dict_base['id_atributo'] = range(1,len(infra_dict_base) + 1)

    # Organizando o csv dicionario de acordo com as colunas necessarias.

    infra_metadata = infra_dict_base[col_dict]
    infra_metadata.to_csv('Infraestrutura/infrastructure_dict.csv', index=False, encoding='utf-8-sig')


    vars_infra = infra_dict_base['variavel'].tolist()


    final_infra = data[col_identify + vars_infra].melt(
        id_vars=col_identify, 
        value_vars=vars_infra,
        var_name='variavel', 
        value_name='valor'
    ).dropna(subset=['valor']) # Removendo as linhas marcadas por nulo (funcao optimized_tables)

    # Fazendo o merge de colunas para linhas.

    final_infra = final_infra.merge(infra_metadata[['id_atributo', 'variavel', 'tipo']], on='variavel')

    # Alterando os nomes das colunas.
    final_infra = final_infra.rename(columns={'NU_ANO_CENSO': 'ano', 'CO_ENTIDADE': 'id_escola', 'tipo': 'tipo_atributo'})

    # Criando Id unico
    final_infra['id'] = range(1, len(final_infra) + 1)

    # Criando arquivo.
    final_infra[['id', 'ano', 'id_escola', 'id_atributo', 'tipo_atributo', 'valor']].to_csv(
        'Infraestrutura/infrastructure_values.csv', index=False, encoding='utf-8-sig'
    )
    
    print("Arquivos de infraestrutura criados com sucesso! e adicionado na pasta de infraestrutura.")

def create_school_enrollment(data, df_dict):

    # Repeteco da funcao infraestructure, leia ele primeiro.

    enroll_dict_base = df_dict[df_dict['area'] == 'MATRICULA'].copy()

    enroll_dict_base['id_atributo'] = range(1,len(enroll_dict_base) + 1)

    enroll_metadata = enroll_dict_base[col_dict]
    enroll_metadata.to_csv('Matricula/enroll_dict.csv', index=False, encoding='utf-8-sig')


    vars_enroll = enroll_dict_base['variavel'].tolist()


    final_enroll = data[col_identify + vars_enroll].melt(
        id_vars=col_identify, 
        value_vars=vars_enroll,
        var_name='variavel', 
        value_name='valor'
    ).dropna(subset=['valor'])

    final_enroll = final_enroll.merge(enroll_metadata[['id_atributo', 'variavel', 'tipo']], on='variavel')

    final_enroll = final_enroll.rename(columns={'NU_ANO_CENSO': 'ano', 'CO_ENTIDADE': 'id_escola', 'tipo': 'tipo_atributo'})
   
    final_enroll['id'] = range(1, len(final_enroll) + 1)

    final_enroll[['id', 'ano', 'id_escola', 'id_atributo', 'tipo_atributo', 'valor']].to_csv(
        'Matricula/enroll_values.csv', index=False, encoding='utf-8-sig'
    )
    
    print("Arquivos de matricula criados com sucesso! e adicionado na pasta de matricula.")

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
        school_data = df_infra_wide.loc[school_id]
        
        sum_acessibility = school_data[acessible_cols[2:]].sum()
        qnt_room = school_data[acessible_cols[0]]
        qnt_acessible_room = school_data[acessible_cols[1]]
        
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
        school_data = df_infra_wide.loc[school_id]
        
        qnt_room = school_data[recreation_cols[0]]
        qnt_air_conditioned_room = school_data[recreation_cols[1]]
        
        ration_room = qnt_air_conditioned_room/qnt_room
        
        sum_recreation = school_data[recreation_cols[2:]].sum() + ration_room
        
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
        school_data = df_infra_wide.loc[school_id]
        
        rating = school_data[wellbeing_cols].sum() / len(wellbeing_cols)
        
        rating_maps[school_id] = rating.round(2)

    return pd.Series(rating_maps)
        
def get_human_support_rating(df_enroll_wide, active_schools_ids):
    support_staff_cols = [
        'QT_PROF_PSICOLOGO', 'QT_PROF_ASSIST_SOCIAL',
        'QT_PROF_FONAUDIOLOGO', 'QT_PROF_NUTRICIONISTA'
    ]
    
    rating_maps = {}
    
    for school_id in active_schools_ids:
        school_data = df_enroll_wide.loc[school_id]
        
        rating = school_data[support_staff_cols].sum() / len(support_staff_cols)
        
        rating_maps[school_id] = rating.round(2)

    return pd.Series(rating_maps)

def get_management_rating(df_enroll_wide, active_schools_ids):
    management_cols = ['IN_ORGAO_ASS_PAIS', 'IN_ORGAO_CONSELHO_ESCOLAR', 'IN_ORGAO_GREMIO_ESTUDANTIL']
    
    rating_maps = {}
    
    for school_id in active_schools_ids:
        school_data = df_enroll_wide.loc[school_id]
        
        rating = school_data[management_cols].sum() / len(management_cols)
        
        rating_maps[school_id] = rating.round(2)

    return pd.Series(rating_maps)

def get_age_grade_distortion(df_enroll_wide, active_schools_ids):
    distortion_cols = [
        'QT_MAT_BAS_15_17', 'QT_MAT_FUND_AF_6',
        'QT_MAT_FUND_AF_7', 'QT_MAT_FUND_AF_8',
        'QT_MAT_FUND_AF_9']
    
    rating_maps = {}
    
    for school_id in active_schools_ids:
        school_data = df_enroll_wide.loc[school_id]
        
        total_15_17 = school_data[distortion_cols[0]]
        
        rating = school_data[distortion_cols[1:]].sum() / total_15_17
        
        rating_maps[school_id] = rating.round(2)

    return pd.Series(rating_maps)

def get_pedagogical_rating(df_infra_wide, active_schools_ids):
    pedagogical_cols = [
        'IN_BIBLIOTECA_SALA_LEITURA', 'IN_LABORATORIO_INFORMATICA',
        'IN_LABORATORIO_CIENCIAS', 'IN_BANDA_LARGA',
        'IN_INTERNET_ALUNOS', 'IN_MATERIAL_PED_JOGOS'
        ]
    
    rating_maps = {}
    
    for school_id in active_schools_ids:
        school_data = df_infra_wide.loc[school_id]
        
        rating = school_data[pedagogical_cols].sum() / len(pedagogical_cols)
        
        rating_maps[school_id] = rating.round(2)

    return pd.Series(rating_maps)


def create_rating_table(data, df_dict):
    df_infra = pd.read_csv("Infraestrutura/infrastructure_values.csv")
    df_dict_infra = pd.read_csv("Infraestrutura/infrastructure_dict.csv")

    df_enroll = pd.read_csv("Matricula/enroll_values.csv")
    df_dict_enroll = pd.read_csv("Matricula/enroll_dict.csv")

    df_school_info = pd.read_csv("Geral/school_info.csv")
    
    df_active = df_school_info[df_school_info['funcionamento'] == 1].copy()
    
    df_school_ratings = pd.DataFrame(index=df_active['id_escola'])
    
    df_school_ratings = pd.DataFrame(index=df_active['id_escola'])
    df_school_ratings['ano'] = 2024
    
    map_infra_names = dict(zip(df_dict_infra['id_atributo'], df_dict_infra['variavel']))
    df_infra_wide = df_infra.pivot(index='id_escola', columns='id_atributo', values='valor')
    df_infra_wide.columns = df_infra_wide.columns.map(map_infra_names)
        
    df_infra_wide = df_infra_wide.reindex(df_school_ratings.index).fillna(0)
    
    map_enroll_names = dict(zip(df_dict_enroll['id_atributo'], df_dict_enroll['variavel']))
    df_enroll_wide = df_enroll.pivot(index='id_escola', columns='id_atributo', values='valor')
    df_enroll_wide.columns = df_enroll_wide.columns.map(map_enroll_names)
    
    df_enroll_wide = df_enroll_wide.reindex(df_school_ratings.index).fillna(0)
    
    df_school_ratings['acessibility_rating'] = get_acessible_rating(df_infra_wide, df_school_ratings.index)
    
    df_school_ratings['recreation_rating'] = get_recreation_rating(df_infra_wide, df_school_ratings.index)
    
    df_school_ratings['wellbeing_rating'] = get_wellbeing_rating(df_infra_wide, df_school_ratings.index)
    
    df_school_ratings['human_support_rating'] = get_human_support_rating(df_enroll_wide, df_school_ratings.index)
    
    df_school_ratings['management_rating'] = get_management_rating(df_enroll_wide, df_school_ratings.index)
    
    df_school_ratings['age_grade_distortion_rating'] = get_age_grade_distortion(df_enroll_wide, df_school_ratings.index)
    
    df_school_ratings['pedagogical_rating'] = get_pedagogical_rating(df_infra_wide, df_school_ratings.index)
    
    
    print(df_school_ratings.head(10))
    


def main():

    # Carregando o dicionario.
    df_dict = pd.read_csv("dicionario.csv")

    # Corrigindo os tipos de valores atraves do dicionario.csv e retornando o data limpo.
    data = fix_dtypes(df_dict)

    # Adicionando etiquetas para colunas quantitativas iguais a 0, evitando muitas linhas desnecessarias.
    data = generate_optimized_tables(data, df_dict)

    # Criando csv para school_info.csv
    # create_school_info(data, df_dict)

    # Criando csv para infrastructure e dict_infraestructure
    # create_infrastructure(data, df_dict)

    # Criando csv para enrollment e dict_enrollment
    # create_school_enrollment(data, df_dict)``


    # Criando csv para tabelas de rating.
    create_rating_table(data, df_dict)


if __name__ == "__main__":
    main()