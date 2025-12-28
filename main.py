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


def create_rating_table(data, df_dict):
    df_infra = pd.read_csv("Infraestrutura/infrastructure_values.csv")
    df_dict_infra = pd.read_csv("Infraestrutura/infrastructure_dict.csv")

    df_enroll = pd.read_csv("Matricula/enroll_values.csv")
    df_dict_enroll = pd.read_csv("Matricula/enroll_dict.csv")

    df_school_info = pd.read_csv("Geral/school_info.csv")
    

    while(True):
        
        coluna1 = df_dict_infra.loc[df_dict_infra['variavel'] == 'QT_SALAS_UTILIZADAS'].iloc[0]
        coluna2 = df_dict_infra.loc[df_dict_infra['variavel'] == 'QT_SALAS_UTILIZADAS_ACESSIVEIS'].iloc[0]
        escola_aleatoria = df_school_info.sample(1).iloc[0]

        qnt_room = df_infra.loc[ (df_infra['id_escola'] == escola_aleatoria['id_escola']) & (df_infra['id_atributo'] == coluna1['id_atributo']), 'valor' ]
        qnt_acessible_room = df_infra.loc[ (df_infra['id_escola'] == escola_aleatoria['id_escola']) & (df_infra['id_atributo'] == coluna2['id_atributo']), 'valor']

    
        print(f"Quantidade de salas utilizadas {qnt_room.item()}, acessiveis: {qnt_acessible_room.item()}")
        break

        # resultado = qnt_acessible_room / qnt_room



        # if not resultado.empty:
        #     valor_final = resultado['valor']
        #     print(f"Escola: {escola_aleatoria['nome_escola']}")
        #     print(f"Variável1 : {coluna1['variavel']}")
        #     print(f"Variável2  : {coluna2['variavel']}")
        #     print(f"Valor Encontrado: {valor_final}")
        #     print(valor_final)
        #     break



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
    # create_school_enrollment(data, df_dict)


    # Criando csv para tabelas de rating.
    create_rating_table(data, df_dict)


if __name__ == "__main__":
    main()