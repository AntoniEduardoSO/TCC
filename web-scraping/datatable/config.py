import csv
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

DB_HOST = 'localhost'
DB_NAME = 'arkhos'
DB_USER = 'postgres'
DB_PASSWORD = '311200'
DB_PORT = '5432'

STRING_CONEXAO = 'postgresql://postgres:311200@localhost:5432/arkhos'

def connect_to_db():
    return psycopg2.connect(
        host = DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def exec_school_rating(conn, cur):

    query = """
        INSERT INTO school_rating (
            id_escola_fk, ano, acessibility_rating, recreation_rating, 
            wellbeing_rating, human_support_rating, management_rating, 
            age_grade_distortion_rating, pedagogical_rating, teacher_stress_rating, 
            teacher_instability_rating, administrative_burden_rating, 
            spending_per_student, spending_per_teacher, pedagogical_spending_per_student, 
            infrastructure_spending_per_student, meal_spending_per_student, 
            transport_spending_per_student, approval_rate, failure_rate, 
            dropout_rate, ideb_rating, saeb_rating
        ) VALUES (
            %(id_escola)s, %(ano)s, %(acessibility_rating)s, %(recreation_rating)s,
            %(wellbeing_rating)s, %(human_support_rating)s, %(management_rating)s,
            %(age_grade_distortion_rating)s, %(pedagogical_rating)s, %(teacher_stress_rating)s,
            %(teacher_instability_rating)s, %(administrative_burden_rating)s,
            %(spending_per_student)s, %(spending_per_teacher)s, %(pedagogical_spending_per_student)s,
            %(infrastructure_spending_per_student)s, %(meal_spending_per_student)s,
            %(transport_spending_per_student)s, %(approval_rate)s, %(failure_rate)s,
            %(dropout_rate)s, %(ideb_rating)s, %(saeb_rating)s
        )
    """

    lote_dados = []

    with open('data/Geral/school_ratings.csv', 'r', encoding='utf-8') as file:
        data_reader = csv.DictReader(file)

        for row in data_reader:

            for key, value in row.items():
                if value is None or value.strip() == "":
                    row[key] = None

            if row.get('id_escola') is not None:
                row['id_escola'] = int(float(row['id_escola'].strip()))
                
            if row.get('ano') is not None:
                row['ano'] = int(float(row['ano'].strip()))

            lote_dados.append(row)

            if len(lote_dados) == 5000:
                cur.executemany(query, lote_dados)
                lote_dados = [] 

        if lote_dados:
            cur.executemany(query, lote_dados)
    
    conn.commit()

def exec_school_info( conn, cur):

    query = """
        INSERT INTO school_info (
            escola_id, nome_escola, id_municipio_fk,
            dependencia, funcionamento, sede,
            alocacao, ocupacao, ano,
            endereco, telefone
        ) VALUES (
            %(id_escola)s, %(nome_escola)s, %(municipio_id)s,
            %(dependencia)s, %(funcionamento)s, %(sede)s,
            %(alocacao)s, %(ocupacao)s, %(ano)s,
            %(endereco)s, %(telefone)s
        )
    """

    lote_dados = []

    with open('data/Geral/school_info.csv', 'r', encoding='utf-8') as file:
        data_reader = csv.DictReader(file)

        for row in data_reader:

            for key, value in row.items():
                if value is None or value.strip() == "":
                    row[key] = None

                elif isinstance(value, str) and value.endswith('.0'):
                    row[key] = value[:-2]

            if row.get('municipio_id') is not None:
                row['municipio_id'] = int(str(row['municipio_id']).strip())

            lote_dados.append(row)

            if len(lote_dados) == 5000:
                    cur.executemany(query, lote_dados)
                    lote_dados = [] # Limpa a lista para o próximo lote
        
        if lote_dados:
            cur.executemany(query, lote_dados)
    
    conn.commit()

def exec_city_info(conn, cur):
    cols = [
        'municipio_id', 'ano', 'nome_municipio',
        'id_mesorregiao','nome_mesorregiao', 'id_microrregiao',
        'nome_microrregiao'
    ]

    df_cities = pd.read_csv("data/Geral/school_info.csv", encoding='utf-8')
    df_pop_cities = pd.read_csv("data/pop_municipios.csv", encoding='utf-8')
    df_territory = pd.read_csv("data/area_territorial_municipios.csv", sep=';', encoding='utf-8')

    # Limpeza de caracteres '', ' ' e afins para evitar erro.
    df_cities['municipio_id'] = df_cities['municipio_id'].astype(str).str.strip()
    df_territory['municipio_id'] = df_territory['municipio_id'].astype(str).str.strip()
    df_pop_cities['municipio_id'] = df_pop_cities['municipio_id'].astype(str).str.strip()

    # Limpeza de tipos errados.
    df_cities['municipio_id'] = pd.to_numeric(df_cities['municipio_id'], errors='coerce')
    df_territory['municipio_id'] = pd.to_numeric(df_territory['municipio_id'], errors='coerce')
    df_pop_cities['municipio_id'] = pd.to_numeric(df_pop_cities['municipio_id'], errors='coerce')

    # Limpeza de dados.
    df_cities = df_cities.drop_duplicates(subset=['municipio_id', 'ano'])
    df_cities = df_cities[cols]

    # Map de municipio_id -> area, com isso colocar de maneira correta.
    map_areas = df_territory.set_index('municipio_id')['area']
    df_cities['area_territorial'] = df_cities['municipio_id'].map(map_areas)

    df_cities = df_cities.merge(
        df_pop_cities[['municipio_id', 'ano', 'pop']], # Pegamos so as colunas que importam
        on=['municipio_id', 'ano'],                    # As duas chaves de ligacao
        how='left'                                     # Mantem todas as cidades, mesmo sem populacao
    )
    
    df_cities = df_cities.rename(columns={'pop': 'populacao_total'})

    # Limpeza de erros.
    df_cities['populacao_total'] = pd.to_numeric(df_cities['populacao_total'], errors='coerce')
    df_cities['area_territorial'] = pd.to_numeric(df_cities['area_territorial'], errors='coerce')

    # Sim, essa densidade pode e deve ser maior no centro da cidade, mas uma boa margem ja esta feito, podemos colocar peso de acordo com o
    # total de pessoas pelo espaco central das escolas, mas esta maneira ja esta Ok
    df_cities['densidade_demografica'] = df_cities['populacao_total'] / df_cities['area_territorial']

    df_cities['densidade_demografica'] = df_cities['densidade_demografica'].round(2)

    query = """
        INSERT INTO city_info (
            municipio_id, ano, nome_municipio,
            id_mesorregiao, nome_mesorregiao,
            id_microrregiao, nome_microrregiao,
            area_territorial, populacao_total, densidade_demografica
        ) VALUES (
            %(municipio_id)s, %(ano)s, %(nome_municipio)s,
            %(id_mesorregiao)s, %(nome_mesorregiao)s,
            %(id_microrregiao)s, %(nome_microrregiao)s,
            %(area_territorial)s, %(populacao_total)s, %(densidade_demografica)s
        )
    """

    lote_dados = df_cities.to_dict('records')
    
    cur.executemany(query, lote_dados)

    conn.commit()

def exec_datatables():

    conn = connect_to_db()
    cur = conn.cursor()

    exec_city_info( conn, cur)
    exec_school_info( conn, cur)
    exec_school_rating(conn, cur)

    conn.commit()