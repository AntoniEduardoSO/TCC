# -*- coding: utf-8 -*-

import csv
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import execute_values

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

def exec_school_infra_values(conn, cur):

    cur.execute("""
        CREATE TABLE IF NOT EXISTS school_infra_values_staging (
            id TEXT,
            ano TEXT,
            id_escola TEXT,
            id_atributo TEXT,
            tipo_atributo TEXT,
            valor TEXT
            );
    """)

    cur.execute("TRUNCATE school_infra_values_staging")

    with open('data/Infraestrutura/infrastructure_values.csv', 'r', encoding='utf-8-sig') as f:
        cur.copy_expert("""
            COPY school_infra_values_staging
            FROM STDIN
            WITH CSV HEADER
        """, f)

    cur.execute("""
        INSERT INTO school_infra_values (
            ano,
            id_escola_fk,
            id_atributo,
            tipo_atributo,
            valor
        )
        SELECT
            NULLIF(TRIM(ano), '')::INT,
            NULLIF(TRIM(id_escola), '')::INT,
            NULLIF(TRIM(id_atributo), '')::INT,
            NULLIF(TRIM(tipo_atributo), ''),
            NULLIF(TRIM(valor), '')::FLOAT
        FROM school_infra_values_staging
        ON CONFLICT DO NOTHING;
    """)

    cur.execute("TRUNCATE school_infra_values_staging")

    conn.commit()

def exec_school_infra_dict(conn, cur):

    query = """
        INSERT INTO school_infra_dict (
            id, variavel, descricao, tipo, tamanho, grupo
        ) VALUES (
            %(id_atributo)s, %(variavel)s, %(descricao)s, %(tipo)s, %(tamanho)s, %(grupo)s
        )
    """

    lote_dados = []

    with open('data/Infraestrutura/infrastructure_dict.csv', 'r', encoding='utf-8-sig') as file:
        data_reader = csv.DictReader(file)

        for row in data_reader:

            for key, value in row.items():
                if value is None or value.strip() == "":
                    row[key] = None

            if row.get('id_atributo') is not None:
                row['id_atributo'] = int(float(row['id_atributo'].strip()))
                
            if row.get('tamanho') is not None:
                row['tamanho'] = int(float(row['tamanho'].strip()))

            lote_dados.append(row)

            if len(lote_dados) == 5000:
                cur.executemany(query, lote_dados)
                lote_dados = [] 

        if lote_dados:
            cur.executemany(query, lote_dados)
    
    conn.commit()

def exec_school_enroll_dict(conn,cur):
    query = """
        INSERT INTO school_enroll_dict (
            id, variavel, descricao, tipo, tamanho, grupo
        ) VALUES (
            %(id_atributo)s, %(variavel)s, %(descricao)s, %(tipo)s, %(tamanho)s, %(grupo)s
        )
    """

    lote_dados = []

    with open('data/Matricula/enroll_dict.csv', 'r', encoding='utf-8-sig') as file:
        data_reader = csv.DictReader(file)

        for row in data_reader:

            for key, value in row.items():
                if value is None or value.strip() == "":
                    row[key] = None

            if row.get('id_atributo') is not None:
                row['id_atributo'] = int(float(row['id_atributo'].strip()))
                
            if row.get('tamanho') is not None:
                row['tamanho'] = int(float(row['tamanho'].strip()))

            lote_dados.append(row)

            if len(lote_dados) == 5000:
                cur.executemany(query, lote_dados)
                lote_dados = [] 

        if lote_dados:
            cur.executemany(query, lote_dados)
    
    conn.commit()

def exec_school_enroll_values(conn, cur):

    df = pd.read_csv('data/Matricula/enroll_values.csv', encoding='utf-8-sig')

    df = df.where(pd.notnull(df), None)

    df['id_escola'] = pd.to_numeric(df['id_escola'], errors='coerce')
    df['ano'] = pd.to_numeric(df['ano'], errors='coerce')
    df['id_atributo'] = pd.to_numeric(df['id_atributo'], errors='coerce')
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    df = df[['ano', 'id_escola', 'id_atributo', 'tipo_atributo', 'valor']]

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO school_enroll_values (
            ano, id_escola_fk, id_atributo, tipo_atributo, valor
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """

    execute_values(cur, query, data, page_size=10000)

    conn.commit()

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
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """

    def to_int(val):
        try:
            return int(float(val)) if val and val.strip() != "" else None
        except:
            return None

    def to_float(val):
        try:
            return float(val) if val and val.strip() != "" else None
        except:
            return None

    lote = []

    with open('data/Geral/school_ratings.csv', 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)

        for row in reader:

            parsed = (
                to_int(row.get('id_escola')),
                to_int(row.get('ano')),
                to_float(row.get('acessibility_rating')),
                to_float(row.get('recreation_rating')),
                to_float(row.get('wellbeing_rating')),
                to_float(row.get('human_support_rating')),
                to_float(row.get('management_rating')),
                to_float(row.get('age_grade_distortion_rating')),
                to_float(row.get('pedagogical_rating')),
                to_float(row.get('teacher_stress_rating')),
                to_float(row.get('teacher_instability_rating')),
                to_float(row.get('administrative_burden_rating')),
                to_float(row.get('spending_per_student')),
                to_float(row.get('spending_per_teacher')),
                to_float(row.get('pedagogical_spending_per_student')),
                to_float(row.get('infrastructure_spending_per_student')),
                to_float(row.get('meal_spending_per_student')),
                to_float(row.get('transport_spending_per_student')),
                to_float(row.get('approval_rate')),
                to_float(row.get('failure_rate')),
                to_float(row.get('dropout_rate')),
                to_float(row.get('ideb_rating')),
                to_float(row.get('saeb_rating')),
            )

            lote.append(parsed)

            if len(lote) >= 10000:
                execute_values(cur, query, lote)
                lote.clear()

        if lote:
            execute_values(cur, query, lote)

    conn.commit()

def exec_school_info( conn, cur):

    coords = {}

    with open('data/localizacao_escolas.csv', 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for r in reader:
            try:
                coords[int(r["id_escola"])] = {
                    "lat": float(r["lat"]) if r["lat"] else None,
                    "lon": float(r["lon"]) if r["lon"] else None
                }
            except:
                continue

    query = """
        INSERT INTO school_info (
            escola_id, nome_escola, id_municipio_fk,
            dependencia, funcionamento, sede,
            alocacao, ocupacao, ano,
            endereco, telefone,
            lat, lon
        ) VALUES (
            %(id_escola)s, %(nome_escola)s, %(municipio_id)s,
            %(dependencia)s, %(funcionamento)s, %(sede)s,
            %(alocacao)s, %(ocupacao)s, %(ano)s,
            %(endereco)s, %(telefone)s,
            %(lat)s, %(lon)s
        )
    """

    lote_dados = []

    with open('data/Geral/school_info.csv', 'r', encoding='utf-8-sig') as file:
        data_reader = csv.DictReader(file)

        for row in data_reader:

            for key, value in row.items():
                if value is None or value.strip() == "":
                    row[key] = None

                elif isinstance(value, str) and value.endswith('.0'):
                    row[key] = value[:-2]

            if row.get('municipio_id') is not None:
                row['municipio_id'] = int(str(row['municipio_id']).strip())

            try:
                id_escola = int(row["id_escola"])

                if id_escola in coords:
                    row["lat"] = coords[id_escola]["lat"]
                    row["lon"] = coords[id_escola]["lon"]
                else:
                    row["lat"] = None
                    row["lon"] = None
            except:
                row["lat"] = None
                row["lon"] = None

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

    df_cities = pd.read_csv("data/Geral/school_info.csv", encoding='utf-8-sig')
    df_pop_cities = pd.read_csv("data/pop_municipios.csv", encoding='utf-8-sig')
    df_territory = pd.read_csv("data/area_territorial_municipios.csv", sep=';', encoding='utf-8-sig')

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

    # Padronizar os anos em que o id_mesorregiao e micro tem valores de 27001, virar o padrao mais recente (1)
    df_cities['id_mesorregiao'] = df_cities['id_mesorregiao'].apply(lambda x: int(x) % 100 if pd.notnull(x) and int(x) > 100 else x)
    df_cities['id_microrregiao'] = df_cities['id_microrregiao'].apply(lambda x: int(x) % 1000 if pd.notnull(x) and int(x) > 1000 else x)

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
        ON CONFLICT (municipio_id, ano) DO NOTHING
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
    exec_school_enroll_dict(conn, cur)
    exec_school_enroll_values(conn, cur)
    exec_school_infra_dict(conn,cur)
    exec_school_infra_values(conn,cur)

    conn.commit()