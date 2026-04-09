# -*- coding: utf-8 -*-

import csv
import pandas as pd
from sqlalchemy import create_engine

import sqlite3


DB_NAME = '../arkhos.db'

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

def connect_to_db():
    return sqlite3.connect(DB_NAME)

def create_sqlite_indexes(conn, cur):
    
    queries_index = [
        "CREATE INDEX IF NOT EXISTS idx_slite_enroll_optim ON school_enroll_values(ano, id_atributo, id_escola_fk, valor);",
        "CREATE INDEX IF NOT EXISTS idx_slite_infra_optim ON school_infra_values(ano, id_atributo, id_escola_fk, valor);",
        "CREATE INDEX IF NOT EXISTS idx_slite_sinfo_optim ON school_info(escola_id, ano, funcionamento, id_municipio_fk);",
        "CREATE INDEX IF NOT EXISTS idx_slite_cinfo_optim ON city_info(municipio_id, ano, id_mesorregiao, id_microrregiao);",
        "CREATE INDEX IF NOT EXISTS idx_slite_transp_mun_fk ON city_transparency_portal(municipio_id_fk);"
    ]

    for query in queries_index:
        cur.execute(query)
    
    conn.commit()
    


def exec_transparency_portal(conn, cur):

    cur.execute("""
        CREATE TABLE IF NOT EXISTS city_transparency_portal (
            id TEXT PRIMARY KEY,
            municipio_id_fk INTEGER,
            data DATE, 
            valor DOUBLE PRECISION,
            credor TEXT,
            elemento_despesa TEXT,
            detalhe TEXT,
            eixo TEXT,
            macro TEXT,
            micro TEXT,
            portal_origem INTEGER
        );
    """)
    conn.commit()

    chunk_size = 50000
    total_inseridos = 0
    
    for df_chunk in pd.read_csv('data/CONSOLIDADO_GERAL_FINAL.csv', encoding='utf-8-sig', chunksize=chunk_size):
        
        df_chunk['data'] = pd.to_datetime(df_chunk['data'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        df_chunk = df_chunk.where(pd.notnull(df_chunk), None)

        if 'municipio_nome' in df_chunk.columns:
            df_chunk = df_chunk.drop(columns=['municipio_nome'])


        colunas_ordem = [
            'id', 'municipio_id', 'data', 'valor', 'credor', 
            'elemento_despesa', 'detalhe', 'eixo', 'macro', 'micro', 'portal_origem'
        ]
        df_chunk = df_chunk[colunas_ordem]
        
        data_tuples = list(df_chunk.itertuples(index=False, name=None))

        query = """
            INSERT OR IGNORE INTO city_transparency_portal (
                id, municipio_id_fk, data, valor, credor,
                elemento_despesa, detalhe, eixo, macro, micro, portal_origem
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cur.executemany(query, data_tuples)
        
        total_inseridos += len(data_tuples)

    conn.commit()


def exec_school_infra_values(conn, cur):

    cur.execute("""
        CREATE TABLE IF NOT EXISTS school_infra_values (
            ano INTEGER,
            id_escola_fk INTEGER,
            id_atributo INTEGER,
            tipo_atributo TEXT,
            valor REAL,
            PRIMARY KEY (ano, id_escola_fk, id_atributo)
        );
    """)

    query = """
        INSERT OR IGNORE INTO school_infra_values (
            ano, id_escola_fk, id_atributo, tipo_atributo, valor
        ) VALUES (?, ?, ?, ?, ?)
    """

    chunk_size = 50000
    total_inseridos = 0

    for df_chunk in pd.read_csv('data/Infraestrutura/infrastructure_values.csv', encoding='utf-8-sig', chunksize=chunk_size):

        df_chunk['valor'] = pd.to_numeric(df_chunk['valor'].astype(str).str.replace(',', '.'), errors='coerce')

        df_chunk['ano'] = pd.to_numeric(df_chunk['ano'], errors='coerce')
        df_chunk['id_escola'] = pd.to_numeric(df_chunk['id_escola'], errors='coerce')
        df_chunk['id_atributo'] = pd.to_numeric(df_chunk['id_atributo'], errors='coerce')
        
        df_chunk = df_chunk.dropna(subset=['ano', 'id_escola', 'id_atributo'])

        df_chunk = df_chunk.astype(object).where(pd.notnull(df_chunk), None)

        df_chunk = df_chunk[['ano', 'id_escola', 'id_atributo', 'tipo_atributo', 'valor']]

        data = list(df_chunk.itertuples(index=False, name=None))
        cur.executemany(query, data)

        total_inseridos += len(data)

    conn.commit()

def exec_school_infra_dict(conn, cur):

    cur.execute("""
        CREATE TABLE IF NOT EXISTS school_infra_dict (
            id INTEGER PRIMARY KEY,
            variavel TEXT,
            descricao TEXT,
            tipo TEXT,
            tamanho INTEGER,
            grupo TEXT
        );
    """)

    query = """
        INSERT OR IGNORE INTO school_infra_dict (
            id, variavel, descricao, tipo, tamanho, grupo
        ) VALUES (
            :id_atributo, :variavel, :descricao, :tipo, :tamanho, :grupo
        )
    """

    lote_dados = []

    with open('data/Infraestrutura/infrastructure_dict.csv', 'r', encoding='utf-8-sig') as file:
        data_reader = csv.DictReader(file)

        for row in data_reader:

            for key, value in row.items():
                if value is None or str(value).strip() == "":
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS school_enroll_dict (
            id INTEGER PRIMARY KEY,
            variavel TEXT,
            descricao TEXT,
            tipo TEXT,
            tamanho INTEGER,
            grupo TEXT
        );
    """)

    query = """
        INSERT OR IGNORE INTO school_enroll_dict (
            id, variavel, descricao, tipo, tamanho, grupo
        ) VALUES (
            :id_atributo, :variavel, :descricao, :tipo, :tamanho, :grupo
        )
    """

    lote_dados = []

    with open('data/Matricula/enroll_dict.csv', 'r', encoding='utf-8-sig') as file:
        data_reader = csv.DictReader(file)

        for row in data_reader:

            for key, value in row.items():
                if value is None or str(value).strip() == "":
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS school_enroll_values (
            ano INTEGER,
            id_escola_fk INTEGER,
            id_atributo INTEGER,
            tipo_atributo TEXT,
            valor REAL,
            PRIMARY KEY (ano, id_escola_fk, id_atributo)
        );
    """)

    query = """
        INSERT OR IGNORE INTO school_enroll_values (
            ano, id_escola_fk, id_atributo, tipo_atributo, valor
        ) VALUES (?, ?, ?, ?, ?)
    """

    chunk_size = 50000
    total_inseridos = 0

    for df_chunk in pd.read_csv('data/Matricula/enroll_values.csv', encoding='utf-8-sig', chunksize=chunk_size):

        df_chunk['valor'] = pd.to_numeric(df_chunk['valor'].astype(str).str.replace(',', '.'), errors='coerce')

        df_chunk['id_escola'] = pd.to_numeric(df_chunk['id_escola'], errors='coerce')
        df_chunk['ano'] = pd.to_numeric(df_chunk['ano'], errors='coerce')
        df_chunk['id_atributo'] = pd.to_numeric(df_chunk['id_atributo'], errors='coerce')

        df_chunk = df_chunk.dropna(subset=['ano', 'id_escola', 'id_atributo'])
        
        df_chunk = df_chunk.astype(object).where(pd.notnull(df_chunk), None)

        df_chunk = df_chunk[['ano', 'id_escola', 'id_atributo', 'tipo_atributo', 'valor']]

        data = list(df_chunk.itertuples(index=False, name=None))

        cur.executemany(query, data)
        
        total_inseridos += len(data)

    conn.commit()

def exec_school_rating(conn, cur):

    cur.execute("""
        CREATE TABLE IF NOT EXISTS school_rating (
            id_escola_fk INTEGER,
            ano INTEGER,
            acessibility_rating REAL,
            recreation_rating REAL,
            wellbeing_rating REAL,
            human_support_rating REAL,
            management_rating REAL,
            age_grade_distortion_rating REAL,
            pedagogical_rating REAL,
            teacher_stress_rating REAL,
            teacher_instability_rating REAL,
            administrative_burden_rating REAL,
            spending_per_student REAL,
            spending_per_teacher REAL,
            pedagogical_spending_per_student REAL,
            infrastructure_spending_per_student REAL,
            meal_spending_per_student REAL,
            transport_spending_per_student REAL,
            approval_rate REAL,
            failure_rate REAL,
            dropout_rate REAL,
            ideb_rating REAL,
            saeb_rating REAL,
            PRIMARY KEY (id_escola_fk, ano)
        );
    """)

    query = """
        INSERT OR IGNORE INTO school_rating (
            id_escola_fk, ano, acessibility_rating, recreation_rating, 
            wellbeing_rating, human_support_rating, management_rating, 
            age_grade_distortion_rating, pedagogical_rating, teacher_stress_rating, 
            teacher_instability_rating, administrative_burden_rating, 
            spending_per_student, spending_per_teacher, pedagogical_spending_per_student, 
            infrastructure_spending_per_student, meal_spending_per_student, 
            transport_spending_per_student, approval_rate, failure_rate, 
            dropout_rate, ideb_rating, saeb_rating
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?
        )
    """

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
                cur.executemany(query, lote)
                lote.clear()

        if lote:
            cur.executemany(query, lote)

    conn.commit()

def exec_school_info( conn, cur):

    cur.execute("""
        CREATE TABLE IF NOT EXISTS school_info (
            escola_id INTEGER,
            nome_escola TEXT,
            id_municipio_fk INTEGER,
            dependencia INTEGER,
            funcionamento INTEGER,
            sede INTEGER,
            alocacao INTEGER,
            ocupacao INTEGER,
            ano INTEGER,
            endereco TEXT,
            telefone TEXT,
            lat REAL,
            lon REAL,
            PRIMARY KEY (escola_id, ano)
        );
    """)

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
            :id_escola, :nome_escola, :municipio_id,
            :dependencia, :funcionamento, :sede,
            :alocacao, :ocupacao, :ano,
            :endereco, :telefone,
            :lat, :lon
        )
    """

    lote_dados = []

    with open('data/Geral/school_info.csv', 'r', encoding='utf-8-sig') as file:
        data_reader = csv.DictReader(file)

        for row in data_reader:


            for key, value in row.items():
                if value is None or str(value).strip() == "":
                    row[key] = None
                elif isinstance(value, str) and value.endswith('.0'):
                    row[key] = value[:-2]


            if row.get('municipio_id') is not None:
                row['municipio_id'] = int(str(row['municipio_id']).strip())
            
            if row.get('ano') is not None:
                row['ano'] = int(str(row['ano']).strip())

            try:
                id_escola = int(row["id_escola"])
                row["id_escola"] = id_escola 

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
                lote_dados = [] 

        if lote_dados:
            cur.executemany(query, lote_dados)
    
    conn.commit()

def exec_city_info(conn, cur):

    cur.execute("""
        CREATE TABLE IF NOT EXISTS city_info (
            municipio_id INTEGER,
            ano INTEGER,
            nome_municipio TEXT,
            id_mesorregiao INTEGER,
            nome_mesorregiao TEXT,
            id_microrregiao INTEGER,
            nome_microrregiao TEXT,
            area_territorial REAL,
            populacao_total REAL,
            densidade_demografica REAL,
            PRIMARY KEY (municipio_id, ano)
        );
    """)

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

    df_cities = df_cities.where(pd.notnull(df_cities), None)

    colunas_ordem = [
        'municipio_id', 'ano', 'nome_municipio',
        'id_mesorregiao', 'nome_mesorregiao',
        'id_microrregiao', 'nome_microrregiao',
        'area_territorial', 'populacao_total', 'densidade_demografica'
    ]

    data_tuples = list(df_cities[colunas_ordem].itertuples(index=False, name=None))

    query = """
        INSERT OR IGNORE INTO city_info (
            municipio_id, ano, nome_municipio,
            id_mesorregiao, nome_mesorregiao,
            id_microrregiao, nome_microrregiao,
            area_territorial, populacao_total, densidade_demografica
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cur.executemany(query, data_tuples)
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
    exec_transparency_portal(conn,cur)

    create_sqlite_indexes(conn,cur)

    conn.commit()