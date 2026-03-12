import pandas as pd
import os

import pandas as pd
import os

DATASETS = [

    # MUNICIPIOS
    {
        "table": "municipios",
        "path": "data/Geral/school_info.csv",
        "columns": [
            "municipio_id",
            "nome_municipio",
            "nome_mesorregiao",
            "id_mesorregiao",
            "nome_microrregiao",
            "id_microrregiao"
        ],
        "deduplicate": "municipio_id"
    },

    # ESCOLAS
    {
        "table": "schools",
        "path": "data/Geral/school_info.csv",
        "rename": {
            "id_escola": "school_id"
        },
        "columns": [
            "school_id",
            "municipio_id",
            "nome_escola",
            "dependencia",
            "localizacao",
            "funcionamento",
            "sede",
            "alocacao",
            "ocupacao",
            "ano",
            "endereco",
            "telefone"
        ]
    },

    # RATINGS GERADOS
    {
        "table": "school_ratings",
        "path": "data/Geral/school_ratings.csv",
        "rename": {
            "id_escola": "school_id"
        }
    },

    # INFRAESTRUTURA
    {
        "table": "infrastructure_attributes",
        "path": "data/Infraestrutura/infrastructure_dict.csv"
    },

    {
        "table": "infrastructure_values",
        "path": "data/Infraestrutura/infrastructure_values.csv",
        "rename": {
            "id_escola": "school_id"
        }
    },

    # MATRICULA
    {
        "table": "enroll_attributes",
        "path": "data/Matricula/enroll_dict.csv"
    },

    {
        "table": "enroll_values",
        "path": "data/Matricula/enroll_values.csv",
        "rename": {
            "id_escola": "school_id"
        }
    },

    # IDEB
    {
        "table": "school_ideb",
        "path": "data/Matricula/school_ideb.csv"
    },

    # PERFORMANCE
    {
        "table": "school_performance_rate",
        "path": "data/Matricula/school_perfomance_rate.csv"
    },

    # SAEB
    {
        "table": "school_saeb",
        "path": "data/Matricula/school_saeb.csv"
    },

    # TRANSPARENCIA
    {
        "table": "transparencia_despesa",
        "path": "data/CONSOLIDADO_GERAL_FINAL.csv",
        "drop_columns": ["municipio_nome"],
        "parse_dates": ["data"]
    }

]

OUTPUT_SQL = "database.sql"

def sql_value(v):

    if pd.isna(v):
        return "NULL"

    if isinstance(v, str):
        v = v.replace("'", "''")
        return f"'{v}'"

    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v)

    return str(v)


def generate_insert(table, df, batch_size=1000):

    columns = ",".join(df.columns)
    inserts = []

    for i in range(0, len(df), batch_size):

        batch = df.iloc[i:i+batch_size]

        values_rows = []

        for row in batch.itertuples(index=False):

            values = ",".join(sql_value(v) for v in row)

            values_rows.append(f"({values})")

        values_sql = ",\n".join(values_rows)

        inserts.append(
            f"INSERT INTO {table} ({columns}) VALUES\n{values_sql};"
        )

    return inserts

def load_dataset(config):

    path = config["path"]

    print("Processing:", path)

    inserts = []

    for chunk in pd.read_csv(path, encoding="utf-8", low_memory=False, chunksize=50000):

        # rename columns
        if "rename" in config:
            chunk = chunk.rename(columns=config["rename"])

        # drop columns
        if "drop_columns" in config:
            chunk = chunk.drop(columns=config["drop_columns"], errors="ignore")

        # select columns
        if "columns" in config:
            chunk = chunk[config["columns"]]

        # deduplicate
        if "deduplicate" in config:
            chunk = chunk.drop_duplicates(subset=[config["deduplicate"]])

        # parse dates
        if "parse_dates" in config:
            for col in config["parse_dates"]:
                chunk[col] = pd.to_datetime(chunk[col], dayfirst=True).dt.strftime("%Y-%m-%d")

        inserts.extend(generate_insert(config["table"], chunk))

    return inserts

def exec_datatables():

    with open(OUTPUT_SQL, "w", encoding="utf8") as f:

        for dataset in DATASETS:

            inserts = load_dataset(dataset)

            for line in inserts:
                f.write(line + "\n")