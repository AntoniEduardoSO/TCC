import pandas as pd
import os

OUTPUT_SQL = "database.sql"
DATA_PATH = "data"


def sql_value(v):

    if pd.isna(v):
        return "NULL"

    if isinstance(v, str):
        v = v.replace("'", "''")
        return f"'{v}'"

    return str(v)


def generate_insert(table, df):

    columns = ",".join(df.columns)
    inserts = []

    for _, row in df.iterrows():
        values = ",".join(sql_value(v) for v in row)
        inserts.append(f"INSERT INTO {table} ({columns}) VALUES ({values});")

    return inserts


def process_csv(path, table):

    print("Processing", path)

    df = pd.read_csv(path, encoding="utf-8", low_memory=False)

    return generate_insert(table, df)


def process_transparencia(path):

    print("Processing transparencia:", path)

    df = pd.read_csv(path, encoding="utf-8", low_memory=False)

    # converter data
    df["data"] = pd.to_datetime(df["data"], dayfirst=True).dt.strftime("%Y-%m-%d")

    return generate_insert("transparencia_despesa", df)


def exec_datatables():

    sql_lines = []

    for root, dirs, files in os.walk(DATA_PATH):

        # remover a pasta raw da busca
        if "raw" in dirs:
            dirs.remove("raw")
        
        if "Transparencia" in dirs:
            dirs.remove("Transparencia")

        for file in files:

            if not file.endswith(".csv"):
                continue

            if file == "scraping_state_global.csv":
                continue

            path = os.path.join(root, file)

            # tratamento especial transparencia
            if "CONSOLIDADO" in file:

                inserts = process_transparencia(path)

            else:

                table = file.replace(".csv", "")
                inserts = process_csv(path, table)

            sql_lines.extend(inserts)

    with open(OUTPUT_SQL, "w", encoding="utf8") as f:
        for line in sql_lines:
            f.write(line + "\n")

    print("SQL gerado com sucesso")