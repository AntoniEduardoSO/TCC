import pandas as pd
from datetime import datetime

STATUS_OK = "OK"
STATUS_EMPTY = "EMPTY"
STATUS_INVALID = "INVALID"
STATUS_INCONSISTENT = "INCONSISTENT"

RAW_REQUIRED_COLUMNS = {
    "Órgão",
    "Unidade",
    "Data",
    "Empenho",
    "Credor",
    "Empenhado"
}

FINAL_REQUIRED_COLUMNS = {
    "Data",
    "Empenho",
    "Orgao_Consolidado",
    "Credor",
    "Valor",
    "Descrição",
    "Categoria",
    "Ano",
    "Municipio_nome",
    "municipio_id"
}

def _parse_date(value):
    if pd.isna(value):
        return None

    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value).strip(), fmt)
        except:
            pass
    return None


def validate_raw_dataframe(df, expected_year, tolerance=0.05):
    """
    Valida o CSV bruto baixado do portal tipo 2
    """

    if df is None or df.empty:
        return False, STATUS_EMPTY, "CSV bruto vazio"

    # Colunas mínimas do portal
    missing = RAW_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        return (
            False,
            STATUS_INVALID,
            f"Colunas ausentes no CSV bruto: {missing}"
        )

    # Datas
    parsed_dates = df["Data"].apply(_parse_date).dropna()
    if parsed_dates.empty:
        return False, STATUS_INVALID, "Nenhuma data válida encontrada"

    wrong_year = parsed_dates.apply(lambda d: d.year != expected_year)
    ratio = wrong_year.sum() / len(parsed_dates)

    if ratio > tolerance:
        return (
            False,
            STATUS_INCONSISTENT,
            f"{ratio:.1%} das datas fora do ano {expected_year}"
        )

    return True, STATUS_OK, "CSV bruto válido"

def validate_dataframe(df):
    """
    Valida o DataFrame já tratado e pronto para salvar
    """

    if df is None or df.empty:
        return False, STATUS_EMPTY, "DataFrame final vazio"

    # Estrutura final
    missing = FINAL_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        return (
            False,
            STATUS_INVALID,
            f"Colunas finais ausentes: {missing}"
        )

    # Tipos básicos
    if not pd.api.types.is_numeric_dtype(df["Valor"]):
        return False, STATUS_INVALID, "Coluna Valor não é numérica"

    if not pd.api.types.is_integer_dtype(df["Ano"]):
        return False, STATUS_INVALID, "Coluna Ano inválida"

    # Sanidade
    if (df["Valor"] < 0).any():
        return False, STATUS_INCONSISTENT, "Valores negativos encontrados"

    return True, STATUS_OK, "DataFrame final válido"