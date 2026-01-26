import pandas as pd
from .categories import categorizar_despesa

EDUCATION_TERMS = [
    'EDUCA',
    'FUNDEB',
    'FUNDO DE EDUCACAO',
    'FUNDO MUNICIPAL DE EDUCACAO',
    'SECRETARIA DE EDUCACAO',
    'SEMED',
    'ENSINO'
]

def _normalize_str(value):
    if not isinstance(value, str):
        return ""
    return value.strip().upper()

def _is_education(row):
    orgao = _normalize_str(row.get("Órgão"))
    unidade = _normalize_str(row.get("Unidade"))

    base = f"{orgao} {unidade}"
    return any(term in base for term in EDUCATION_TERMS)

def _build_description(row):
    parts = []

    for col in ["DsEmpenho", "DsItemDespesa", "Elemento"]:
        val = row.get(col)
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())

    if not parts:
        return "SEM DESCRIÇÃO"

    return " | ".join(parts)

def transform_portal_type2(
    df_raw: pd.DataFrame,
    municipio_nome: str,
    codigo_ibge: str,
    ano_referencia: int
) -> pd.DataFrame:
    """
    Transforma CSV bruto do Portal Tipo 2
    em DataFrame final do TCC
    """

    df = df_raw.copy()

    # Filtra Educação
    df = df[df.apply(_is_education, axis=1)]

    if df.empty:
        return pd.DataFrame()

    # Colunas finais básicas
    df["Data"] = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
    df["Empenho"] = df["Empenho"].astype(str)
    df["Credor"] = df["Credor"].astype(str)
    df["Valor"] = pd.to_numeric(df["Empenhado"], errors="coerce").fillna(0)

    # Órgão consolidado
    df["Orgao_Consolidado"] = (
        df["Órgão"].astype(str).str.strip() +
        " - " +
        df["Unidade"].astype(str).str.strip()
    )

    # Descrição final
    df["Descrição"] = df.apply(_build_description, axis=1)

    # Categoria (ML / Dashboard)
    df["Categoria"] = df["Descrição"].apply(categorizar_despesa)

    # Metadados do TCC
    df["Ano"] = int(ano_referencia)
    df["Municipio_nome"] = municipio_nome
    df["municipio_id"] = codigo_ibge

    # Seleção final
    df_final = df[
        [
            "Data",
            "Empenho",
            "Orgao_Consolidado",
            "Credor",
            "Valor",
            "Descrição",
            "Categoria",
            "Ano",
            "Municipio_nome",
            "municipio_id",
        ]
    ].copy()

    return df_final