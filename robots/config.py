from .portal_type1 import exec1

MUNICIPIOS_TIPO1 = [
    {
        "nome": "ARAPIRACA",
        "codigo_ibge": "2700300",
        "url": "https://transparencia.arapiraca.al.gov.br/despesas"
    }
]
def exec_robots():
    exec1(MUNICIPIOS_TIPO1)