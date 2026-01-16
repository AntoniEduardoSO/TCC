import os

from .portal_type1 import exec1
from .driver_setup import get_driver
from .portal_type2 import exec2


MUNICIPIOS_TIPO1 = [
    {
        "nome": "ARAPIRACA",
        "codigo_ibge": "2700300",
        "url": "https://transparencia.arapiraca.al.gov.br/despesas"
    }
]

MUNICIPIOS_TIPO2 = [
    {
        "nome": "BARRAO_DE_SAO_MIGUEL",
        "codigo_ibge": "2700607",
        "url": "https://municipioonline.com.br/al/prefeitura/barradesaomiguel/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021, 2022]
    }
]


def exec_robots():
    base_dir = os.getcwd()

    downloads_folder = os.path.join(base_dir, "..", "data/raw")

    driver, wait = get_driver(downloads_folder)

    try:
        # exec1(MUNICIPIOS_TIPO1, driver, wait, downloads_folder)

        exec2(MUNICIPIOS_TIPO2, driver, wait, downloads_folder)

    finally:
        driver.quit()
        print("Finalizado o web scrapping")

