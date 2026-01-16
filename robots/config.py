import os

from .portal_type1 import exec1
from .portal_type2 import exec2
from .portal_type3 import exec3

from .driver_setup import get_driver
from .verificar_dados import verificar_dados


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

MUNICIPIOS_TIPO3 = [
    {
        "nome": "BATALHA",
        "codigo_ibge": "2700706",
        "url": "https://portalbatalha.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024]
    }
]

def exec_robots():
    base_dir = os.getcwd()

    downloads_folder = os.path.join(base_dir, "..", "data/raw")

    driver, wait = get_driver(downloads_folder)

    try:
        # exec1(MUNICIPIOS_TIPO1, driver, wait, downloads_folder)

        # exec2(MUNICIPIOS_TIPO2, driver, wait, downloads_folder)

        exec3(MUNICIPIOS_TIPO3, driver, wait, downloads_folder)

        # verificar_dados()
        

    finally:
        driver.quit()
        print("Finalizado o web scrapping")

