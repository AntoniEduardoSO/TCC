import os

from .portal_type1 import exec1
from .portal_type2 import exec2
from .portal_type3 import exec3
from .portal_type4 import exec4

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
        "nome": "BARRA_DE_SAO_MIGUEL",
        "codigo_ibge": "2700607",
        "url": "https://municipioonline.com.br/al/prefeitura/barradesaomiguel/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021, 2022]
    },
    {
        "nome": "BELEM",
        "codigo_ibge": "2700805",
        "url": "https://municipioonline.com.br/al/prefeitura/belem/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021]
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

MUNICIPIOS_TIPO4 = [
    {
        "nome": "BARRA_DE_SAO_MIGUEL_2",
        "codigo_ibge": "2700607",
        "url": "https://www.kalana.com.br/transparencia/12263869000108/0000/despesas?m=false",
        "api_url": "https://kalana.com.br/mobiledados"
    }
]

def exec_robots():
    base_dir = os.getcwd()

    downloads_folder = os.path.join(base_dir, "..", "data/raw")

    driver, wait = get_driver(downloads_folder)

    try:
        # exec1(MUNICIPIOS_TIPO1, driver, wait, downloads_folder)

        # exec2(MUNICIPIOS_TIPO2, driver, wait, downloads_folder)

        # exec3(MUNICIPIOS_TIPO3, driver, wait, downloads_folder)

        exec4(MUNICIPIOS_TIPO4, driver, wait, downloads_folder)

        # verificar_dados()
        

    finally:
        driver.quit()
        print("Finalizado o web scrapping")

