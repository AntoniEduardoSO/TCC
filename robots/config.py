import os
import time

from .portal_type1 import exec1
from .portal_type2 import exec2
from .portal_type3 import exec3
# from .portal_type4 import exec4

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
    # {
    #     "nome": "BARRA_DE_SAO_MIGUEL",
    #     "codigo_ibge": "2700607",
    #     "url": "https://municipioonline.com.br/al/prefeitura/barradesaomiguel/cidadao/despesa",
    #     "years_list": [2017, 2018, 2019, 2020, 2021, 2022]
    # },
    # {
    #     "nome": "BELEM",
    #     "codigo_ibge": "2700805",
    #     "url": "https://municipioonline.com.br/al/prefeitura/belem/cidadao/despesa",
    #     "years_list": [2017, 2018, 2019, 2020, 2021]
    # },
    # {
    #     "nome": "BELO_MONTE",
    #     "codigo_ibge": "2700904",
    #     "url": "https://www.municipioonline.com.br/al/prefeitura/belomonte/cidadao/despesa",
    #     "years_list": [2017, 2018, 2019, 2020, 2021]
    # },
    # {
    #     "nome": "CAMPO_ALEGRE",
    #     "codigo_ibge": "2701407",
    #     "url": "https://www.municipioonline.com.br/al/prefeitura/campoalegre/cidadao/despesa",
    #     "years_list": [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    # },
    # {
    #     "nome": "DOIS_RIACHOS",
    #     "codigo_ibge": "2702504",
    #     "url": "https://www.municipioonline.com.br/al/prefeitura/doisriachos/cidadao/despesa",
    #     "years_list": [2017, 2018, 2019, 2020, 2025]
    # },
    # {
    #     "nome": "IBATEGUARA",
    #     "codigo_ibge": "2703007",
    #     "url": "https://www.municipioonline.com.br/al/prefeitura/ibateguara/cidadao/despesa",
    #     "years_list": [2017, 2018, 2019, 2020, 2021]
    # },
    {
        "nome": "TEOTONIO_VILELA",
        "codigo_ibge": "2709152",
        "url": "https://www.municipioonline.com.br/al/prefeitura/teotoniovilela/cidadao/despesa",
        "years_list": [2017] # , 2019, 2020, 2021, 2022, 2023, 2024, 2025
    },
    
]

MUNICIPIOS_TIPO3 = [
    {
        "nome": "BATALHA",
        "codigo_ibge": "2700706",
        "url": "https://portalbatalha.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024]
    },
    {
        "nome": "BRANQUINHA",
        "codigo_ibge": "2701100",
        "url": "https://portalbranquinha.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },

    {
        "nome": "CANAPI",
        "codigo_ibge": "2701605",
        "url": "https://portalpmcanapi.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },

    {
        "nome": "FLEXEIRAS",
        "codigo_ibge": "2702801",
        "url": "http://portalflexeiras.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },

    {
        "nome": "IGACI",
        "codigo_ibge": "2703106",
        "url": "http://portalflexeiras.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },
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
        time_execs = []
        inicio = time.perf_counter()
        # exec1(MUNICIPIOS_TIPO1, driver, wait, downloads_folder)
        # time_execs.append(time.time() - inicio)

        
        exec2(MUNICIPIOS_TIPO2, driver, wait, downloads_folder)
        time_execs.append( (time.perf_counter() - inicio) / 60)
        

        # exec3(MUNICIPIOS_TIPO3, driver, wait, downloads_folder)

        # exec4(MUNICIPIOS_TIPO4, driver, wait, downloads_folder)

        fim = time.time()



        print("Tempos de cada tipo de portal:")
        i = 0
        while i < len(time_execs):
            print(f"Tempo de execucao do portal {i + 1}: {time_execs[i]:.2f} minutos")
            i+=1

        # verificar_dados()

        print()
        

    finally:
        driver.quit()
        print("Finalizado o web scrapping")

