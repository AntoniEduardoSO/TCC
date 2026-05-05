import os
import sys
import json

from robots.config import exec_robots
from processing.config import exec_processing
from datatable.config import exec_datatables

from datetime import datetime

FLAG_PATH = "status/scraping_done.flag"

def clean_flag():
    if os.path.exists(FLAG_PATH):
        os.remove(FLAG_PATH)

def mark_as_finished():
    os.makedirs(os.path.dirname(FLAG_PATH), exist_ok=True)
    with open(FLAG_PATH, "w") as f:
        json.dump({
            "status": "done",
            "timestamp": datetime.now().isoformat()
        }, f)

def already_executed():
    return os.path.exists(FLAG_PATH)

def main():

    if len(sys.argv) < 2:
        print("Informe o modo: 1 (completo) ou 2 (somente criar as tabelas do banco)")
        return


    modo = sys.argv[1]

    if modo == "1":
        print("Modo 1, execucao completa (Vai demorar pelo menos umas 2 horas)")

        clean_flag()

        # print("Inicializando automação nos portais da transparência.")
        # exec_robots()
        
        print("Inicializando processamento e limpeza de dados do microdados e rendimento do censo.")
        exec_processing()

        print("Inicializando a criacao da database limpa.")
        exec_datatables()

        mark_as_finished()

    elif modo == "2":

        if not already_executed():
            print("Erro: scraping ainda não foi executado.")
            return
        
        print("Inicializando a criacao da database limpa.")
        exec_datatables()
    
    else:
        print("Modo inválido. Use 1 ou 2.")

if __name__ == "__main__":
    main()