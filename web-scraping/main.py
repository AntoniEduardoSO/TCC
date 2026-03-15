from robots.config import exec_robots
from processing.config import exec_processing
from datatable.config import exec_datatables

def main():
    
    # print("Inicializando automação nos portais da transparência.")
    # exec_robots()
    
    print("Inicializando processamento e limpeza de dados do microdados e rendimento do censo.")
    exec_processing()

    print("Inicializando a criacao da database limpa.")
    exec_datatables()


if __name__ == "__main__":
    main()