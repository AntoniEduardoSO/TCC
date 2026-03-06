from robots.config import exec_robots
from processing.config import exec_processing

def main():
    
    print("Inicializando automação nos portais da transparência.")
    exec_robots()
    
    # print("Inicializando processamento e limpeza de dados do microdados e rendimento do censo")
    # exec_processing()


if __name__ == "__main__":
    main()