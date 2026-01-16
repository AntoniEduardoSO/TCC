import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait

def get_driver(download_folder, headless=False):
    
    # Configura e retorna uma instância do Chrome Driver e o WebDriverWait.
    
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    chrome_options = webdriver.ChromeOptions()
    
    # Configurações de Download e PDF
    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "directory_upgrade": True,
        "plugins.always_open_pdf_externally": True # Evita visualizador de PDF do Chrome
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Opções extras de estabilidade
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080") # Garante que elementos estejam visíveis
    
    if headless:
        chrome_options.add_argument("--headless")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Um Wait padrão de 20 segundos (sites governamentais são lentos)
    wait = WebDriverWait(driver, 20) 
    
    return driver, wait