from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


# Colocando o webdriver do google dentro do drive.
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

try:
    url = "https://transparencia.arapiraca.al.gov.br/despesas"
    driver.get(url) # va para o site indicado

    wait = WebDriverWait(driver, 10) # basicamente um timer do selenium para esperar 10 segundos (carregar infos)

    cookies_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "msg-cookies")))
    cookies_button.click()
    print("aviso de cookie fechado.")

    orgao_element = wait.until(EC.presence_of_element_located((By.ID, "orgao"))) # basicamente procure dentro da html, algum elemento com o ID "orgao"
    select_orgao = Select(orgao_element) # Acesse o element, que no caso do html eh um SELECT

    select_orgao.select_by_value("06") # basicamente como temos um select, ele deve ter um valor agregado para a lista, e o de educacao eh o 06
    print(f"Orgao {select_orgao} selecionado ") # confirmacao.

    botao_buscar = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-primary"))) # busca o elemento clicavel (button) que tem em seu css button.btn-primary
    botao_buscar.click() # clique no elemento buscado.

    print("botao buscar clicado. Aguarde resultados")
    time.sleep(5)

finally:
    driver.quit()