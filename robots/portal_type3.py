from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time

def exec3(cities_config, driver, wait, downloads_folder):
    for city in cities_config:

        url = city['url']
        termos_busca = ["Educa", "Fundeb", "FUNDEB"]
        driver.get(url)

        try:
            # O ID do botão é geralmente o ID do input + "_B-1"
            dropdown_button_id = "EntidadeId_B-1"
            
            btn_expandir = wait.until(EC.element_to_be_clickable((By.ID, dropdown_button_id)))
            btn_expandir.click()
            
            # O ID da tabela que contém a lista é "EntidadeId_DDD_L_LBT" baseado no seu HTML
            lista_container_id = "EntidadeId_DDD_L_LBT"
            wait.until(EC.visibility_of_element_located((By.ID, lista_container_id)))
            
            # Pequena pausa de segurança para garantir que a animação do DevExpress terminou
            time.sleep(0.5) 

            # As opções são células (td) com a classe 'dxeListBoxItem_Mulberry'
            # Usamos CSS Selector para pegar as TDs dentro da tabela especifica
            opcoes = driver.find_elements(By.CSS_SELECTOR, f"#{lista_container_id} td.dxeListBoxItem_Mulberry")
            
            entidade_encontrada = False

            for opcao in opcoes:
                texto_opcao = opcao.text
                
                if any(termo in texto_opcao for termo in termos_busca):
                    opcao.click()
                    entidade_encontrada = True
                    break 
            
            if not entidade_encontrada:
                print(f"Nenhuma entidade com os termos {termos_busca} foi encontrada na lista.")

        except Exception as e:
            print(f"Erro ao selecionar a entidade: {e}")
            raise e

    # --- Resto do código (seleção de ano, clique em pesquisar, etc) ---
    # ...