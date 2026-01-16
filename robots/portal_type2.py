import time
import os
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.support.ui import Select


def exec2(cities_config, driver, wait, downloads_folder):
    for city in cities_config:
        
        df_city_years = []

        years_to_process = city.get("years_list", [])


        try:

            driver.get(city['url'])

            tab_link = wait.until(EC.presence_of_element_located((By.ID, "lnkEmpenhos")))
            driver.execute_script("arguments[0].click();", tab_link)

            time.sleep(1)

            for year in years_to_process:

                select_year = Select(wait.until(EC.presence_of_element_located((By.ID, "ddlAnoEmpenhos"))))
                select_year.select_by_value(str(year))

                time.sleep(5)



            


        
        except Exception as e:
            print("nao deu bixcoitao: {e}")
