import os
import json
import time

from tqdm import tqdm
import colorama

from .core import io
from .core import transform

from .portal_type1 import exec1
from .portal_type2 import exec2
from .portal_type3 import exec3
from .portal_type4 import exec4
from .portal_type5 import exec5
from .portal_type6 import exec6
from .portal_type7 import exec7
from .portal_type8 import exec8
from .portal_type9 import exec9
from .portal_type10 import exec10
from .portal_type11 import exec11

from .core.state import ScrapingState

ROBOT_STRATEGIES = {
    "1": exec1,
    "2": exec2,
    "3": exec3,
    "4": exec4,
    "5": exec5,
    "6": exec6,
    "7": exec7,
    "8": exec8,
    "9": exec9,
    "10": exec10,
    "11": exec11
}

def load_cities_config(filepath):
    if not os.path.exists(filepath):
        print(f"ERRO: Arquivo de configuração não encontrado em {filepath}")
        return {}
    
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def exec_robots():

    base_dir = os.getcwd()
    downloads_folder = os.path.join(base_dir, "data", "raw")
    
    config_path = os.path.join(base_dir, "robots", "core", "municipios_config.json")
    state_path = os.path.join(base_dir, "data", "state", "scraping_state_global.csv")

    state = ScrapingState()
    state.load_csv(state_path)

    cities_by_type = load_cities_config(config_path)

    try:
        for type_id, city_list in cities_by_type.items():
            executor_func = ROBOT_STRATEGIES.get(type_id)

            desc_text = f"Tipo {type_id.ljust(2)}"

            with tqdm(total=len(city_list), desc=desc_text, unit="mun", colour="green", leave=True) as pbar:

                def update_progress():
                    pbar.update(1)

                try:
                    executor_func(city_list, downloads_folder, state, progress_callback=update_progress)
                    
                    state.save_csv(state_path)
                    io.clean_tmp_folder(downloads_folder)
                    
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    tqdm.write(f"Erro no Tipo {type_id}: {str(e)}")


    except KeyboardInterrupt:
        print("\n\nOperação interrompida pelo usuário.")

    finally:
        transform.save_all_files()
        print("\n")
        io.clean_tmp_folder(downloads_folder)

