import os
import tempfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait

os.environ['WDM_LOG_LEVEL'] = '0'
CHROMEDRIVER_PATH = ChromeDriverManager().install()

def get_driver(download_folder, headless=False):

    download_folder = os.path.abspath(download_folder)
    os.makedirs(download_folder, exist_ok=True)

    cache_dir = os.path.join(download_folder, "chrome_cache")
    os.makedirs(cache_dir, exist_ok=True)

    chrome_options = webdriver.ChromeOptions()

    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }

    chrome_options.add_experimental_option("prefs", prefs)

    chrome_options.add_argument(f"--disk-cache-dir={cache_dir}")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--disable-cache")
    chrome_options.add_argument("--log-level=3") 
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    if headless:
        chrome_options.add_argument("--headless=new")

    service = Service(
        CHROMEDRIVER_PATH,
        log_path=os.devnull
    )
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": download_folder
    })

    wait = WebDriverWait(driver, 40)

    return driver, wait