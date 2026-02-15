import os
import tempfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait

CHROMEDRIVER_PATH = ChromeDriverManager().install()

def get_driver(download_folder, headless=False):

    download_folder = os.path.abspath(download_folder)
    
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    chrome_options = webdriver.ChromeOptions()
    
    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
        "download.transfer_protection_enabled": False, 
        "browser.helperApps.alwaysAsk.force": False,
    }

    profile_dir = tempfile.mkdtemp()
    
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    chrome_options.add_argument(f"--user-data-dir={profile_dir}")
    chrome_options.add_argument("--profile-directory=Default")

    chrome_options.add_argument("--log-level=3") 
    chrome_options.add_argument("--silent")

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-features=InsecureDownloadWarnings")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    if headless:
        chrome_options.add_argument("--headless=new")
    
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": download_folder
    })
    
    wait = WebDriverWait(driver, 40) # Aumentado para 40s
    
    return driver, wait