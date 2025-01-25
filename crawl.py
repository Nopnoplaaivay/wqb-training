import os
import time
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException
)
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv(".env")


class CrawlService:
    @classmethod
    def crawl(cls):
        '''SETUP CHROME DRIVER'''
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument("--log-level=3")  
        options.add_argument("--disable-logging")  
        options.add_argument("--silent") 
        # options.add_argument("headless")
        options.add_argument("window-size=1920x1080")
        options.add_argument("disable-gpu")
        service = Service(executable_path=ChromeDriverManager().install())
        driver = webdriver.Chrome(options=options, service=service)
        driver.set_page_load_timeout(60)

        '''LOGIN'''
        driver.get("https://platform.worldquantbrain.com/sign-in")
        time.sleep(5)
        auth_content = driver.find_element(By.CSS_SELECTOR, ".auth__content")
        input_email = auth_content.find_element(By.CSS_SELECTOR, "input[type='email']")
        input_password = auth_content.find_element(By.CSS_SELECTOR, "input[type='password']")
        submit_button = auth_content.find_element(By.CSS_SELECTOR, "button[type='submit']")
        cookie_button = driver.find_element(By.CSS_SELECTOR, ".cookie-consent__modal-section--buttons .button--primary")

        email = os.getenv("EMAIL")
        password = os.getenv("PASSWORD")

        input_email.send_keys(email)
        input_password.send_keys(password)
        cookie_button.click()
        time.sleep(1)
        submit_button.click()
        time.sleep(1)

        '''GET COOKIES'''
        cookies = cls.get_cookies(driver)
        print(f"Cookie: {cookies}")


        time.sleep(60*60)
        driver.quit()

    @classmethod
    def get_cookies(cls, driver):
        data_page_link = "https://platform.worldquantbrain.com/data?delay=1&instrumentType=EQUITY&region=USA&universe=TOP3000"
        driver.get(data_page_link)
        browse_dataset_button = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".data-main__browse-buttons-section .button--primary")))
        browse_dataset_button.click()
        time.sleep(3)

        def check_cookie(str):
            return str.startswith("_g") or str.startswith("_fbp") or str.startswith("t") 

        '''GET COOKIES'''
        cookies = []
        cookie_list = driver.execute_cdp_cmd("Network.getAllCookies", {}).get("cookies")
        for cookie in cookie_list:
            if check_cookie(cookie['name']):
                print(f"{cookie['name']}={cookie['value']}")
                cookies.append(f"{cookie['name']}={cookie['value']}")

        # insert "; " between cookies
        cookies = "; ".join(cookies)
        return cookies


if __name__ == "__main__":
    CrawlService.crawl()