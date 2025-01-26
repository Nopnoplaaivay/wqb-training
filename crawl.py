import os
import time
import requests
import sqlite3
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv(".env")


class CrawlService:

    API_URL = "https://api.worldquantbrain.com"
    DATA_FIELDS_URL = [
        "https://api.worldquantbrain.com/data-fields?dataset.id=fundamental6&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # fundamental6
        "https://api.worldquantbrain.com/data-fields?dataset.id=fundamental2&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # fundamental2
        "https://api.worldquantbrain.com/data-fields?dataset.id=analyst4&delay=1&instrumentType=EQUITY&limit=50&offset=20&region=USA&universe=TOP3000",  # analyst4
        "https://api.worldquantbrain.com/data-fields?dataset.id=model16&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # model16
        "https://api.worldquantbrain.com/data-fields?dataset.id=model51&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # model51
        "https://api.worldquantbrain.com/data-fields?dataset.id=news12&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # news12
        "https://api.worldquantbrain.com/data-fields?dataset.id=news18&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # news18
        "https://api.worldquantbrain.com/data-fields?dataset.id=option8&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # option8
        "https://api.worldquantbrain.com/data-fields?dataset.id=option9&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # option9
        "https://api.worldquantbrain.com/data-fields?dataset.id=pv1&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # pv1
        "https://api.worldquantbrain.com/data-fields?dataset.id=pv13&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # pv13
        "https://api.worldquantbrain.com/data-fields?dataset.id=univ1&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # univ1
        "https://api.worldquantbrain.com/data-fields?dataset.id=socialmedia12&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # socialmedia12
        "https://api.worldquantbrain.com/data-fields?dataset.id=socialmedia8&delay=1&instrumentType=EQUITY&limit=50&region=USA&universe=TOP3000",  # socialmedia8
    ]

    @classmethod
    def crawl(cls):
        """Initialize sqlite"""
        db = sqlite3.connect("data.db")
        cursor = db.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS data_fields (id TEXT PRIMARY KEY, desc TEXT, name TEXT, category TEXT, sub_category TEXT, region TEXT, delay TEXT, universe TEXT, data_type TEXT, coverage TEXT, user_count TEXT, alpha_count TEXT)"
        )
        db.commit()

        """SETUP CHROME DRIVER"""
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument("--log-level=3")
        options.add_argument("--disable-logging")
        options.add_argument("--silent")
        options.add_argument("headless")
        options.add_argument("window-size=1920x1080")
        options.add_argument("disable-gpu")
        service = Service(executable_path=ChromeDriverManager().install())
        driver = webdriver.Chrome(options=options, service=service)
        driver.set_page_load_timeout(60)

        """LOGIN"""
        print("Logging in...")
        driver.get("https://platform.worldquantbrain.com/sign-in")
        time.sleep(5)
        auth_content = driver.find_element(By.CSS_SELECTOR, ".auth__content")
        input_email = auth_content.find_element(By.CSS_SELECTOR, "input[type='email']")
        input_password = auth_content.find_element(
            By.CSS_SELECTOR, "input[type='password']"
        )
        submit_button = auth_content.find_element(
            By.CSS_SELECTOR, "button[type='submit']"
        )
        cookie_button = driver.find_element(
            By.CSS_SELECTOR, ".cookie-consent__modal-section--buttons .button--primary"
        )

        email = os.getenv("EMAIL")
        password = os.getenv("PASSWORD")

        input_email.send_keys(email)
        input_password.send_keys(password)
        cookie_button.click()
        time.sleep(1)
        submit_button.click()
        time.sleep(1)

        """GET COOKIES"""
        cookie = cls.get_cookie(driver)
        print(f"Cookie: {cookie}")

        """GET DATASETS INFO"""
        for data_fields_url in cls.DATA_FIELDS_URL:
            res = requests.get(data_fields_url, headers={"cookie": cookie})
            data_fields = res.json()
            count = data_fields["count"]
            results = data_fields["results"]
            sub_category = results[0].get("subcategory").get("id")
            print(f"Total count: {count}")
            print(f"Crawling {sub_category} data fields...")

            # Iterate over 50 data fields until fill count
            offset = 0
            while offset < count:
                res = requests.get(
                    f"{data_fields_url}&offset={offset}", headers={"cookie": cookie}
                )
                data_fields = res.json()
                results = data_fields["results"]
                for result in results:
                    data_id = result.get("id")
                    desc = result.get("description")
                    name = result.get("dataset").get("name")
                    category = result.get("category").get("id")
                    sub_category = result.get("subcategory").get("id")
                    region = result.get("region")
                    delay = result.get("delay")
                    universe = result.get("universe")
                    data_type = result.get("type")
                    coverage = result.get("coverage")
                    user_count = result.get("userCount")
                    alpha_count = result.get("alphaCount")

                    cursor.execute(
                        "INSERT OR REPLACE INTO data_fields (id, desc, name, category, sub_category, region, delay, universe, data_type, coverage, user_count, alpha_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            data_id,
                            desc,
                            name,
                            category,
                            sub_category,
                            region,
                            delay,
                            universe,
                            data_type,
                            coverage,
                            user_count,
                            alpha_count,
                        ),
                    )
                db.commit()
                offset += 50
            time.sleep(3)
        driver.quit()
        db.close()
        print("DONE")

    @classmethod
    def get_cookie(cls, driver):
        data_page_link = "https://platform.worldquantbrain.com/data?delay=1&instrumentType=EQUITY&region=USA&universe=TOP3000"
        driver.get(data_page_link)
        browse_dataset_button = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".data-main__browse-buttons-section .button--primary")
            )
        )
        browse_dataset_button.click()
        time.sleep(3)

        def check_cookie(str):
            return str.startswith("_g") or str.startswith("_fbp") or str.startswith("t")

        """GET COOKIES"""
        cookies = []
        cookie_list = driver.execute_cdp_cmd("Network.getAllCookies", {}).get("cookies")
        for cookie in cookie_list:
            if check_cookie(cookie["name"]):
                cookies.append(f"{cookie['name']}={cookie['value']}")

        # insert "; " between cookies
        cookies = "; ".join(cookies)
        return cookies


if __name__ == "__main__":
    CrawlService.crawl()
