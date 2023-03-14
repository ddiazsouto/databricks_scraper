from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.by import By


class Scraper:

    def __init__(self):
        options = webdriver.ChromeOptions() 
        options.add_argument("user-data-dir=C:\\Users\\DestRuktoR\\AppData\\Local\\Google\\Chrome\\User Data\\Profile 3")
        driver = webdriver.Chrome(options=options)
        self._driver = driver

    def _find_element(self, by: By, value):
        return self._driver.find_element((by, value))

    def _await_element_located(self, by: By, value, timeout):
        wait = WebDriverWait(self._driver, timeout)
        return wait.until(EC.visibility_of_element_located((by, value)))