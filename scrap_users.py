from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from scraper import Scraper
import time
from login_info import EMAIL, PASSWD


databricks_target_path = "https://adb-909870082157824.4.azuredatabricks.net/"\
                         "?o=909870082157824#notebook/908271178432007/command/908271178432009"


class DatabricksUsers(Scraper):

    def login(self, databricks_target_path):

        driver = self._driver    
        driver.get(databricks_target_path)

        sign_in_button = self._driver.find_element(By.XPATH, "//uses-legacy-bootstrap/div/div/div/div/div[1]")
        sign_in_button.click()
        try:            
            time.sleep(3)
            print("2 --> #############################################################") # We got here, then we failed
            email_box = driver.find_element(By.NAME, "loginfmt") # This broke it with NoSuchElementException
            email_box.send_keys(EMAIL)
            accept_email = driver.find_element(By.ID, "idSIButton9").click()

            time.sleep(6)
            passwd_button = driver.find_element(By.NAME, "passwd")
            passwd_button.send_keys(PASSWD)
            accept_passwd = driver.find_element(By.ID, "idSIButton9").click()
            time.sleep(2)
            remember_me = driver.find_element(By.ID, "idSIButton9").click()

            print("Please verify manually")
            time.sleep(300)
        except NoSuchElementException:
            print("We're in I would say")

    def _next_window(self):
        new_window = self._driver.window_handles[1]
        self._driver.close()
        time.sleep(20)
        self._driver.switch_to.window(new_window)


    def access_the_data(self):
        self._driver.get(databricks_target_path)
        open_cluster_dropdown = self._await_element_located(By.XPATH, 
                                                            "//button[@data-testid='cluster-dropdown-v2-button']", 15)
        open_cluster_dropdown.click()

        select_cluster_name = self._await_element_located(By.XPATH, "//span[@data-testid='attached-cluster-name']", 15)
        select_cluster_name.click()
        access_spark_UI = self._await_element_located(By.XPATH, "//div[normalize-space()='Spark UI']", 15)
        access_spark_UI.click()

        self._next_window()
        open_notebooks_tab = self._await_element_located(By.XPATH, "/html[1]/body[1]/div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/div[1]/form[1]/div[2]/uses-legacy-bootstrap[1]/ul[1]/li[2]/a[1]", 10)
        open_notebooks_tab.click()


        read_table_with_usernames = self._driver.find_element(By.XPATH, "//tbody[@class='du-bois-light-table-tbody']", 10)

        notebook_ui_information = read_table_with_usernames.text        
        breakpoint()

    def quit():
        self._driver.quit()




scraper = DatabricksUsers()
scraper.login(databricks_target_path)
scraper.access_the_data()
scraper.quit()

breakpoint()



# print("\n\n Inner html")
# print(reat_table_with_usernames.get_attribute('innerHTML'))
# time.sleep(50)

def read_exception(ex):
    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    message = template.format(type(ex).__name__, ex.args)
    print(message)

    