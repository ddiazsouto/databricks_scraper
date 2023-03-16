from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from scraper import Scraper
import time
from login_info import EMAIL, PASSWD


databricks_target_path = "https://adb-909870082157824.4.azuredatabricks.net/"\
                         "?o=909870082157824#notebook/908271178432007/command/908271178432009"

logs = {}


class DatabricksUsers(Scraper):

    def __init__(self, headless):
        super().__init__(headless)
        self.clusters_path = []

    def login(self, databricks_target_path):
        self._driver.get(databricks_target_path)
        sign_in_button = self._await_element_located(By.XPATH, "//uses-legacy-bootstrap/div/div/div/div/div[1]", 10)
        sign_in_button.click()
        try:
            email_box = self._await_element_located(By.NAME, "loginfmt", 5).send_keys(EMAIL)
            accept_email = self._await_element_located(By.ID, "idSIButton9", 5).click()

            passwd_button = self._await_element_located(By.NAME, "passwd", 5).send_keys(PASSWD)
            accept_passwd = self._await_element_located(By.ID, "idSIButton9", 5).click()
            remember_me = self._await_element_located(By.ID, "idSIButton9", 5).click()
            print("Please verify manually")
            time.sleep(300)
        except TimeoutException:
            print("We're in I would say")

    def collect_existing_clusters_path(self):
        open_compute_tab = self._driver.find_element(By.XPATH, "//a[@data-testid='Compute']")
        open_compute_tab.click()
        try:
            for table_row in range(2, 10):
                cluster = self._await_element_located(By.XPATH, "//div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/div[1]"
                                                                "/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]"
                                                                f"/div[1]/div[2]/table[1]/tbody[1]/tr[{table_row}]", 5)
                tag_content = cluster.get_attribute('innerHTML')
                cluster_relative_path = tag_content[tag_content.find("<a href="):]
                cluster_relative_path = cluster_relative_path[:cluster_relative_path.find(">")]
                self.clusters_path.append(self._driver.current_url + cluster_relative_path[26:])
        except TimeoutException:
            print("Finished at:", table_row)
        logs['found_clusters'] = table_row
        breakpoint()

    def lookup_active_clusters_notebooks(self):
        for cluster_path in self.clusters_path:
            self._driver.get(cluster_path)
            

    def _next_window(self):
        new_window = self._driver.window_handles[1]
        self._driver.close()
        time.sleep(20)
        self._driver.switch_to.window(new_window)

    def access_the_data(self):
        open_cluster_dropdown = self._await_element_located(By.XPATH,
                                                            "//button[@data-testid='cluster-dropdown-v2-button']", 15)
        open_cluster_dropdown.click()

        select_cluster_name = self._await_element_located(By.XPATH, "//span[@data-testid='attached-cluster-name']", 15)
        select_cluster_name.click()
        access_spark_UI = self._await_element_located(By.XPATH, "//div[normalize-space()='Spark UI']", 15)
        access_spark_UI.click()
        # print(access_spark_UI.get_attribute('innerHTML'))

        self._next_window()
        open_notebooks_tab = self._await_element_located(By.XPATH, "/html[1]/body[1]/div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/div[1]/form[1]/div[2]/uses-legacy-bootstrap[1]/ul[1]/li[2]/a[1]", 10)
        open_notebooks_tab.click()

        read_table_with_usernames = self._driver.find_element(By.XPATH, "//tbody[@class='du-bois-light-table-tbody']")

        page_2_rel_path = self._driver.find_element(By.XPATH, "//a[normalize-space()='2']")
        next_page = self._driver.find_element(By.XPATH, "//span[@aria-label='right']//*[name()='svg']")

        notebook_ui_information = read_table_with_usernames.text



    def quit():
        self._driver.quit()



scraper = DatabricksUsers(True)
scraper.login(databricks_target_path)
scraper.collect_existing_clusters_path()
scraper.quit()

breakpoint()



# print("\n\n Inner html")
# print(reat_table_with_usernames.get_attribute('innerHTML'))
# time.sleep(50)

def read_exception(ex):
    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    message = template.format(type(ex).__name__, ex.args)
    print(message)

    