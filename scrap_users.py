from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from scraper import Scraper
import time
from functions import  extract_user, extract_date
from login_info import EMAIL, PASSWD


databricks_target_path = "https://adb-909870082157824.4.azuredatabricks.net/"\
                         "?o=909870082157824#notebook/908271178432007/command/908271178432009"

logs = {'Inactive clusters': [], 'Active clusters': []}


class DatabricksUsers(Scraper):

    def __init__(self, headless):
        super().__init__(headless)
        self.found_clusters = {}

    def login(self, databricks_target_path):
        self._driver.get(databricks_target_path)
        sign_in_button = self._await_element_located(By.XPATH, "//uses-legacy-bootstrap/div/div/div/div/div[1]", 5)
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
        open_compute_tab = self._driver.find_element(By.XPATH, "//a[@data-testid='Compute']").click()
        try:
            for table_row in range(2, 10):
                cluster_row = self._await_element_located(By.XPATH, "//div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/div[1]"
                                                                "/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]"
                                                                f"/div[1]/div[2]/table[1]/tbody[1]/tr[{table_row}]", 5)
                cluster_name, cluster_path = self._extract_cluster_name_and_path(cluster_row)
                self.found_clusters[cluster_name] = cluster_path
        except TimeoutException:
            pass # The table of clusters does not contain more rows

    def lookup_active_clusters_notebooks(self):
        for cluster_name, cluster_path in self.found_clusters.items():
            self._driver.get(cluster_path)
            try:
                scraper._await_element_located(By.XPATH, "//span[normalize-space()='Terminate']", 5)
                logs['Active clusters'].append(cluster_name)
                try:
                    print("Accessing the data")
                    self.open_and_read_notebooks_tab()
                except TimeoutException:
                    continue
            except TimeoutException:
                logs['Inactive clusters'].append(cluster_name)

    def open_and_read_notebooks_tab(self):
        open_notebooks_tab = self._await_element_located(By.XPATH, "/html[1]/body[1]/div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/div[1]/form[1]/div[2]/uses-legacy-bootstrap[1]/ul[1]/li[2]/a[1]", 10)
        open_notebooks_tab.click()

        read_notebooks_tab = self._await_element_located(By.XPATH, "//tbody[@class='du-bois-light-table-tbody']", 5)
        self.clusters_information = read_notebooks_tab.text

        while notebooks_tab_contains_next_page := True:            
            try:
                print("HERE")
                next_page = self._await_element_located(By.XPATH, "//span[@aria-label='right']//*[name()='svg']", 5)
                print("1")
                breakpoint()
                next_page.click()
            except NoSuchElementException:
                print("10")                
                breakpoint()
                break
        print("2")
        breakpoint()

    def _extract_cluster_name_and_path(self, cluster_row):
        cluster_name = cluster_row.text.split("\n")[0]
        tag_content = cluster_row.get_attribute('innerHTML')
        cluster_relative_path = tag_content[tag_content.find("<a href="):]
        cluster_relative_path = cluster_relative_path[:cluster_relative_path.find(">")]
        cluster_absolute_path = self._driver.current_url + cluster_relative_path[26:]        

        return cluster_name, cluster_absolute_path

    def quit(self):
        self._driver.quit()



scraper = DatabricksUsers(True)
print("\n\n>>> Login\n")
scraper.login(databricks_target_path)
print("\n\n>>> Collect clusters path\n")
scraper.collect_existing_clusters_path()
print("\n\n>>> Look at the active clusters\n")
scraper.lookup_active_clusters_notebooks()
breakpoint()
scraper.quit()



# print("\n\n Inner html")
# print(reat_table_with_usernames.get_attribute('innerHTML'))
# time.sleep(50)

def read_exception(ex):
    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    message = template.format(type(ex).__name__, ex.args)
    print(message)

    