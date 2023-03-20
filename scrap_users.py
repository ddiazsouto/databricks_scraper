from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from scraper import Scraper
from functions import extract_user, extract_date, build_array
from login_info import EMAIL, PASSWD


databricks_target_path = "https://adb-909870082157824.4.azuredatabricks.net/"\
                         "?o=909870082157824#notebook/908271178432007/command/908271178432009"

logs = {'Inactive clusters': [], 'Active clusters': [], "Cluster iterations": []}


class DatabricksUsers(Scraper):

    def __init__(self, headless):
        super().__init__(headless)
        self.found_clusters = {}

    def login(self, databricks_target_path):
        """
        Description:
            The method that takles loging in to Azure Databricks.
        """
        self._get(databricks_target_path)
        self._await_element_located(By.XPATH, "//uses-legacy-bootstrap/div/div/div/div/div[1]", 5).click()
        try:
            self._manual_verification_needed
        except TimeoutException:
            print("We're in I would say")

    def _manual_verification_needed(self):
        for text_box_locator, text_to_input in zip(["loginfmt", "passwd"], [EMAIL, PASSWD]):
            self._await_element_located(By.NAME, text_box_locator, 5).send_keys(text_to_input)
            self._await_element_located(By.ID, "idSIButton9", 5).click()
        self._await_element_located(By.ID, "idSIButton9", 5).click()
        while wait_for_manual_login := input("\nPlease verify manually \n\n"
                                             "Press 'q' to continue\n\n>>>>") != "q":
            pass

    def collect_existing_clusters_path(self):
        """
        Description:
            Takes a start in the main Databricks dashboard, then goes to the Compute tab and reads the existing clusters
            and collects theis url addresses from the compute clusters table.
        """
        self._driver.find_element(By.XPATH, "//a[@data-testid='Compute']").click()
        try:
            table_row = 1
            while table_row := table_row + 1:
                cluster_row = self._await_element_located(By.XPATH,
                                                          "//div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/div[1]"
                                                          "/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]"
                                                          f"/div[1]/div[2]/table[1]/tbody[1]/tr[{table_row}]", 5)
                cluster_name, cluster_path = self._extract_cluster_name_and_path(cluster_row)
                self.found_clusters[cluster_name] = cluster_path
        except TimeoutException:
            pass  # The table of clusters does not contain more rows, so we finalize the method

    def lookup_active_clusters_notebooks_and_its_users(self):
        for cluster_name, cluster_path in self.found_clusters.items():
            self._get(cluster_path)
            try:
                scraper._await_element_located(By.XPATH, "//span[normalize-space()='Terminate']", 5)
                self._open_notebooks_tab_and_collect_information()
            except TimeoutException:
                logs['Inactive clusters'].append(cluster_name)

    def _open_notebooks_tab_and_collect_information(self):
        # Open notebooks tab from a cluster's main dashboard
        self._await_element_located(By.XPATH, "/html[1]/body[1]/div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/div[1]"
                                              "/form[1]/div[2]/uses-legacy-bootstrap[1]/ul[1]/li[2]/a[1]", 10).click()
        cluster_notebooks = []
        while notebooks_tab_contains_next_page := True:
            notebooks_table = self._await_element_located(By.XPATH, "//tbody[@class='du-bois-light-table-tbody']", 5)
            cluster_notebooks += build_array(notebooks_table.text)
            try:
                next_page = self._await_element_located(By.XPATH, "//span[@aria-label='right']//*[name()='svg']", 5)
                next_page.click()
            except TimeoutException:
                breakpoint()
                break

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
scraper.lookup_active_clusters_notebooks_and_its_users()
breakpoint()
scraper.quit()


# print("\n\n Inner html")
# print(reat_table_with_usernames.get_attribute('innerHTML'))
# time.sleep(50)
