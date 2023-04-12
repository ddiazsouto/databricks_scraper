from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from scraper import Scraper
from functions import extract_user, extract_date, build_array
from login_info import EMAIL, PASSWD
import pandas as pd
import datetime
import os


databricks_target_path = "https://adb-909870082157824.4.azuredatabricks.net/"\
                         "?o=909870082157824#notebook/908271178432007/command/908271178432009"

dummy_data = {"Daniel Diaz Souto's Cluster": [['Alias Idle', 'Tue, Apr 11, 2023, 16:45:46 GMT+1', 'by ddiazsouto@gmail.com', '/Users/ddiazsouto@gmail.com/Alias']],
              "Retail_Users": [['Eight Idle', 'Tue, Apr 11, 2023, 16:46:05 GMT+1', 'by ddiazsouto@gmail.com', '/Users/ddiazsouto@gmail.com/Contains/Eight'],
                               ['Scrap it Idle', 'Tue, Apr 11, 2023, 16:35:07 GMT+1', 'by ddiazsouto@gmail.com', '/Users/ddiazsouto@gmail.com/Scrap it'],
                               ['Test Idle', 'Tue, Apr 11, 2023, 16:45:21 GMT+1', 'by ddiazsouto@gmail.com', '/Users/ddiazsouto@gmail.com/Test'],
                               ['Five Idle', 'Tue, Apr 11, 2023, 16:46:12 GMT+1', 'by ddiazsouto@gmail.com', '/Users/ddiazsouto@gmail.com/Contains/Five']]
                               }
logs = {'Inactive clusters': []}

class DatabricksUsers(Scraper):

    def __init__(self, headless):
        super().__init__(headless)
        self.found_clusters = {}
        self.data = {}

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
        self._await_element_located(By.XPATH, "//a[@data-testid='Compute']", 10).click()
        try:
            table_row = 1
            while table_row := table_row + 1:
                cluster_row = self._await_element_located(By.XPATH, "//div[1]/div[3]/div[1]/div[2]/div[1]/div[1]"
                                                          "/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[1]/"
                                                          f"div[{table_row}]", 5)
                cluster_name, cluster_path = self._extract_cluster_name_and_path(cluster_row)
                self.found_clusters[cluster_name] = cluster_path
        except TimeoutException:
            pass  # The table of clusters does not contain more rows, so we finalize the method

    def lookup_active_clusters_notebooks_and_its_users(self):
        for cluster_name, cluster_path in self.found_clusters.items():
            self._get(cluster_path)
            try:
                scraper._await_element_located(By.XPATH, "//span[normalize-space()='Terminate']", 5)
                notebooks_information = self._open_notebooks_tab_and_collect_information(cluster_name)
                self.data[cluster_name] = notebooks_information
            except TimeoutException:
                logs['Inactive clusters'].append(cluster_name)

    @property
    def dataframe_from_current_data(self):
        dataframe = {'Cluster_name': [], 'Notebook_name': [], 'Status': [],
                     'Workspace_name': [], 'Last_command_run': [], 'User_location': []}
        for cluster, notebook_information in self.data.items():
            for table_row in notebook_information:
                dataframe['User_location'].append(table_row[-1])
                dataframe['Last_command_run'].append(extract_date(table_row))
                dataframe['Status'].append(table_row[0].split()[-1])
                dataframe['Notebook_name'].append(table_row[0].split()[0])
                dataframe['Cluster_name'].append(cluster)
                dataframe['Workspace_name'].append(0)  # TODO: change that 0 for other value
        
        return pd.DataFrame(dataframe)

    def save_file_in_path(self, df: pd.DataFrame):

        path = f"databricks/data/cluster_information/{datetime.date.today()}/"
        os.makedirs(path, exist_ok=True)
        current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")
        df.to_csv(f"{path}/{current_timestamp}.csv", index=False)

    def _open_notebooks_tab_and_collect_information(self, cluster_name):
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
                if notebooks_table.text == 'No notebooks are attached to this cluster':
                    break # Need to design what would happen here
                return cluster_notebooks

    def _extract_cluster_name_and_path(self, cluster_row):
        cluster_name = cluster_row.text.split("\n")[0]
        tag_content = cluster_row.get_attribute('innerHTML')
        cluster_relative_path = tag_content[tag_content.find("<a href="):]
        cluster_relative_path = cluster_relative_path[:cluster_relative_path.find(">")]
        cluster_absolute_path = self._driver.current_url + cluster_relative_path[26:]

        return cluster_name, cluster_absolute_path

    def run(self):
        print("\n\n>>>>> Login\n")
        self.login(databricks_target_path)
        print("\n\n>>>>> Collect clusters path\n")
        self.collect_existing_clusters_path()
        print("\n\n>>>>> Look at the active clusters\n")
        self.lookup_active_clusters_notebooks_and_its_users()
        print(">>>>> Parsing collected data")
        df = self.dataframe_from_current_data
        self.save_file_in_path(df)
        self._quit()

    def _quit(self):
        self._driver.quit()


scraper = DatabricksUsers(True)
scraper.run()
