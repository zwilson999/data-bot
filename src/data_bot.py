import argparse
import time
import json
import requests
import pandas as pd
import sqlalchemy

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from datetime import datetime

# For removing SSL verify request warning
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GeotabIgnition:
    
    def __init__(self, user: str, pwd: str) -> None:
        self.user: str = user
        self.pwd: str = pwd
        self.ignition_login_url: str = "https://ignition.geotab.com"
        self.create_query_job_url: str = "https://ignition.geotab.com/createQueryJob"
        self.check_job_status_url: str = "https://ignition.geotab.com/getJobStatus"
        self.get_query_results_url: str = "https://ignition.geotab.com/getQueryResults"

    def login(self) -> "GeotabIgnition":

        # Load Chromedriver executable and configure chrome options for driver
        ser: Service = Service("../webdriver/chromedriver_linux64/chromedriver")
        opts: webdriver.ChromeOptions = webdriver.ChromeOptions()
        opts.add_argument("log-level=3")
        opts.add_experimental_option("detach", True)

        # Set ChromeDriver capabilities
        capabilities: dict = DesiredCapabilities.CHROME
        capabilities["goog:loggingPrefs"] = {"performance": "ALL"}

        # Create driver object to work with web page
        self.driver: webdriver.Chrome = webdriver.Chrome(service=ser, options=opts, desired_capabilities=capabilities)
        self.driver.get(self.ignition_login_url)

        # Wait until page renders and we can find the email field
        WebDriverWait(self.driver, timeout=5).until(lambda d: d.find_element(by=By.NAME, value="email"))

        # Enter username in dialog box
        username_box: webdriver.WebElement = self.driver.find_element(by=By.NAME, value="email")
        username_box.send_keys(self.user)

        # Enter password in dialog box
        pwd_box: webdriver.WebElement = self.driver.find_element(by=By.NAME, value="password")
        pwd_box.send_keys(self.pwd)

        # Login to Ignition
        login_button: webdriver.WebElement = self.driver.find_element(by=By.ID, value="login-page-submit-button")
        login_button.click()

        # Wait for login to render new page before proceeding
        time.sleep(5)

        return self

    def get_api_token(self) -> "GeotabIgnition":
        """
        Process login network events and retrive the token that is created upon login into Geotab Ignition
        """

        def process_browser_log_entry(entry: dict) -> dict:
            try:
                processed_log: dict = json.loads(entry['message'])['message']['params']['request']['postData']
                return processed_log
            except KeyError:
                pass

        # For Network logging
        browser_log: dict = self.driver.get_log('performance')
        browser_events: list = [process_browser_log_entry(event) for event in browser_log]
        browser_events: list = [i for i in browser_events if i is not None]
        browser_events: list = list(set([event for event in browser_events if "token=" in event if "&" not in event]))

        # Extract API token that is generated at login
        self.token: str = "".join(browser_events)

        return self

    def create_query_job(self, query: str) -> "GeotabIgnition":
        """
        creates a query job with Geotab's Ignition API.
        Returns a JobId which we can use to monitor the status of and eventually extract it's results
        """
        query_params: dict = {
            "token": self.token[6:],
            "projectId": "geotab-public-intelligence",
            "query": query,
            "maxResults": 50000
        }
        # Create query job and get Job ID
        query_job_resp = requests.post(self.create_query_job_url, data=query_params, verify=False).json()
        time.sleep(1)
        self.job_id: str = query_job_resp['id']
        return self

    def check_job_status(self) -> "GeotabIgnition":
        job_status_params: dict = {
            "token": self.token[6:],
            "projectId": "geotab-public-intelligence",
            "jobId": self.job_id
        }
        # Evaluate job's status until it is complete
        curr_job_status: str = requests.post(self.check_job_status_url, data=job_status_params, verify=False).json()['status']
        while curr_job_status == "RUNNING":
            curr_job_status: str = requests.post(self.check_job_status_url, data=job_status_params, verify=False).json()['status']
            time.sleep(2)
        return self

    def get_counties(self) -> "list[str]":

        query_results_params: dict = {
            "token": self.token[6:],
            "jobId": self.job_id,
            "projectId": "geotab-public-intelligence",
            "maxResults": 50000
        }
        query_results: dict = requests.post(self.get_query_results_url, data=query_results_params, verify=False).json()
        counties: list[str] = [d['f'] for d in query_results['apiResponse']['rows']]
        counties: list[str] = [d['v'] for l in counties for d in l] # d is nested within a list of lists of dicts (one element per list which is a dict)

        return counties

    def get_query_results(self) -> pd.DataFrame:
        query_results_params: dict = {
            "token": self.token[6:],
            "jobId": self.job_id,
            "projectId": "geotab-public-intelligence",
            "maxResults": 50000
        }
        query_results: dict = requests.post(self.get_query_results_url, data=query_results_params, verify=False).json()
        time.sleep(1)

        # Within each object we will have a key named "f" which will have another array of objects (each object consists of a single column-row entry)
        try:
            df_tmp: pd.DataFrame = pd.DataFrame(query_results['apiResponse']['rows'])
            rows: list = [l for l in df_tmp['f'].tolist()]

            # The schema for our table
            column_schema: list[str] = [
                "geohash",
                "geohash_bounds",
                "latitude_sw",
                "longitude_sw",
                "latitude_ne",
                "longitude_ne",
                "location",
                "latitude",
                "longitude",
                "city",
                "county",
                "state",
                "country",
                "iso_3166_2",
                "severity_score",
                "incidents_total",
                "update_date",
                "version"
            ]

            # Create pandas df from list of lists and resolve dictionaries to values
            df_final: pd.DataFrame = (pd.DataFrame.from_records(rows, index=df_tmp.index, columns=column_schema)
                                                  .applymap(lambda f: f['v']))
            df_final.loc[:, "inserted_date"] = datetime.today()
            return df_final
        except KeyError:
            print("Keys not found for JSON")
            print(f"This error was found for State: {df_final['state'].unique()}")
            pass


def write_to_sql(df: pd.DataFrame, usr: str, pwd: str, server: str="DataFiles", db: str="GeotabIgnition", tbl: str="hazardous_driving_areas") -> None:

    engine = sqlalchemy.create_engine(f"postgresql://{usr}:{pwd}@localhost:5432/GeotabIgnition")
    conn = engine.connect()

    dtypes: dict = {
        "geohash": sqlalchemy.VARCHAR(40),
        "geohash_bounds": sqlalchemy.VARCHAR(None),
        "latitude_sw": sqlalchemy.DECIMAL(8, 6),
        "longitude_sw": sqlalchemy.DECIMAL(9, 6),
        "latitude_ne": sqlalchemy.DECIMAL(8, 6),
        "longitude_ne": sqlalchemy.DECIMAL(9, 6),
        "location": sqlalchemy.VARCHAR(None),
        "latitude": sqlalchemy.DECIMAL(8, 6),
        "longitude": sqlalchemy.DECIMAL(9, 6),
        "city": sqlalchemy.VARCHAR(100),
        "county": sqlalchemy.VARCHAR(100),
        "state": sqlalchemy.VARCHAR(50),
        "country": sqlalchemy.VARCHAR(40),
        "iso_3166_2": sqlalchemy.VARCHAR(10),
        "severity_score": sqlalchemy.FLOAT,
        "incidents_total": sqlalchemy.INT,
        "update_date": sqlalchemy.DATE,
        "version": sqlalchemy.VARCHAR(5),
        "inserted_date": sqlalchemy.TIMESTAMP
    }
    print("Writing Data to SQL...")
    print(df.head())
    df.to_sql(name=tbl, con=conn, if_exists='append', index=False, schema='public', chunksize=10000, dtype=dtypes)
    

def get_credentials(creds_path: str="../creds/ignition_creds.txt") -> "tuple[str, str]":
    with open(creds_path, mode='r') as f:
        text: list[str] = f.read().splitlines()
        user: str = text[0]
        pwd: str = text[1]
        f.close()

    return user, pwd

def main(args: argparse.Namespace) -> None:

    # Obtain user credentials for login
    if args.creds is None:
        user, pwd = get_credentials()
    else:
        user, pwd = get_credentials(args.creds)

    geotab_ignition: GeotabIgnition = GeotabIgnition(user, pwd)

    # Login to Geotab Ignition
    ignition_driver: GeotabIgnition = geotab_ignition.login()

    # Scrape API token after login
    ignition_driver.get_api_token()

    # Retrieve State names as they exist in Geotab db
    with open("../data/states.txt", mode='r') as f:
        states_to_query: list[str] = f.read().splitlines()
        f.close()

    state_dfs: list[pd.DataFrame] = []
    starttime = datetime.now()
    for state in states_to_query:
        print(f"Processing {state}")

        # For TX we will query by all counties to avoid data limits
        if state == "Texas":
            county_query: str = f"select distinct County from UrbanInfrastructure.HazardousDrivingAreas where Country = 'United States of America (the)' and State = '{state}'"
            print(f"Getting TX County List...")
            county_list: list[str] = ignition_driver.create_query_job(county_query).check_job_status().get_counties()

            for c in county_list:
                query: str = f"select * from UrbanInfrastructure.HazardousDrivingAreas where Country = 'United States of America (the)' and State = '{state}' and County = '{c}'"
                print(f"Processing TX County: {c}")
                data: pd.DataFrame = ignition_driver.create_query_job(query).check_job_status().get_query_results()
                state_dfs.append(data)

        # For CA we will query by all counties to avoid data limits
        elif state == "California":
            county_query: str = f"select distinct County from UrbanInfrastructure.HazardousDrivingAreas where Country = 'United States of America (the)' and State = '{state}'"
            print(f"Getting CA County List...")
            county_list: list[str] = ignition_driver.create_query_job(county_query).check_job_status().get_counties()

            for c in county_list:
                query: str = f"select * from UrbanInfrastructure.HazardousDrivingAreas where Country = 'United States of America (the)' and State = '{state}' and County = '{c}'"
                print(f"Processing CA County: {c}")
                data: pd.DataFrame = ignition_driver.create_query_job(query).check_job_status().get_query_results()
                state_dfs.append(data)
        else:
            query: str = f"select * from UrbanInfrastructure.HazardousDrivingAreas where Country = 'United States of America (the)' and State = '{state}'"
            data: pd.DataFrame = ignition_driver.create_query_job(query).check_job_status().get_query_results()
            state_dfs.append(data)

        print(f"Finished Processing {state}")

    combined_df: pd.DataFrame = pd.concat(state_dfs, ignore_index=True)

    # Round columns to correct precision for storing lat-long (5 places after decimal).
    # Due to float precision issues, sometimes lat longs from geotab overflow into too many digits.
    cols_to_round: list[str] = ['latitude_sw', 'longitude_sw', 'latitude_ne', 'longitude_ne', 'latitude','longitude']
    combined_df.loc[:, cols_to_round] = combined_df.loc[:, cols_to_round].astype(float).round(5)

    # Write final Combined frame to DB
    write_to_sql(combined_df, usr=args.psql_user, pwd=args.psql_pwd)
    print(f"Finished loading data in: {datetime.now() - starttime}")

    # Close the web driver upon completion
    ignition_driver.driver.close()


if __name__ == "__main__":
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument("--creds", required=False, type=str, help="Path to point to text file with first line as Ignition username and second line as Ignition password (must be registered)")
    parser.add_argument("--psql_user", required=False, type=str, help="Username for Postgres server.")
    parser.add_argument("--psql_pwd", required=False, type=str, help="Password for Postgres server.")
    args: argparse.Namespace = parser.parse_args()
    main(args)