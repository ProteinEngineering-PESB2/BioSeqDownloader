import requests
import time
import random
from typing import Optional, Union, List
import pandas as pd

from .constants import INTERPRO
from .utils import get_nested

## Docs available at: https://interpro-documentation.readthedocs.io/en/latest/download.html
## https://github.com/ProteinsWebTeam/interpro7-api/tree/master/docs
## https://www.ebi.ac.uk/interpro/result/download/#

# Define constants for InterPro API
data_types = ["entry", "protein", "structure", "taxonomy", "proteome", "set"]
db_types = ["InterPro", "antifam", "pfam", "ncbifam"] #More can be added
entry_integration_types = ["all", "integrated", "unintegrated"]

# Constants for search
filter_types = ["protein", "structure", "taxonomy", "proteome", "set"]
filter_db_types = {
    "protein": ["reviewed", "unreviewed", "UniProt"],
    "structure": ["pdb"],
    "taxonomy": ["uniprot"],
    "proteome": ["uniprot"],
    "set": ["cdd", "pfam", "pirsf"]
}


class InterproInstance():
    def __init__(
            self,
            max_workers: int = 5,
            min_wait: float = 0,
            max_wait: float = 2,
            fields_to_extract: Optional[Union[list, dict]] = None,
    ):
        """
        Initialize the InterproInstance.
        Args:
            max_workers (int): Maximum number of parallel requests.
            min_wait (float): Minimum wait time between requests.
            max_wait (float): Maximum wait time between requests.
            fields_to_extract (list|dict): Fields to keep from the original response.
                - If list: Keep those keys.
                - If dict: Maps {desired_name: real_field_name}.
        """
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.fields_to_extract = fields_to_extract
    
    def fetch(
            self, 
            type: str,
            db: str,
            entry_integration: Optional[str] = None,
            modifiers: Optional[dict] = None,
            filter_type: Optional[str] = None,
            filter_db: Optional[str] = None,
            filter_value: Optional[str] = None,
    ):
        """
        Fetch data from InterPro API.
        Args:
            interpro_id (str): The InterPro ID to fetch.
            type (str): Type of data to fetch.
            db (str): Database type.
            entry_integration (str): Entry integration type.
            modifiers (dict): Modifiers for the request.
            query (dict): Query parameters for the request.
        Returns:
            dict: The fetched data.
        """
        # Check if data provided is valid
        if type not in data_types:
            raise ValueError(f"Invalid data type: {type}. Valid types are: {data_types}")
        if db and db not in db_types:
            raise ValueError(f"Invalid database type: {db}. Valid types are: {db_types}")
        if entry_integration and entry_integration not in entry_integration_types:
            raise ValueError(f"Invalid entry integration type: {entry_integration}. Valid types are: {entry_integration_types}")
        if modifiers and not isinstance(modifiers, dict):
            raise ValueError(f"Invalid modifiers type: {type(modifiers)}. Modifiers should be a dictionary.")
        if filter_type and filter_type not in filter_types:
            raise ValueError(f"Invalid filter type: {filter_type}. Valid types are: {filter_types}")
        
        # Construct the base URL
        url = f"{INTERPRO.API_URL}{type}/"

        if db:
            url += f"{db}/"
        if entry_integration:
            url += f"{entry_integration}/"
        if filter_type and filter_db and filter_value:
            if filter_db in filter_db_types[filter_type]:
                url += f"{filter_type}/{filter_db}/{filter_value}/"
            else:
                raise ValueError(f"Invalid filter database type: {filter_db}. Valid types are: {filter_db_types[filter_type]}")
        if modifiers:
            url += "?"
            for key, value in modifiers.items():
                if value is not None and value != "":
                    url += f"{key}={value}&"
            #remove the last '&'
            url = url[:-1]
    
        try:
            response = requests.get(url)
            time.sleep(random.uniform(self.min_wait, self.max_wait))
            if response.status_code == 200:
                response = response.json()
                print(f"{response["count"]} records found")
                return response['results']
            else:
                print(f"Failed to fetch pathway data, code {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
    
    def parse(self, results: List[dict]) -> dict:
        """
        Parse the fetched data.
        Args:
            result (dict): The fetched data.
        Returns:
            dict: The parsed data.
        """
        if not results:
            return None
        
        parsed_list = []

        for result in results:
            parsed = {}
            # Determine which fields to include 
            if self.fields_to_extract is None:
                parsed = get_nested(result, "")

            elif isinstance(self.fields_to_extract, list):
                for key in self.fields_to_extract:
                    parsed[key] = get_nested(result, key)

            elif isinstance(self.fields_to_extract, dict):
                for new_key, nested_path in self.fields_to_extract.items():
                    parsed[new_key] = get_nested(result, nested_path)
            
            parsed_list.append(parsed)

        return parsed_list
    
    def fetch_to_dataframe(self, queries: List[dict]) -> pd.DataFrame:
        """
        Fetch data from InterPro API and convert it to a DataFrame.
        Args:
            query (list[dict]): List of queries to fetch data for.
            an example query is:
                {
                    "type": "entry",
                    "db": "InterPro",
                    "entry_integration": "all",
                    "modifiers": {"go_term": "GO:12345"},
                    "filter_type": "protein",
                    "filter_db": "reviewed",
                    "filter_value": "P12345"
                }
        Returns:
            pd.DataFrame: DataFrame containing the fetched data.
        """
        parsed_results = []
        for query in queries:
            result = self.fetch(
                type=query.get("type"),
                db=query.get("db"),
                entry_integration=query.get("entry_integration"),
                modifiers=query.get("modifiers"),
                filter_type=query.get("filter_type"),
                filter_db=query.get("filter_db"),
                filter_value=query.get("filter_value"),
            )
            if result:
                parsed = self.parse(result)
                if parsed:
                    parsed_results.extend(parsed)
                else:
                    print(f"Failed to parse result for query: {query}")
            else:
                print(f"Failed to fetch result for query: {query}")
        
        return pd.DataFrame(parsed_results)