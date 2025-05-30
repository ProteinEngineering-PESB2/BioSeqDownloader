from typing import Optional, Union, List, Dict
import requests, time, random

import pandas as pd

from .constants import BIOGRID
from .utils import get_nested

# Rest documentation: https://wiki.thebiogrid.org/doku.php/biogridrest

METHODS = [
    "interactions",
    "organisms",
    "identifiers",
    "evidence"
]

# TODO add more from docs
query_params_base = {
    "accessKey": (None, "string", "Your BioGRID access key"),
    "start": (0, "integer", "Start index for pagination"),
    "max": (10000, "integer", "Maximum number of results to return"),
    "interSpeciesExclude": (False, "boolean", "Include interactions between different species"),
    "selfInteractionsExclude": (False, "boolean", "If ‘true’, interactions with one interactor will be excluded"),
    "evidenceList" : (None, "string", "Comma-separated list of evidence codes to filter results"),
    "includeEvidence" : (False, "boolean", "If ‘true’, evidence codes will be included in the results"),
    "geneList" : (None, "string", "List of gene names to filter results."),
    "searchIds" : (False, "boolean", "If ‘true’, the interactor ENTREZ_GENE, ORDERED LOCUS and SYSTEMATIC_NAME (orf) will be examined for a match with the geneList."),
    "format" : ("tab2", "string", "Format of the response. Options are 'tab1','tab2', 'extendedTab2', 'count', 'json', 'jsonExtended'. Default is 'tab2'."),
}

# TODO considerar si hacer default json o no
# TODO falta threading o batching

class BioGRIDInterface():
    def __init__(
            self,
            max_workers: int = 5,
            min_wait: int = 1,
            max_wait: int = 2,
            fields_to_extract: Optional[Union[List, Dict]] = None,
    ):
        """
        Initialize the BioGRIDInterface class.
        Args:
            max_workers (int): Maximum number of workers for parallel processing.
            min_wait (int): Minimum wait time between requests.
            max_wait (int): Maximum wait time between requests.
            fields_to_extract (list or dict): Fields to extract from the response.
        """
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.fields_to_extract = fields_to_extract
    
    def usage(self):
        """
        Prnt the usage of the BioGRID API.
        Returns:
            str: Usage information.
        """
        return "\n".join([f"{key}: {value[0]} ({value[1]}) - {value[2]}" for key, value in query_params_base.items()])
    
    def fetch(
            self,
            method: str,
            query: Dict = {}
    ):
        """
        Fetch data from the BioGRID API.
        Args:
            method (str): Method to use for the request. Currently, only 'interaction' is supported.
            query (Dict): Query parameters to filter the results.
        Returns:
            any: response from the API.
        """
        if method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")
        
        for key in query.keys():
            if key not in query_params_base:
                raise ValueError(f"Query parameter {key} is not supported. Supported parameters are: {', '.join(query_params_base.keys())}.")

        
        # Generate url
        url = f"{BIOGRID.API_URL}{method}?"

        for key, value in query.items():
            if isinstance(value, List):
                url += f"{key}=" + "|".join(value)  # Convert list to pipe-separated string
            else:
                url += f"{key}={str(value)}"

            url += "&"

        url = url.rstrip("&")
        
        print(f"Fetching data from {url}")
        try:
            response = requests.get(url)
            time.sleep(random.uniform(self.min_wait, self.max_wait))
            if response.status_code == 200:
                 return response
            else:
                print(f"Failed to fetch data from BioGRID API, code {response.status_code}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return {}

    def parse(
            self,
            response: requests.Response
    ):
        """
        Parse the response from the BioGRID API.
        Args:
            response (requests.Response): Response object from the API.
        Returns:
            any: Parsed data from the response.
        """
        if not response:
            return {}

        if isinstance(response, requests.models.Response):
            data = response.json()
        elif isinstance(response, dict):
            data = response
        else:
            raise ValueError("Response must be a requests.Response object or a dictionary.")
        
        parsed_list = []

        for _, value in data.items():
            parsed = {}
            if self.fields_to_extract is None:
                parsed = get_nested(value, "")

            elif isinstance(self.fields_to_extract, list):
                for key in self.fields_to_extract:
                    parsed[key] = get_nested(value, key)

            elif isinstance(self.fields_to_extract, dict):
                for new_key, nested_path in self.fields_to_extract.items():
                    parsed[new_key] = get_nested(value, nested_path)
            else:
                raise ValueError("fields_to_extract must be a list or a dictionary.")
            
            parsed_list.append(parsed)
        
        return parsed_list
            

    def fetch_to_dataframe(
            self,
            method: str,
            query: Dict = {}
    ):
        """
        Fetch data from the BioGRID API and return it as a pandas DataFrame.
        Args:
            method (str): Method to use for the request.
            query (Dict): Query parameters to filter the results.
        Returns:
            pd.DataFrame: DataFrame containing the fetched data.
        """
        response = self.fetch(method, query)
        return pd.DataFrame(self.parse(response))
    
