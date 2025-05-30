from typing import Optional, Union, List, Dict, Any
import requests, random, time

import pandas as pd

from .constants import GENONTOLOGY
from .utils import get_nested

METHODS = [
    "ontology-term",
    "go"
]

METHOD_OPTIONS = {
    "ontology-term": ["graph", "subgraph"],
    "go": ["hierarchy", "models"]
}

class GenOntologyInterface():
    def __init__(
            self,
            max_workers: int = 5,
            min_wait: int = 1,
            max_wait: int = 2,
            fields_to_extract: Optional[Union[List, Dict]] = None,
    ):
        """
        Initialize the GenOntologyInterface class.
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

    def fetch(
            self,
            method: str,
            query: str,
            option: Optional[str] = None,
    ):
        """
        Fetch data from the GenOntology API.
        Args:
            method (str): Method to use for the request. Used methods are 'ontology-term' and 'go'.
            query (str): Query string to search for.
            option (str): Additional options for the request.
                - For 'ontology-term' method, options can be 'graph' or 'subgraph'.
                - For 'go' method, options can be 'hierarchy' or 'models'.    
        Returns:
            any: response from the API.
        """
        if method not in METHODS:
                raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")
        if option and option not in METHOD_OPTIONS.get(method, []):
            raise ValueError(f"Option {option} is not supported for method {method}. Supported options are: {', '.join(METHOD_OPTIONS.get(method, []))}.")

        url = f"{GENONTOLOGY.API_URL}{method.replace('-', '/')}/{query.upper().replace(':', '%3A')}" # Replace ':' with '%3A' for URL encoding

        if option:
            url += f"/{option}"
        print(f"Fetching data from {url}")

        try:
            response = requests.get(url)
            time.sleep(random.uniform(self.min_wait, self.max_wait))
            if response.status_code == 200:
                 return response
            else:
                print(f"Failed to fetch data from GenOntology API, code {response.status_code}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return {}

    def parse(
            self,
            response: Any,
            look_for_relationships: bool = False,
    ) -> Dict:
        """
        Parse the response from the GenOntology API.
        Args:
            response (any): Response from the API.
            look_for_relationships (bool): If True, will look for relationships in the response.
        Returns:
            dict: Parsed response.
        """
        if not response:
            return {}

        if isinstance(response, requests.models.Response):
            data = response.json()
        elif isinstance(response, dict):
            data = response
        else:
            raise ValueError("Response must be a requests.Response object or a dictionary.")
        
        parsed = {}

        if self.fields_to_extract is None:
            parsed = get_nested(data, "")

        elif isinstance(self.fields_to_extract, list):
            for key in self.fields_to_extract:
                parsed[key] = get_nested(data, key)

        elif isinstance(self.fields_to_extract, dict):
            for new_key, nested_path in self.fields_to_extract.items():
                parsed[new_key] = get_nested(data, nested_path)
        else:
            raise ValueError("fields_to_extract must be a list or a dictionary.")

        # Get related gen ontology terms if requested
        if look_for_relationships:
            try:
                rel_response = self.fetch(method="ontology-term", query=parsed.get("goid", ""), option="graph")
                if isinstance(rel_response, requests.models.Response) and rel_response.status_code == 200:
                    graph_json = rel_response.json()
                    nodes = graph_json.get("topology_graph_json", {}).get("nodes", [])
                    relationships = [node.get("id") for node in nodes if "id" in node]
                    parsed["relationships"] = relationships
                else:
                    parsed["relationships"] = []
            except Exception as e:
                print(f"Error fetching relationships: {e}")

        return parsed
    
    def fetch_to_dataframe(
            self,
            method: str,
            query: Union[str, List[str]],
            option: Optional[str] = None,
            look_for_relationships: bool = False
    ) -> Dict:
        """
        Fetch data from the GenOntology API and parse it into a dictionary.
        Args:
            method (str): Method to use for the request.
            query (str or list): Query string or list of strings to search for.
            option (str): Additional options for the request.
                - For 'ontology-term' method, options can be 'graph' or 'subgraph'.
                - For 'go' method, options can be 'hierarchy' or 'models'.
            look_for_relationships (bool): If True, will look for relationships in the response.
        Returns:
            dict: Parsed response from the API.
        """
        export_df = pd.DataFrame()

        if isinstance(query, str):
            response = self.fetch(method, query, option)
            parsed_data = self.parse(response, look_for_relationships=look_for_relationships)
            export_df = pd.DataFrame([parsed_data])
        elif isinstance(query, list):
            for q in query:
                response = self.fetch(method, q, option)
                parsed_data = self.parse(response, look_for_relationships=look_for_relationships)
                export_df = pd.concat([export_df, pd.DataFrame([parsed_data])], ignore_index=True)
        else:
            raise ValueError("Query must be a string or a list of strings.")
        if export_df.empty:
            print("No data found for the given query.")
            return export_df
  
        return export_df.reset_index(drop=True)
            
