import requests, time, random, os
from typing import Optional, Union, List, Any, Dict
import pandas as pd

from .base import BaseAPIInterface
from .constants import INTERPRO
from .utils import get_nested

## Docs available at: https://interpro-documentation.readthedocs.io/en/latest/download.html
## https://github.com/ProteinsWebTeam/interpro7-api/tree/master/docs
## https://www.ebi.ac.uk/interpro/result/download/#

# Define constants for InterPro API
data_types = ["entry", "protein", "structure", "taxonomy", "proteome", "set"]
entry_integration_types = ["all", "integrated", "unintegrated"]

# Constants for search
filter_types = data_types[1:]  # Exclude 'entry' from filter types
db_types = {
    "entry": ["InterPro", "antifam", "pfam", "ncbifam"], # More can be added
    "protein": ["reviewed", "unreviewed", "UniProt"],
    "structure": ["pdb"],
    "taxonomy": ["uniprot"],
    "proteome": ["uniprot"],
    "set": ["cdd", "pfam", "pirsf"]
}


class InterproInstance(BaseAPIInterface):
    def __init__(
            self,
            fields_to_extract: Optional[Union[list, dict]] = None,
            output_dir: Optional[str] = None,
            **kwargs
    ):
        """
        Initialize the InterproInstance.
        Args:
            fields_to_extract (list|dict): Fields to keep from the original response.
                - If list: Keep those keys.
                - If dict: Maps {desired_name: real_field_name}.
        """
        cache_dir = INTERPRO.CACHE_DIR if INTERPRO.CACHE_DIR is not None else ""
        super().__init__(cache_dir=cache_dir, **kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.fields_to_extract = fields_to_extract
    
    def validate_query(self, method: str, query: Dict):
        """
        Validate the query parameters.
        Args:
            method (str): The method to validate against.
            query (dict): The query parameters to validate.
        Raises:
            ValueError: If the query parameters are invalid.
        """
        rules = {
            'db': lambda v: v in db_types[method],
            'entry_integration': lambda v: v in entry_integration_types,
            'modifiers': lambda v: isinstance(v, dict),
            'filters': lambda v: (
                isinstance(v, dict)
                and v.get('type') in filter_types
                and v.get('type') != method
            ),
        }

        for key, check in rules.items():
            if key in query and not check(query[key]):
                if key == 'filters':
                    valid = [ftype for ftype in filter_types if ftype != method]
                    raise ValueError(
                        f"Invalid filter type: {query[key].get('type')}. Valid types are: {valid}"
                    )
                elif key == 'db':
                    raise ValueError(
                        f"Invalid database type: {query['db']} for method {method}. "
                        f"Valid types are: {db_types[method]}"
                    )
                elif key == 'entry_integration':
                    raise ValueError(
                        f"Invalid entry integration type: {query['entry_integration']}. "
                        f"Valid types are: {entry_integration_types}"
                    )
                elif key == 'modifiers':
                    raise ValueError(
                        f"Invalid modifiers type: {type(query['modifiers'])}. Modifiers should be a dictionary."
                    )
                
    def fetch_pages(self, next_url: str, method: str, pages_to_fetch: int = 1):
        """
        Fetch the next page of results from the InterPro API.
        Args:
            next_url (str): The URL for the next page of results.
            method (str): The method used for the initial request.
        Returns:
            dict: The fetched data from the next page.
        """
        try:
            response = self.session.get(next_url)
            self._delay()
            response.raise_for_status()
            next = None
            if 'next' in response.json() and response.json()['next']:
                next = self.fetch_pages(response.json()['next'], method, pages_to_fetch - 1) if pages_to_fetch > 1 else []
            
            response = response.json()['results']
            response = [r['metadata'] for r in response if 'metadata' in r]
            if next:
                response.extend(next)
            return response
        except requests.exceptions.RequestException as e:
            print(f"Error fetching next page for method {method}: {e}")
            return {}

    def fetch(
            self,
            query: Union[str, tuple, dict],
            **kwargs
    ):
        """
        Fetch data from InterPro API.
        Args:
            interpro_id (str): The InterPro ID to fetch.
            db (str): Database type.
            entry_integration (str): Entry integration type.
            modifiers (dict): Modifiers for the request.
            query (dict): Query parameters for the request.
        Returns:
            dict: The fetched data.
        """
        method = kwargs.get("method", None)
        pages_to_fetch = kwargs.get("pages_to_fetch", 1)
  
        if not method:
            raise ValueError("Method must be specified in the query parameters.")
        

        if not isinstance(query, dict):
            raise ValueError("Query must be a dictionary containing 'db', 'entry_integration', 'modifiers', 'filter_type', 'filter_db', and 'filter_value' keys.")

        # Validate the query parameters
        self.validate_query(method, query)

        filter_type = None
        filter_db = None
        filter_value = None

        # Construct the base URL
        url = f"{INTERPRO.API_URL}{method}/"

        if 'filters' in query.keys():
            filter_type = query['filters']['type']
            filter_db = query['filters']['db']
            filter_value = query['filters']['value']

        if 'db' in query:
            url += f"{query['db']}/"
        if 'entry_integration' in query:
            url += f"{query['entry_integration']}/"
        if filter_type and filter_db and filter_value:
            if filter_db in db_types[filter_type]:
                url += f"{filter_type}/{filter_db}/{filter_value}/"
            else:
                raise ValueError(f"Invalid filter database type: {filter_db}. Valid types are: {db_types[filter_type]}")
        if query['modifiers']:
            url += "?"
            for key, value in query['modifiers'].items():
                if value is not None and value != "":
                    url += f"{key}={value}&"
            #remove the last '&'
            url = url[:-1]

        return self.fetch_pages(url, method, pages_to_fetch)

    def parse(self, raw_data: Any, **kwargs) -> Dict:
        """
        Parse the fetched data.
        Args:
            raw_data (dict): The fetched data.
        Returns:
            dict: The parsed data.
        """
        if not raw_data:
            return {}
        if isinstance(raw_data, dict):
            raw_data = [raw_data]
        
        parsed_list = []

        for data in raw_data:
            parsed = {}
            # Determine which fields to include 
            if self.fields_to_extract is None:
                parsed = get_nested(data, "")
                
            elif isinstance(self.fields_to_extract, list):
                for key in self.fields_to_extract:
                    parsed[key] = get_nested(data, key)

            elif isinstance(self.fields_to_extract, dict):
                for new_key, nested_path in self.fields_to_extract.items():
                    parsed[new_key] = get_nested(data, nested_path)
            
            parsed_list.append(parsed)

        return parsed_list
    
    def get_dummy(self, *, method: Optional[str] = None, **kwargs) -> Dict:
        """
        Return a dummy response for testing purposes.
        Args:
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        Returns:
            dict: A dummy response.
        """
        # TODO - Implement a dummy response for InterPro API
        raise NotImplementedError(
            "Dummy response is not implemented for InterPro API. "
        )

    def query_usage(self) -> str:
        return (
            ""
            "Available methods: \n"
            f"{', '.join(data_types)}\n"
            "Available databases: \n"
            f"{', '.join(db_types['entry'])}\n"
            "Available entry integration types: \n"
            f"{', '.join(entry_integration_types)}\n"

            "Example query:\n"
            "interpro_instance.fetch(\n"
            "    query={\n"
            "        'db': 'InterPro',\n"
            "        'entry_integration': 'all',\n"
            "        'modifiers': {'page_size': 10, 'page': 1},\n"
            "        'filters': {'type': 'protein', 'db': 'UniProt', 'value': 'P12345'}\n"
            "    },\n"
            "    method='entry'\n"
            ")\n"
            "This will fetch InterPro entries with the specified filters and modifiers.\n"
            "You can also specify the number of pages to fetch using the 'pages_to_fetch' parameter.\n"
            "For more information, refer to the InterPro API documentation.\n"
        )