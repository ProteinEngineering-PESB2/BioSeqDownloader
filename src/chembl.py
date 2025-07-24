import os, re
from typing import Optional, Union, Dict, List, Any
import requests

from .base import BaseAPIInterface
from .constants import CHEMBL

# For the moment, only activity is necessary, but more methods can be added later.
METHODS = [
    "activity",
    "binding_site",
]

METHODS_FORMATS = {
    "activity": ["json", "xml"],
    "binding_site": ["json", "xml"],
}

class ChEMBLInterface(BaseAPIInterface):
    def __init__(
            self,
            cache_dir: Optional[str] = None,
            config_dir: Optional[str] = None,
            output_dir: Optional[str] = None,
            **kwargs
    ):
        """
        Initialize the ChEMBLInterface class.
        Args:
            cache_dir (str): Directory to cache results.
            config_dir (str): Directory for configuration files.
            output_dir (str): Directory to save output files.
        """
        if cache_dir:
            cache_dir = os.path.abspath(cache_dir)
        else:
            cache_dir = CHEMBL.CACHE_DIR if CHEMBL.CACHE_DIR is not None else ""

        if config_dir is None:
            config_dir = CHEMBL.CONFIG_DIR if CHEMBL.CONFIG_DIR is not None else ""

        super().__init__(cache_dir=cache_dir, config_dir=config_dir, **kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def validate_query(self, method: str, query: Dict):
        """
        Validate the query parameters.
        Args:
            method (str): The method to validate against.
            query (dict): The query parameters to validate.
        Raises:
            ValueError: If the query parameters are invalid.
        """
        # TODO - Add more validation rules based on the method and query structure.
        rules = {
            'target_chembl_id': lambda v: isinstance(v, str) and v.strip() != "",
            'pchembl_value': lambda v: isinstance(v, (int, float)),
        }

        for key, check in rules.items():
            if key in query and not check(query[key]):
                if key == 'target_chembl_id':
                    raise ValueError(f"Invalid target_chembl_id: {query['target_chembl_id']}. It should be a non-empty string.")
                elif key == 'pchembl_value':
                    raise ValueError(f"Invalid pchembl_value: {query['pchembl_value']}. It should be a number (int or float).")

    def fetch_pages(self, next_url: str, method: str, pages_to_fetch: int = 1):
        """
        Fetch the next page of results from the ChEMBL API.
        Args:
            next_url (str): The URL for the next page of results.
            method (str): The method used for the initial request.
        Returns:
            dict: The fetched data from the next page.
        """
        print(f"Fetching page: {next_url} for method {method} with pages_to_fetch={pages_to_fetch}")
        responses = []
        try:
            response = self.session.get(next_url, headers={"Content-Type": "application/json"})
            self._delay()
            response.raise_for_status() 
            
            if response.status_code == 204:
                print(f"No content returned for URL {next_url}.")
                return {}

            data = response.json()

            if 'activities' in data.keys() and isinstance(data['activities'], list):
                responses.extend(data['activities'])
            else:
                responses.append(data)

            next = None
            if 'page_meta' in data and data['page_meta'].get('next'):
                next = self.fetch_pages(
                    "https://www.ebi.ac.uk" + data['page_meta']['next'],
                    method,
                    pages_to_fetch - 1
                ) if pages_to_fetch > 1 else None
                if next:
                    responses.extend(next)

            return responses
        except requests.exceptions.RequestException as e:
            print(f"Error fetching next page for method {method}: {e}")
            return {}
    
    def fetch(self, query: Union[str, dict, list], *, method: str = "activity", **kwargs):
        """
        Fetch data from the ChEMBL API.
        Args:
            query (str): Query string to search for.
            **kwargs: Additional parameters for the request.
            - `method`: Method to use for the request. Default is "compound".
        Returns:
            any: response from the API.
        """
        format = kwargs.get("format", "json")
        pages_to_fetch = kwargs.get("pages_to_fetch", 1)

        if method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")

        if format not in METHODS_FORMATS.get(method, []):
            raise ValueError(f"Format {format} is not supported for method {method}. Supported formats are: {', '.join(METHODS_FORMATS.get(method, []))}.")


        if not isinstance(query, (str, dict)):
            raise ValueError("Query must be a string or a dictionary.")
        
        
        if isinstance(query, dict):
            self.validate_query(method, query)
            # Convert dictionary to a query string
            query = "&".join(f"{key}={value}" for key, value in query.items())
        elif isinstance(query, str) and re.match(r"^CHEMBL\d+$", query):
            # If query is a string and matches the ChEMBL ID pattern, use it directly
            query = f"target_chembl_id={query}"

        # Generate url
        url = f"{CHEMBL.API_URL}{method}?{query}"

        url += f"&format={format}"

        return self.fetch_pages(url, method, pages_to_fetch)

    def parse(
            self,
            data: Any,
            fields_to_extract: Optional[Union[list, dict]],
            **kwargs
    ) -> Union[Dict, List]:
        """
        Parse the response from the ChEMBL API.
        Args:
            data (Any): Raw data from the API response.
            fields_to_extract (List|Dict): Fields to keep from the original response.
                - If List: Keep those keys.
                - If Dict: Maps {desired_name: real_field_name}.
        Returns:
            dict: Parsed response.
        """
        if not data:
            return {}

        if isinstance(data, requests.models.Response):
            data = data.json()
        elif isinstance(data, dict):
            data = data
        else:
            raise ValueError("Response must be a requests.Response object or a dictionary.")
        
        parsed = self._extract_fields(data, fields_to_extract)

        return parsed
    
    def query_usage(self) -> str:
        return (
            "ChEMBL API allows you to search for compounds, activities, and other chemical data.\n"
            "You can use methods like 'activity' and 'activity-search' to fetch data.\n"
            "For example, to search for activities, use:\n"
            "`fetch(query='CHEMBL1824', method='activity-search')`"
        )
    
    def get_dummy(self, *, method: Optional[str] = None, **kwargs) -> Dict:
        """Get dummy data for the ChEMBL API.
        Args:
            method (str): Method to use for the dummy data. Default is "activity-search".
        Returns:
            Dict: Dummy data with example fields.
        """
        query = {"target_chembl_id": "CHEMBL1824", "pchembl_value": 5.62}

        if method is None:
            method = "activity"
        if method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")
        
        return super().get_dummy(
            query=query,
            method=method,
            **kwargs
        )
