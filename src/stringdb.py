import requests, time, random
from typing import Optional, Union, Any
import pandas as pd

from .utils import get_nested
from .constants import STRING



## More info about SRING API: https://string-db.org/cgi/help

METHODS = [
    "get_string_ids",       "network",      "get_link", 
    "interaction_partners", "homology",     "homology_best", 
    "enrichment",           "ppi_enrichment", "valueranks_enrichment_submit"
]

METHOD_FORMATS = {
    "get_string_ids": ["json", "tsv", "tsv-no-header", "xml"],
    "network": ["image", "highres_image", "svg"],
    "get_link": ["json", "tsv", "tsv-no-header", "xml"],
    "interaction_partners": ["json", "tsv", "tsv-no-header", "xml", "psi-mi", "psi-mi-lab"],
    "homology": ["tsv", "tsv-no-header", "json", "xml"],
    "homology_best": ["json", "tsv", "tsv-no-header", "xml"],
}

METHOD_PARAMS = {
    "get_string_ids": ["identifiers", "echo_query", "species", "caller_identity"],
    "network": ["identifiers", "species", "add_color_nodes", "add_white_nodes", "required_score", "network_type", "caller_identify"],
    "get_link": ["identifiers", "species", "add_color_nodes", "add_white_nodes", "required_score", "network_flavor", "network_type", "hide_node_labels", "hide_disconnected_nodes", "show_query_node_labels", "block_structure_pics_in_bubbles","caller_identify"],
    "interaction_partners": ["identifiers", "species", "limit", "required_score", "network_type", "caller_identity"],
    "homology": ["identifiers", "species", "caller_identity"],
    "homology_best": ["identifiers", "species", "species_b", "caller_identity"],
}

class StringInterface():
    def __init__(
            self, 
            max_workers: int = 2, 
            min_wait: int = 1,
            max_wait: int = 2,
            fields_to_extract: Optional[Union[list, dict]] = None,
    ):
        """
        Initialize the StringInterface class.
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
            outfmt: str = "json", 
            method: str = "get_string_ids",
            params: dict = None,
    ):
        """
        Fetch data from the STRING API.
        Args:
            outfmt (str): Output format for the response.
            method (str): Method to use for the request.
            params (dict): Parameters for the request.
        Returns:
            dict: Parsed response from the API.
        """
        if method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")
        
        if outfmt not in METHOD_FORMATS[method]:
            raise ValueError(f"Output format {outfmt} is not supported for method {method}. Supported formats are: {', '.join(METHOD_FORMATS[method])}.")
        
        if params is None:
            params = {}
        
        url = f"{STRING.API_URL}{outfmt}/{method}?"

        for key, value in params.items():
            if key not in METHOD_PARAMS[method]:
                raise ValueError(f"Parameter {key} is not supported for method {method}. Supported parameters are: {', '.join(METHOD_PARAMS[method])}.")
            if key == "identifiers":
                if isinstance(value, list):
                    identifiers = "%0d".join(value)
                    url += f"{key}={identifiers}&"
                elif not isinstance(value, str):
                    raise ValueError(f"Parameter {key} must be a string or a list of strings.")
            else:
                url += f"{key}={value}&"
        
        if url.endswith("&"):
            url = url[:-1]
        

        try:
            response = requests.get(url)
            time.sleep(random.uniform(self.min_wait, self.max_wait))
            if response.status_code == 200:
                return response
            else:
                print(f"Failed to fetch pathway data, code {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None

    def parse(self, response: Any, fmt: str = "json"):
        """
        Parse the response from the STRING API.
        Args:
            response (any): Response from the API.
            fmt (str): Format of the response.
        Returns:
            dict: Parsed response.
        """
        if not response:
            return None
        if fmt == "json":
            parsed = {}
            # Determine which fields to include 
            if self.fields_to_extract is None:
                parsed = get_nested(response.json(), "")

            elif isinstance(self.fields_to_extract, list):
                for key in self.fields_to_extract:
                    parsed[key] = get_nested(response.json(), key)

            elif isinstance(self.fields_to_extract, dict):
                for new_key, nested_path in self.fields_to_extract.items():
                    parsed[new_key] = get_nested(response.json(), nested_path)
            
            return parsed

        elif fmt == "tsv":
            return response.text
        elif fmt == "image":
            print("Image format is not supported for parsing. Please use the method save_image() to save the image.")
        else:
            raise ValueError(f"Format {fmt} is not supported. Supported formats are: json, tsv")
        
    def save_image(self, response: Any, filename: str):
        """
        Save the image response from the STRING API.
        Args:
            response (any): Response from the API.
            filename (str): Name of the file to save the image.
        """
        if not filename.endswith(".png"):
            filename += ".png"
        
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"Image saved as {filename}")

    def fetch_to_dataframe(
            self, 
            method: str = "get_string_ids", 
            outfmt: str = "json",
            params: dict = None,
    ):
        """
        Fetch data from the STRING API and return it as a DataFrame.
        Args:
            identifiers (list): List of identifiers to fetch.
            method (str): Method to use for the request.
            outfmt (str): Output format for the response.
            params (dict): Parameters for the request.
        Returns:
            pd.DataFrame: DataFrame containing the fetched data.
        """
        response = self.fetch(outfmt=outfmt, method=method, params=params)
        parsed_response = self.parse(response, fmt=outfmt)
        return pd.DataFrame(parsed_response) if parsed_response else None
