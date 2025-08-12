import os, requests
from typing import Optional, Set, Union, Any, List, Dict
import pandas as pd
from requests import Request
from requests.exceptions import RequestException

from .base import BaseAPIInterface
from .utils import get_nested, validate_parameters
from .constants import STRING


# TODO Finish this interface
## More info about STRING API: https://string-db.org/cgi/help

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

class StringInterface(BaseAPIInterface):
    METHODS = {
        "get_string_ids": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "identifiers": (str, None, True),
                "species": (int, None, False),
                "echo_query": (int, 0, False),
                "format": (str, "json", False),
            },
            "group_queries": ["identifiers", "species"],
            "separator": "%0d"
        },
        "interaction_partners": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "identifiers": (str, None, True),
                "species": (int, None, False),
                "limit": (int, None, False),
                "required_score": (int, None, False),
                "network_type": (str, "functional", False),
            },
            "group_queries": ["identifiers", "species"],
            "separator": "%0d"
        },
        # Add other methods as needed
    }
    def __init__(
            self,
            cache_dir: Optional[str] = None,
            config_dir: Optional[str] = None,
            output_dir: Optional[str] = None,
            **kwargs
    ):
        """
        Initialize the StringInterface class.
        Args:
            cache_dir (str): Directory to cache API responses. If None, defaults to the cache directory defined in constants.
            config_dir (str): Directory for configuration files. If None, defaults to the config directory defined in constants.
            output_dir (str): Directory to save downloaded files. If None, defaults to the cache directory.
        """
        if cache_dir:
            cache_dir = os.path.abspath(cache_dir)
        else:
            cache_dir = STRING.CACHE_DIR if STRING.CACHE_DIR is not None else ""

        if config_dir is None:
            config_dir = STRING.CONFIG_DIR if STRING.CONFIG_DIR is not None else ""

        super().__init__(cache_dir=cache_dir, config_dir=config_dir, **kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    # def get_subquery_match_keys(self) -> Set[str]:
    #     return super().get_subquery_match_keys().union({"identifiers", "species"})

    def fetch(self, query: Union[str, dict, list], *, method: str = "get_string_ids", **kwargs):
        """
        Fetch data from the STRING API.
        Args:
            query (str|dict|list): Query parameters for the API.
            method (str): Method to use for the request.
            outfmt (str): Output format for the response.
        Returns:
            dict: Parsed response from the API.
        """
        if method not in self.METHODS.keys():
            raise ValueError(f"Method {method} is not supported. Available methods: {list(self.METHODS.keys())}")

        http_method, path_param, parameters, inputs = self.initialize_method_parameters(query, method, self.METHODS, **kwargs)

        try:
            validated_params = validate_parameters(inputs, parameters)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Parameter validation failed: {e}")
        
        if "format" in validated_params:
            outfmt = validated_params.pop("format")
        else:
            outfmt = "json"

        if outfmt not in METHOD_FORMATS[method]:
            raise ValueError(f"Output format {outfmt} is not supported for method {method}. Supported formats are: {', '.join(METHOD_FORMATS[method])}.")
        
        url = f"{STRING.API_URL}{outfmt}/{method}"


        req = Request(
            method=http_method,
            url=url,
            params=validated_params
        )

        prepared = self.session.prepare_request(req)
        print(f"Prepared request URL: {prepared.url}")

        try:
            response = self.session.send(prepared)
            self._delay()
            response.raise_for_status()

            return response.json()
        except RequestException as e:
            print(f"Error fetching {query} for method '{method}': {e}")
            return {}

        # outfmt = kwargs.get("outfmt", "json")

        # if method not in METHODS:
        #     raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")
        
        # if outfmt not in METHOD_FORMATS[method]:
        #     raise ValueError(f"Output format {outfmt} is not supported for method {method}. Supported formats are: {', '.join(METHOD_FORMATS[method])}.")
        
        # if not isinstance(query, dict):
        #     raise ValueError("Query must be a dictionary containing parameters for the API request.")
        
        
        # url = f"{STRING.API_URL}{outfmt}/{method}?"

        # for key, value in query.items():
        #     if key not in METHOD_PARAMS[method]:
        #         raise ValueError(f"Parameter {key} is not supported for method {method}. Supported parameters are: {', '.join(METHOD_PARAMS[method])}.")
        #     if key == "identifiers":
        #         if isinstance(value, list):
        #             identifiers = "%0d".join(value)
        #             url += f"{key}={identifiers}&"
        #         elif not isinstance(value, str):
        #             raise ValueError(f"Parameter {key} must be a string or a list of strings.")
        #     else:
        #         url += f"{key}={value}&"
        
        # if url.endswith("&"):
        #     url = url[:-1]
        

        # try:
        #     response = self.session.get(url)
        #     self._delay()
        #     response.raise_for_status()
        #     return response.json()
        # except requests.exceptions.RequestException as e:
        #     print(f"Error fetching prediction for {query}: {e}")
        #     return {}
    
    def parse(
            self, 
            data: Any,
            fields_to_extract: Optional[Union[list, dict]],
            **kwargs
        ) -> Any:
        """
        Parse the response from the STRING API.
        Args:
            data (Any): Data to parse.
            fields_to_extract (List|Dict): Fields to keep from the original response.
                - If List: Keep those keys.
                - If Dict: Maps {desired_name: real_field_name}
            fmt (str): Format of the response.
        Returns:
            dict: Parsed response.
        """
        fmt = kwargs.get("fmt", "json")
        if not data:
            return {}
        
        if fmt == "json":
            return self._extract_fields(data, fields_to_extract)

        elif fmt == "tsv":
            return data.text
        elif fmt == "image":
            print("Image format is not supported for parsing. Please use the method save_image() to save the image.")
        else:
            raise ValueError(f"Format {fmt} is not supported. Supported formats are: json, tsv")
        
    def query_usage(self) -> str:
        return (
            "To query STRING, use the method name and parameters as a dictionary. "
            "Example usage:\n"
            "string_instance.fetch(query={'identifiers': ['p53', 'cdk2'], 'species': 9606}, method='get_string_ids', outfmt='json')\n"
            "Supported methods: " + ", ".join(METHODS) + "\n"
            "Supported output formats: " + ", ".join(METHOD_FORMATS['get_string_ids'])
        )
        
    # def save_image(self, response: Any, filename: str):
    #     """
    #     Save the image response from the STRING API.
    #     Args:
    #         response (any): Response from the API.
    #         filename (str): Name of the file to save the image.
    #     """
    #     if not filename.endswith(".png"):
    #         filename += ".png"
        
    #     with open(filename, "wb") as f:
    #         f.write(response.content)
    #     print(f"Image saved as {filename}")

    # def fetch_to_dataframe(
    #         self, 
    #         method: str = "get_string_ids", 
    #         outfmt: str = "json",
    #         params: dict = None,
    # ):
    #     """
    #     Fetch data from the STRING API and return it as a DataFrame.
    #     Args:
    #         identifiers (list): List of identifiers to fetch.
    #         method (str): Method to use for the request.
    #         outfmt (str): Output format for the response.
    #         params (dict): Parameters for the request.
    #     Returns:
    #         pd.DataFrame: DataFrame containing the fetched data.
    #     """
    #     response = self.fetch(outfmt=outfmt, method=method, params=params)
    #     parsed_response = self.parse(response, fmt=outfmt)
    #     return pd.DataFrame(parsed_response) if parsed_response else None
