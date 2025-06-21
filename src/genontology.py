import os
from typing import Optional, Union, List, Dict, Any
import requests, random, time

import pandas as pd

from .base import BaseAPIInterface
from .constants import GENONTOLOGY
from .utils import get_nested

METHODS = {
    "ontology-term": ["", "graph"],
    "go": [""],
    "bioentity-function": [""]
}

class GenOntologyInterface(BaseAPIInterface):
    def __init__(
            self,
            cache_dir: Optional[str] = None,
            config_dir: Optional[str] = None,
            output_dir: Optional[str] = None,
            **kwargs
    ):
        """
        Initialize the GenOntologyInterface class.
        Args:
            cache_dir (str): Directory to cache results.
            config_dir (str): Directory for configuration files.
            output_dir (str): Directory to save output files.
        """
        if cache_dir:
            cache_dir = os.path.abspath(cache_dir)
        else:
            cache_dir = GENONTOLOGY.CACHE_DIR if GENONTOLOGY.CACHE_DIR is not None else ""

        if config_dir is None:
            config_dir = GENONTOLOGY.CONFIG_DIR if GENONTOLOGY.CONFIG_DIR is not None else ""

        super().__init__(cache_dir=cache_dir, config_dir=config_dir,**kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch(self, query: Union[str, dict, list], **kwargs):
        """
        Fetch data from the GenOntology API.
        Args:
            query (str): Query string to search for.
            **kwargs: Additional parameters for the request.
            - `method`: Method to use for the request. Used methods are 'ontology-term' and 'go'.
            - `option`: Additional options for the request. (currently not used)  
        Returns:
            any: response from the API.
        """
        method = kwargs.get("method", "ontology-term")  # Default method is 'ontology-term'
        option = kwargs.get("option", "")
    
        if not isinstance(query, str):
            raise ValueError("Query must be a string representing the ontology term (e.g., 'GO:0008150').")

        if method and method not in METHODS.keys():
                raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS.keys())}.")
        if option and option not in METHODS.get(method, []):
            raise ValueError(f"Option {option} is not supported for method {method}. Supported options are: {', '.join(METHODS.get(method, []))}.")

        url = f"{GENONTOLOGY.API_URL}{method.replace('-', '/')}/{query.upper().replace(':', '%3A')}" # Replace ':' with '%3A' for URL encoding

        if option:
            url += f"/{option}"

        try:
            response = self.session.get(url)
            self._delay()
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {query} with method {method}: {e}")
            return {}

    def parse(
            self,
            data: Any,
            fields_to_extract: Optional[Union[list, dict]],
            **kwargs
    ) -> Union[Dict, List]:
        """
        Parse the response from the GenOntology API.
        Args:
            data (Any): Raw data from the API response.
            fields_to_extract (List|Dict): Fields to keep from the original response.
                - If List: Keep those keys.
                - If Dict: Maps {desired_name: real_field_name}.
            **kwargs: Additional parameters for parsing.
            - `look_for_relationships`: If True, fetch related ontology terms.
        Returns:
            dict: Parsed response.
        """
        look_for_relationships = kwargs.get("look_for_relationships")
        if not data:
            return {}

        if isinstance(data, requests.models.Response):
            data = data.json()
        elif isinstance(data, dict):
            data = data
        else:
            raise ValueError("Response must be a requests.Response object or a dictionary.")
        
        parsed = self._extract_fields(data, fields_to_extract)
        # parsed = {}

        # if fields_to_extract is None:
        #     parsed = get_nested(data, "")

        # elif isinstance(fields_to_extract, list):
        #     for key in fields_to_extract:
        #         parsed[key] = get_nested(data, key)

        # elif isinstance(fields_to_extract, dict):
        #     for new_key, nested_path in fields_to_extract.items():
        #         parsed[new_key] = get_nested(data, nested_path)
        # else:
        #     raise ValueError("fields_to_extract must be a list or a dictionary.")

        # Get related gen ontology terms if requested
        if look_for_relationships:
            if isinstance(parsed, list):
                parsed = [self.fetch_related_ontology_terms(item) for item in parsed]
            else:
                parsed =  self.fetch_related_ontology_terms(parsed)

        return parsed

    def fetch_related_ontology_terms(self, parsed: Dict) -> Dict:
        """
        Fetch related ontology terms for a given ontology term.
        Args:
            parsed (dict): Parsed ontology term data.
        Returns:
            dict: Updated parsed data with relationships.
        """
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
    
    def get_dummy(self, *, method: Optional[str] = None, **kwargs) -> Dict:
        """
        Get example data returned by the API.

        Args:
            method (str, optional): Specific API method to test (e.g., "ontology-term", "subgraph").
                If None, returns dummy data for all available methods.

        Returns:
            dict: A dictionary where each key is a method name and each value is example data.
        """
        dummy_results = {}

        query = "GO:0008150"

        if method:
            dummy_results = super().get_dummy(query=query, method=method, **kwargs)
        else:
            for method in METHODS.keys():
                for option in METHODS[method]:
                        dummy_results[
                            f"{method}_{option}" if option else method
                        ] = super().get_dummy(query=query, method=method, option=option, **kwargs)

        return dummy_results 
    
    def query_usage(self) -> str:
        return (
            "GenOntology API allows you to fetch ontology terms and their relationships.\n"
            "Available methods:\n"
            "- ontology-term: Fetch ontology term details.\n"
            "- go: Fetch Gene Ontology hierarchy or models.\n"
            "Options for 'ontology-term': graph, subgraph.\n"
            "Options for 'go': hierarchy, models.\n"
            "Example usage:\n"
            "  - Fetch ontology term: fetch_single('GO:0008150', method='ontology-term')\n"
            "  - Fetch GO hierarchy: fetch_single('GO:0008150', method='go', option='hierarchy')\n"
        )