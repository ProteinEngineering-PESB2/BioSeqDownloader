import os
from typing import Optional, Union, List, Dict, Any
import requests

import pandas as pd

from .base import BaseAPIInterface
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
    "taxId" : (None, "string", "Taxonomy ID of the organism to filter results. If not provided, all organisms will be included."),
    "searchIds" : (False, "boolean", "If ‘true’, the interactor ENTREZ_GENE, ORDERED LOCUS and SYSTEMATIC_NAME (orf) will be examined for a match with the geneList."),
    "format" : ("tab2", "string", "Format of the response. Options are 'tab1','tab2', 'extendedTab2', 'count', 'json', 'jsonExtended'. Default is 'tab2'."),
}


class BioGRIDInterface(BaseAPIInterface):
    def __init__(
            self,
            fields_to_extract: Optional[Union[List, Dict]] = None,
            output_dir: Optional[str] = None,
            **kwargs
    ):
        """
        Initialize the BioGRIDInterface class.
        Args:
            fields_to_extract (list or dict): Fields to extract from the response.
        """
        
        cache_dir = BIOGRID.CACHE_DIR if BIOGRID.CACHE_DIR is not None else ""
        super().__init__(cache_dir=cache_dir, **kwargs)
        self.fields_to_extract = fields_to_extract
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def fetch(
            self,
            query: Union[str, tuple, dict],
            **kwargs
    ):
        """
        Fetch data from the BioGRID API.
        Args:
            query (str): Query string to search for.
            **kwargs: Additional parameters for the request.
            - `method`: Method to use for the request. Used methods are
            'interactions', 'organisms', 'identifiers', 'evidence'.
        Returns:
            any: response from the API.
        """
        method = kwargs.get("method")

        if method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")
        if not isinstance(query, dict):
            raise ValueError("Query must be a dictionary with keys matching the query parameters.")

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
        
        try:
            response = self.session.get(url)
            self._delay()
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching prediction for {query}: {e}")
            return {}

    def parse(
            self,
            raw_data: Any,
            **kwargs
    ):
        """
        Parse the response from the BioGRID API.
        Args:
            response (requests.Response): Response object from the API.
        Returns:
            any: Parsed data from the response.
        """
        if not raw_data:
            return {}

        if isinstance(raw_data, requests.models.Response):
            raw_data = raw_data.json()
        elif isinstance(raw_data, dict):
            raw_data = raw_data
        else:
            raise ValueError("Response must be a requests.Response object or a dictionary.")
        
        parsed_list = []

        for _, value in raw_data.items():
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
    
    def get_dummy(self, access_key: str = "", method: str = "", **kwargs) -> Dict:
        """
        Get a dummy response.
        Useful for knowing the structure of the data returned by the API.
        Args:
            access_key (str): Your BioGRID access key.
        Returns:
            Dict: Dummy response with example fields.
        """
        if not access_key:
            raise ValueError("Access key must be provided to get dummy data.")
        if method != "" and method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")
        dummy_results = {}
        query = {
            "accessKey": access_key,
            "geneList": ["cdc27", "apc1", "apc2"],
            "taxId": "559292",
            'max': 1,
            "format": "json"
        }

        if method:
            dummy_results = super().get_dummy(
                query=query,
                method=method,
                parse=True
            )
        else:
            for method in METHODS:
                dummy_results[method] = super().get_dummy(
                    query=query,
                    method=method,
                    parse=True
                )  
        return dummy_results

        
    def query_usage(self):
        """
        Get usage information for the BioGRID API query parameters.
        Returns:
            str: Usage information.
        """

        usage = """Usage: To fetch interactions, use the BioGRID API with the following parameters.
        Example:
            - fetch_single(method="interactions", query={})
        Available methods: """ + ", ".join(METHODS) + "\n\n"
        usage += "Query Parameters:\n"
        usage += "\n".join([f"\t{key}: {value[0]} ({value[1]}) - {value[2]}" for key, value in query_params_base.items() if value[0] is not None])
        usage += "\n\nExample Query:\n"
        usage += """
        {
            "accessKey": "YOUR_ACCESS_KEY",
            "geneList": ["P53"],
            "max": 10,
            "format": "json"
        }
        """
        usage += "\n\nResponse Format:\n"
        usage += """
        {
            "BIOGRID_INTERACTION_ID": "int",
            "ENTREZ_GENE_A": "str",
            "ENTREZ_GENE_B": "str",
            "BIOGRID_ID_A": "int",
            "BIOGRID_ID_B": "int",
            "SYSTEMATIC_NAME_A": "str",
            "SYSTEMATIC_NAME_B": "str",
            "OFFICIAL_SYMBOL_A": "str",
            "OFFICIAL_SYMBOL_B": "str",
            "SYNONYMS_A": "str",
            "SYNONYMS_B": "str",
            "EXPERIMENTAL_SYSTEM": "str",
            "EXPERIMENTAL_SYSTEM_TYPE": "str",
            "PUBMED_AUTHOR": "str",
            "PUBMED_ID": "int",
            "ORGANISM_A": "int",
            "ORGANISM_B": "int",
            "THROUGHPUT": "str",
            "QUANTITATION": "str",
            "MODIFICATION": "str",
            "ONTOLOGY_TERMS": {},
            "QUALIFICATIONS": "",
            "TAGS": "",
            "SOURCEDB": ""
        }"""

        return usage