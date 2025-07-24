import os
from typing import Optional, Set, Tuple, Union, List, Dict, Any
import requests

import pandas as pd

from .base import BaseAPIInterface
from .constants import BIOGRID
from .utils import get_nested

# Rest documentation: https://wiki.thebiogrid.org/doku.php/biogridrest

METHODS = [
    "interactions"
]

# TODO add more from docs
# TODO ISSUES:
# For some reason, running this query:
# query={
#     "accessKey": biogrid_api_key,
#     "geneList": ['1148170', '1148186', '112090'],
#     "searchBiogridIds" : True,
#     "format": "tab2"
# },
# gives an error:
# Error fetching data for {...}: Extra data: line 1 column 8 (char 7). Tried URL: https://webservice.thebiogrid.org/interactions?accessKey={ACCESS_KEY}&geneList=1148170|1148186|112090&searchBiogridIds=True&format=tab2
# This error will go to a low priority issue, as it is not as used as the JSON format.
query_params_base = {
    "accessKey": (None, "string", "Your BioGRID access key"),
    "id": (None, "string", "BioGRID interaction ID to filter results"),
    "start": (0, "integer", "Start index for pagination"),
    "max": (10000, "integer", "Maximum number of results to return"),
    "interSpeciesExclude": (False, "boolean", "Include interactions between different species"),
    "selfInteractionsExclude": (False, "boolean", "If ‘true’, interactions with one interactor will be excluded"),
    "evidenceList" : (None, "string", "Comma-separated list of evidence codes to filter results"),
    "includeEvidence" : (False, "boolean", "If ‘true’, evidence codes will be included in the results"),
    "geneList" : (None, "string", "List of gene names to filter results."),
    "searchBiogridIds" : (False, "boolean", "If ‘true’, the interactor BIOGRID_ID will be examined for a match with the geneList."),
    "taxId" : (None, "string", "Taxonomy ID of the organism to filter results. If not provided, all organisms will be included."),
    "searchIds" : (False, "boolean", "If ‘true’, the interactor ENTREZ_GENE, ORDERED LOCUS and SYSTEMATIC_NAME (orf) will be examined for a match with the geneList."),
    "format" : ("json", "string", "Format of the response. Options are 'tab1','tab2', 'extendedTab2', 'count', 'json', 'jsonExtended'. Default is 'json'."),
}


class BioGRIDInterface(BaseAPIInterface):

    def __init__(
            self,
            cache_dir: Optional[str] = None,
            config_dir: Optional[str] = None,
            output_dir: Optional[str] = None,
            **kwargs
    ):
        """
        Initialize the BioGRIDInterface class.
        Args:
            cache_dir (str): Directory to cache results.
            config_dir (str): Directory for configuration files.
            output_dir (str): Directory to save output files.
        """
        if cache_dir:
            cache_dir = os.path.abspath(cache_dir)
        else:
            cache_dir = BIOGRID.CACHE_DIR if BIOGRID.CACHE_DIR is not None else ""

        if config_dir is None:
            config_dir = BIOGRID.CONFIG_DIR if BIOGRID.CONFIG_DIR is not None else ""

        super().__init__(cache_dir=cache_dir, config_dir=config_dir, **kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)

    # Critiacl to ignore the accessKey when caching
    def get_cache_ignore_keys(self) -> Set[str]:
        """
        Get the keys to ignore when caching.
        Returns:
            Set[str]: Set of keys to ignore.
        """
        return super().get_cache_ignore_keys().union({"accessKey"})
    
    # Probably max is another key to use. If you want to cache the results with different max values, then you should not ignore it.
    def get_subquery_match_keys(self) -> Set[str]:
        return super().get_subquery_match_keys().union({"id", "geneList", "taxId"})

    
    def fetch(self, query: Union[str, dict, list], *, method: str = "interactions", **kwargs):
        """
        Fetch data from the BioGRID API.
        Args:
            query (str): Query string to search for.
            **kwargs: Additional parameters for the request.
            - `method`: Method to use for the request. Default is "interactions".
        Returns:
            any: response from the API.
        """
        #method = kwargs.get("method", "interactions")

        if method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")
        if not isinstance(query, dict):
            raise ValueError("Query must be a dictionary with keys matching the query parameters.")

        for key in query.keys():
            if key not in query_params_base:
                raise ValueError(f"Query parameter {key} is not supported. Supported parameters are: {', '.join(query_params_base.keys())}.")

        # Generate url
        url = f"{BIOGRID.API_URL}{method}"

        if "id" in query.keys():
            if isinstance(query["id"], str):
                url += f"/{query["id"]}"
            query.pop("id")  # Remove id from query to avoid duplication in URL
        
        url += "?"

        for key, value in query.items():
            if isinstance(value, List):
                url += f"{key}=" + "|".join(value)  # Convert list to pipe-separated string
            else:
                url += f"{key}={str(value)}"

            url += "&"

        if "format" not in query.keys():
            url += "format=json"
        
        try:
            response = self.session.get(url)
            self._delay()
            response.raise_for_status()

            r = response.json()

            # Special case for BioGRID
            if isinstance(r, dict) and all(str(key).isdigit() for key in r.keys()):
                # Convert to list of interactions
                r = list(r.values())

            return r
        except requests.exceptions.RequestException as e:
            # If message has 400 Client Error: Bad Request for url probably didn't found a given taxId or geneList
            if response.status_code == 400:
                error_message = response.json().get("message", "Unknown error")
                print(f"Gene or Taxonomy ID not found for {query}.")
            else:
                print(f"Error fetching data for {query}: {e}. Tried URL: {url}")
            return {}

    def parse(
            self,
            data: Any,
            fields_to_extract: Optional[Union[list, dict]],
            **kwargs
    ) -> Union[Dict, List]:
        """
        Parse the response from the BioGRID API.
        Args:
            data (dict): The fetched data.
            fields_to_extract (List|Dict): Fields to keep from the original response.
                - If List: Keep those keys.
                - If Dict: Maps {desired_name: real_field_name}.
        Returns:
            any: Parsed data from the response.
        """
        if not data:
            return {}

        if isinstance(data, requests.models.Response):
            data = data.json()
        elif isinstance(data, (dict, list)):
            data = data
        else:
            raise ValueError("Response must be a requests.Response object, list or a dictionary.")

        # # Check if all keys are numbers (indicating a list of interactions)
        # if isinstance(data, dict) and all(str(key).isdigit() for key in data.keys()):
        #     # Convert to list of interactions
        #     data = list(data.values())

        return self._extract_fields(data, fields_to_extract)

    
    def get_dummy(self, access_key: str = "", method: str = "", **kwargs) -> Dict:
        """
        Get a dummy response.
        Useful for knowing the structure of the data returned by the API.
        Args:
            access_key (str): Your BioGRID access key.
        Returns:
            Dict: Dummy response with example fields.
        """
        parse = kwargs.get("parse", False)

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
                parse=parse
            )
        else:
            for method in METHODS:
                dummy_results[method] = super().get_dummy(
                    query=query,
                    method=method,
                    parse=parse
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