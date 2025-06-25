import requests, time, random, re, os
from typing import Optional, Union, List, Dict, Any
import pandas as pd

from .base import BaseAPIInterface
from .constants import KEGG
from .utils import get_nested

# More info about KEGG API: https://www.kegg.jp/kegg/rest/keggapi.html
# TODO Solve known problem with KEGG API:
# For the queries that have more than one search like
# ["hsa:10458", "ece:Z5100"] It saves in cache a response for both entries
# But if you try to fetch only one of them, it saves another cache file.
# What it should do is to get the response from the cache file
# and return it without saving another cache file.

# TODO Should I make the method query for multiple entries or do one entry at a time?
# Doing multiple entries at a time is more efficient, but it requires more complex coding.

METHODS = [
    "info", "list", "find", "get", "conv", "link", "ddi"
]

DATABASES = [
    "pathway", "brite", "module", "genome",
    "compound", "glycan", "reaction", "enzyme", "network",
    "disease", "drug", "genes", "ligand", "kegg"
]

METHOD_OPTIONS = {
    "find": ["formula", "exact_mass", "mol_weight", "nop"],
    "get": ["aaseq", "ntseq", "mol", "kcf", "image", "conf", "kml", "json"],
    "link": ["turtle", "n-triple"]
}

class KEGGInterface(BaseAPIInterface):
    def __init__(
            self,
            cache_dir: Optional[str] = None,
            config_dir: Optional[str] = None,
            output_dir: Optional[str] = None,
            **kwargs
            ):
        """
        Initialize the KEGGInterface class.
        Args:
            fields_to_extract (list or dict): Fields to extract from the response.
            output_dir (str): Directory to save the output files. If None, uses the cache directory.
        """
        if cache_dir:
            cache_dir = os.path.abspath(cache_dir)
        else:
            cache_dir = KEGG.CACHE_DIR if KEGG.CACHE_DIR is not None else ""

        if config_dir is None:
            config_dir = KEGG.CONFIG_DIR if KEGG.CONFIG_DIR is not None else ""

        super().__init__(cache_dir=cache_dir, config_dir=config_dir, **kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)


    def validate_query(self, method: str, query: Dict):
        """
        Validate the query parameters.
        Args:
            method (str): The method to validate against.
            query (Union[str, tuple, dict]): The query parameters to validate.
        Raises:
            ValueError: If the query parameters are invalid.
        """
        rules = {
            'entries': lambda v: isinstance(v, (str, list)),
            'db': lambda v: v in DATABASES,
            #'todb' : lambda v: v in DATABASES,
            'option': lambda v: v in METHOD_OPTIONS.get(method, []),
        }

        for key, check in rules.items():
            if key in query and not check(query[key]):
                if key == 'entries':
                    raise ValueError(f"Invalid entries: {query['entries']}. Must be a string or a list of strings.")
                elif key == 'db':
                    raise ValueError(f"Invalid database type: {query['db']}. Valid types are: {', '.join(DATABASES)}.")
                elif key == 'option':
                    raise ValueError(f"Invalid option: {query['option']} for method {method}. Supported options are: {', '.join(METHOD_OPTIONS.get(method, []))}.")
    
    def fetch(self, query: Union[str, dict, list], *, method: str = "get", **kwargs):
        """
        Fetch data from the KEGG API.
        Args:
            query (str): Query string to search for.
            method (str): Method to use for the request. Used methods are
                'info', 'list', 'find', 'get', 'conv', 'link', 'ddi'.
            **kwargs: Additional parameters for the request.
            - `database`: Database to use for the request. Used databases are
                'pathway', 'brite', 'module', 'genome', 'compound',
                'glycan', 'reaction', 'enzyme', 'network', 'disease',
                'drug', 'genes', 'ligand', 'kegg'.
            - `option`: Additional options for the request. Used options are
                'aaseq', 'ntseq', 'mol', 'kcf', 'image', 'conf', 'kml', 'json'
                for method 'get' and 'turtle', 'n-triple' for method 'link'.
        Raises:
            ValueError: If the method or option is not supported.
        Returns:
            any: Response from the API.
        """

        if not method:
            raise ValueError("Method must be specified. Supported methods are: " + ", ".join(METHODS))
        if not isinstance(query, dict):
            raise ValueError("Query must be a dictionary with keys 'entries', 'db', and 'option'.")

        self.validate_query(method, query)

        if method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")   

        url = f"{KEGG.API_URL}{method}"

        if 'db' in query.keys() and query['db']:
            url += f"/{query['db']}"
        if 'entries' in query.keys() and query['entries']:
            if isinstance(query['entries'], list):
                q = "+".join(query['entries'])
            elif isinstance(query['entries'], str):
                q = str(query['entries'])
            else:
                raise ValueError("Query must be a string or a list of strings.")
            
            url += f"/{q}"
        if 'option' in query.keys() and query['option']:
            if method not in METHOD_OPTIONS or query['option'] not in METHOD_OPTIONS[method]:
                raise ValueError(f"Option {query['option']} is not supported for method {method}. Supported options are: {', '.join(METHOD_OPTIONS.get(method, []))}.")
            url += f"/{query['option']}"

        try:
            response = self.session.get(url)
            self._delay()
            response.raise_for_status()
            if not response or not hasattr(response, 'text'):
                print(f"Warning: No response or invalid response for query {query} with method {method}.")
                return {}
            return response.text # TODO check if for other functions we need to return json or text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {query} with method {method}: {e}")
            return {}
    
    def parse(
            self,
            data: Any,
            fields_to_extract: Optional[Union[List, Dict]] = None,
            **kwargs
    ) -> Union[Dict, List]:
        """
        Parse the response from the KEGG API.
        Args:
            data (Any): Raw data from the API response.
            fields_to_extract (list or dict): Fields to extract from the response.
                - If list: Keep those keys.
                - If dict: Maps {desired_name: real_field_name}.
                
            **kwargs: Additional parameters for parsing.
            - `type_response`: Type of data to parse. It can be "table" or "entry".
            - `columns`: List of column names to use for parsing.
            - `delimiter`: Delimiter used in the response. Default is tab ("\t").
            - `header`: Whether the first line contains headers. Default is True.
        Raises:
            ValueError: If the type_response is not supported.
        Returns:
            list: Parsed data as a list of dictionaries.
        """
        type_response = kwargs.get("type_response", "entry")
        columns = kwargs.get("columns", None)
        delimiter = kwargs.get("delimiter", "\t")
        header = kwargs.get("header", True)
        if not data:
            print("Warning: No data to parse.")
            return {}

        if type_response not in ["table", "entry"]:
            raise ValueError("Type must be either 'table' or 'entry'.")
        
        if type_response == "table":
            d = data.strip().split("\n")
            if not d:
                return {}
            parsed_data = []
            if columns:
                headers = columns
            else:
                headers = d[0].split(delimiter)
            
            if header:
                d = d[1:]

            for line in d:
                values = line.split(delimiter)
                if len(values) != len(headers):
                    print(f"Warning: Line '{line}' does not match header length. Skipping.")
                    continue
                entry = {headers[i]: values[i] for i in range(len(headers))}
                parsed_data.append(entry)
            return parsed_data
        else:
            d = data.strip().split("///")[:-1]  # Split entries by "///" and remove the last empty entry

            parsed_data = []
            key_val_pattern = re.compile(r"^(\w+)(?:\s{2,}|\t+)(.+)$")

            for entry in d:
                parsed_entry = {}
                current_key = None
                for line in entry.strip().split("\n"):
                    pattern_match = key_val_pattern.match(line)
                    if pattern_match:
                        key, value = pattern_match.groups()
                        current_key = key

                        if key not in parsed_entry:
                            parsed_entry[key] = value
                        else:
                            if isinstance(parsed_entry[key], list):
                                parsed_entry[key].append(value)
                            else:
                                parsed_entry[key] = [parsed_entry[key], value]
                    else:
                        continuation = line.strip()
                        if current_key is None:
                            continue
                        if isinstance(parsed_entry[current_key], list):
                            parsed_entry[current_key].append(continuation)
                        else:
                            parsed_entry[current_key] += ' ' + continuation

                # Special key values handling
                if 'AASEQ' in parsed_entry:
                    parsed_entry["AALEN"] = parsed_entry["AASEQ"].split(" ")[0]
                    parsed_entry["AASEQ"] = "".join(parsed_entry["AASEQ"].split(" ")[1:])
                
                if 'NTSEQ' in parsed_entry:
                    parsed_entry["NTLEN"] = parsed_entry["NTSEQ"].split(" ")[0]
                    parsed_entry["NTSEQ"] = "".join(parsed_entry["NTSEQ"].split(" ")[1:])
                
                parsed_data.append(self._extract_fields(
                    parsed_entry, fields_to_extract
                ))
            
            return parsed_data
        
    def get_dummy(self, *, method: Optional[str] = None, **kwargs) -> dict:
        """
        Get a dummy response for testing purposes.
        Args:
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        Returns:
            dict: Dummy response.
        """
        # TODO implement a more meaningful dummy response
        dummy_results = {}

        query = ""

        if method:
            dummy_results = super().get_dummy(query=query, method=method, **kwargs)
        else:
            for method in METHODS:
                dummy_results[method] = super().get_dummy(query=query, method=method, **kwargs)
        
        return dummy_results
        
    def query_usage(self) -> str:
        return (
            "KEGG API allows you to fetch data from KEGG databases.\n"
            "You can use methods like 'info', 'list', 'find', 'get', 'conv', 'link', and 'ddi'.\n"
            "Supported databases include 'pathway', 'brite', 'module', 'ko', 'vg', 'vp', 'ag', "
            "'genome', 'compound', 'glycan', 'reaction', 'rclass', 'enzyme', 'network', "
            "'variant', 'disease', 'drug', 'dgroup', and more.\n"
            "Example usage:\n"
            "    kegg = KEGGInterface()\n"
            "    response = kegg.fetch_single("
            "        query='hsa:10458', method='get', parse=True\n"
            "    )\n"
            "    print(response)\n"
            "For more information, visit: https://www.kegg.jp/kegg/rest/keggapi.html"
        )
    