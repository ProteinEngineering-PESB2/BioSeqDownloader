import requests, time, random, re, os
from typing import Optional, Union, List, Dict, Any
import pandas as pd

from .base import BaseAPIInterface
from .constants import KEGG
from .utils import get_nested

# More info about KEGG API: https://www.kegg.jp/kegg/rest/keggapi.html

METHODS = [
    "info", "list", "find", "get", "conv", "link", "ddi"
]

DATABASES = [
    "pathway", "brite", "module", "ko", "vg", "vp", "ag", "genome",
    "compound", "glycan", "reaction", "rclass", "enzyme", "network",
    "variant", "disease", "drug", "dgroup", "disease_ja", "dgroup_ja",
    "compound_ja", "genes", "ligand", "kegg"
]

OUTSIDE_DBS = [
    "pubmed", "ncbi-geneid", "ncbi-proteinid", "uniprot",
    "pubchem", "chebi", "atc", "jtc", "ndc", "yj", "yk"
]

METHOD_OPTIONS = {
    "find": ["formula", "exact_mass", "mol_weight", "nop"],
    "get": ["aaseq", "ntseq", "mol", "kcf", "image", "conf", "kml", "json"],
    "link": ["turtle", "n-triple"]
}

class KEGGInterface(BaseAPIInterface):
    def __init__(
            self,
            fields_to_extract: Optional[Union[List, Dict]] = None,
            output_dir: Optional[str] = None,
            **kwargs
            ):
        """
        Initialize the KEGGInterface class.
        Args:
            min_wait (int): Minimum wait time between requests.
            max_wait (int): Maximum wait time between requests.
            fields_to_extract (list or dict): Fields to extract from the response.
        """
        cache_dir = KEGG.CACHE_DIR if KEGG.CACHE_DIR is not None else ""
        super().__init__(cache_dir=cache_dir, **kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.fields_to_extract = fields_to_extract

    def fetch(
            self,
            method: str,
            database: Optional[str] = None,
            query: Optional[Union[str, List[str]]] = None,
            option: Optional[str] = None,
    ):
        """
        Fetch data from the KEGG API.
        Args:
            method (str): Method to use for the request.
            database (str): Database to query.
            query (str or list): Query string or list of strings to search for.
            option (str): Additional options for the request.
        Returns:
            dict: Parsed response from the API.
        """
        if method not in METHODS:
            raise ValueError(f"Method {method} is not supported. Supported methods are: {', '.join(METHODS)}.")

        url = f"{KEGG.API_URL}{method}"

        if database:
            url += f"/{database}"
        if query:
            if isinstance(query, list):
                query = "+".join(query)
            else:
                query = str(query)
            if not isinstance(query, str):
                raise ValueError("Query must be a string or a list of strings.")
            
            url += f"/{query}"
        if option:
            if method not in METHOD_OPTIONS or option not in METHOD_OPTIONS[method]:
                raise ValueError(f"Option {option} is not supported for method {method}. Supported options are: {', '.join(METHOD_OPTIONS.get(method, []))}.")
            url += f"/{option}"

        try:
            response = requests.get(url)
            time.sleep(random.uniform(self.min_wait, self.max_wait))
            if response.status_code == 200:
                return response
            else:
                print(f"Failed to fetch data from KEGG API, code {response.status_code}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return {}
    
    def parse(
            self,
            response: Any,
            type_response: str = "table",
            columns: Optional[List[str]] = None,
            delimiter: Optional[str] = "\t",
            header: Optional[bool] = True,
    ):
        """
        Parse the response from the KEGG API.
        Args:
            response (any): Response from the API.
            type_response (str): Type of data to parse. It can be "table" or "entry".
            columns (list): List of column names to use for parsing.
            delimiter (str): Delimiter used in the response. Default is tab ("\t").
            header (bool): Whether the first line contains headers. Default is True.
        Returns:
            list: Parsed data as a list of dictionaries.
        """
        if not response or not hasattr(response, 'text'):
            return {}
        if type_response not in ["table", "entry"]:
            raise ValueError("Type must be either 'table' or 'entry'.")
        
        if type_response == "table":
            data = response.text.strip().split("\n")
            if not data:
                return {}
            parsed_data = []
            if columns:
                headers = columns
            else:
                headers = data[0].split(delimiter)
            
            if header:
                data = data[1:]

            for line in data:
                values = line.split(delimiter)
                if len(values) != len(headers):
                    print(f"Warning: Line '{line}' does not match header length. Skipping.")
                    continue
                entry = {headers[i]: values[i] for i in range(len(headers))}
                parsed_data.append(entry)
            return parsed_data
        else:
            data = response.text.strip().split("///")[:-1]  # Split entries by "///" and remove the last empty entry

            parsed_data = []
            key_val_pattern = re.compile(r"^(\w+)(?:\s{2,}|\t+)(.+)$")

            for entry in data:
                parsed_entry = {}
                filtered_entry = {}
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
                
                
                # Parse the response based on fields_to_extract     
                if self.fields_to_extract is None:
                    filtered_entry = get_nested(parsed_entry, "")

                elif isinstance(self.fields_to_extract, list):
                    for key in self.fields_to_extract:
                        filtered_entry[key] = get_nested(parsed_entry, key)

                elif isinstance(self.fields_to_extract, dict):
                    for new_key, nested_path in self.fields_to_extract.items():
                        filtered_entry[new_key] = get_nested(parsed_entry, nested_path)

                parsed_data.append(filtered_entry)
            
            
            return parsed_data
    
    def fetch_to_dataframe(
            self,
            method: str,
            database: Optional[str] = None,
            query: Optional[Union[str, List[str]]] = None,
            option: Optional[str] = None,
            columns: Optional[List[str]] = None,
            delimiter: Optional[str] = "\t",
            header: Optional[bool] = False,
            batch_size: Optional[int] = 10
    ) -> pd.DataFrame:
        """
        Fetch data from the KEGG API and parse it into a DataFrame.
        Args:
            method (str): Method to use for the request.
        Returns:
            list: Parsed data as a list of dictionaries.
        """
        export_df = pd.DataFrame()

        # Separate batches
        if isinstance(query, list):
            if batch_size is None:
                batch_size = len(query)
            batches = [query[i:i + batch_size] for i in range(0, len(query), batch_size)]

        for batch in batches:
            response = self.fetch(method, database, batch, option)

            if method == "get":
                type_response = "entry"
            else:
                type_response = "table"

            export_df = pd.concat(
                [
                    export_df, 
                    pd.DataFrame(self.parse(response, type_response, columns, delimiter, header))
                ]
            )
        
        return export_df.reset_index(drop=True)
            