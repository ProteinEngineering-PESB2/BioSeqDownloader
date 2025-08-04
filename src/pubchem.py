import os

from typing import Union, List, Dict, Set, Optional
from requests import Request, Response
from requests.exceptions import RequestException

import pandas as pd

from .base import BaseAPIInterface
# Add the import for your database in constants
from .constants import PUBCHEM

from .utils import validate_parameters

# pwaccs: Pathway Accession Codes
# Falta Pwaccs de gene
OPTIONS = {
    "protein": ["summary", "aids", "concise", "pwaccs"],
    "compound": [ "record", "synonyms", "sids", "cids", "aids", "assaysummary", "description"],
    "gene": ["summary","aids","concise","pwaccs"]
}

class PubChemInterface(BaseAPIInterface):
    METHODS = {
        "compound": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "cid": (str, None, True),
                "name": (str, None, True),
                "smiles": (str, None, True),
                "property": (str, None, True),
            },
            "group_queries": ["cid", "property"],
            "separator": ","
        },
        "protein": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "accession": (str, None, True),
            },
            "group_queries": [None],
            "separator": None 
        },
        "gene": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "genesymbol": (str, None, True),
                "geneid": (str, None, True),
                "synonym": (str, None, True),
                "taxid": (str, None, True),
            },
            "group_queries": ["genesymbol"],
            "separator": ","
        }
}

    def __init__(
            self,  
            cache_dir: Optional[str] = None,
            config_dir: Optional[str] = None,
            output_dir: Optional[str] = None,
            **kwargs
        ):

        if cache_dir:
            cache_dir = os.path.abspath(cache_dir)
        else:
            cache_dir = PUBCHEM.CACHE_DIR if PUBCHEM.CACHE_DIR is not None else ""

        if config_dir is None:
            config_dir = PUBCHEM.CONFIG_DIR if PUBCHEM.CONFIG_DIR is not None else ""

        super().__init__(cache_dir=cache_dir, config_dir=config_dir, **kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)


    def fetch(
            self, 
            query: Union[str, dict, list], 
            *, 
            method: str = "DEFAULT", 
            **kwargs
        ):
        option = kwargs.get("option", None)
        if method not in self.METHODS:
            raise ValueError(f"Method {method} is not supported. Available methods: {list(self.METHODS.keys())}")
        if option and option not in OPTIONS.get(method, []):
            raise ValueError(f"Option '{option}' is not valid for method '{method}'. Allowed options: {OPTIONS.get(method, [])}")

        http_method, path_param, parameters, inputs = self.initialize_method_parameters(query, method, self.METHODS, **kwargs)

        if option and inputs.get("property"):
            raise ValueError("Cannot specify both 'option' and 'property' parameter. Please choose one.")

        # Validate and clean parameters
        try:
            validated_params = validate_parameters(inputs, parameters)
        except ValueError as e:
            raise ValueError(f"Invalid parameters for method '{method}': {e}")


        if method == "compound":
            if sum(bool(inputs.get(validated_params)) for validated_params in ["cid", "name", "smiles"]) != 1:
                raise ValueError("Only one 'cid', 'name', or 'smiles' parameters must be specified.")
            # if "property" in validated_params:
            #     for prop in  validated_params["property"].split(","):
            #         if prop not in PROPERTIES[method]:
            #             raise ValueError(f"Property '{prop}' is not valid for method '{method}'. Allowed properties: {PROPERTIES[method]}")
        elif method == "gene":
            if sum(bool(inputs.get(validated_params)) for validated_params in ["genesymbol", "geneid", "synonym"]) != 1:
                raise ValueError("Only one 'genesymbol', 'geneid', or 'synonym' parameters must be specified.")

        # if "specification" in validated_params and validated_params["specification"] not in PROPERTIES[method]:
        #     raise ValueError(f"Specification '{validated_params['specification']}' is not valid for method '{method}'. Allowed specifications: {PROPERTIES[method]}")

        url = f"{PUBCHEM.API_URL}{method}"

        for key, value in validated_params.items():
            url += f"/{key}/{value}"
        
        if option:
            url += f"/{option}"
        url += "/json"  # Assuming JSON output for simplicity

        response = Request(
            url=url,
            method=http_method
        )

        prepared = self.session.prepare_request(response)
        print(f"Prepared request: {prepared.url}")

        try:
            response = self.session.send(prepared)
            self._delay()
            response.raise_for_status()
            response = response.json() if response.headers.get('Content-Type') == 'application/json' else response.text

            # Me pertuba ver tantos elif
            if "PropertyTable" in response:
                response = response.get("PropertyTable", {}).get("Properties", [])
            elif "InformationList" in response:
                response = response.get("InformationList", {}).get("Information", [])
                if method == "gene" and option == "pwaccs":
                    # A little hack to force the response to have a "GeneSymbol" key
                    for r in response:
                        r["GeneSymbol"] = validated_params.get("genesymbol", [])

            elif "Table" in response:
                # Convert Table response to list of dicts with key:value pairs
                table = response.get("Table", {})
                columns = table.get("Columns", {}).get("Column", [])
                rows = table.get("Row", [])
                response = [
                    dict(zip(columns, row.get("Cell", [])))
                    for row in rows
                ]
            elif "PC_Compounds" in response:
                response = response.get("PC_Compounds", [])
            elif "IdentifierList" in response:
                response = response.get("IdentifierList", [])
            elif "ProteinSummaries" in response:
                response = response.get("ProteinSummaries", {}).get("ProteinSummary", [])
            elif "GeneSummaries" in response:
                response = response.get("GeneSummaries", {}).get("GeneSummary", [])
            else:
                response = response

            return response
        except RequestException as e:
            print(f"Error fetching {query} for method '{method}': {e}")
            return {}
    
    def parse(
            self, 
            data: Union[List, Dict],
            fields_to_extract: Optional[Union[list, dict]],
            **kwargs
        ) -> Union[List, Dict]:
        if not data:
            return {}
        option = kwargs.get("option", None)

        if option:
            if isinstance(fields_to_extract, dict) and option in fields_to_extract.keys():
                fields_to_extract = fields_to_extract.get(option, [])
            else:
                fields_to_extract = {}
        else:
            fields_to_extract = fields_to_extract.get("properties", []) 

        if isinstance(data, Response):
            data = data.json()
        elif isinstance(data, dict):
            data = data
        else:
            raise ValueError("Response must be a requests.Response object or a dictionary.")
        

        return self._extract_fields(data, fields_to_extract)
    
    def get_dummy(self, **kwargs) -> Dict:
        return {
            "message": "This is a dummy response.",
            "status": "success"
        }
    
    def query_usage(self) -> str:
        return """
        This is a dummy query usage for YourDatabaseInterface.
        """