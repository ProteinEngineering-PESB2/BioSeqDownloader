import requests, os, json
from typing import Union, List, Dict, Optional

import numpy as np
import pandas as pd

from .base import BaseAPIInterface
from .constants import ALPHAFOLD
from .utils import get_nested

# TODO Incluir un outputfmt
# TODO Test get_dummy

class AlphafoldInterface(BaseAPIInterface):
    def __init__(
            self,  
            structures: List[str] = ["pdb"],
            fields_to_extract: Optional[Union[List, Dict]] = None,
            output_dir: Optional[str] = "",
            **kwargs
        ):
        """
        Initialize the AlphafoldInterface.
        Args:
            structures (List[str]): List of structures extensions to download. Available options are ["pdb", "cif", "bcif"] or none.
            self.fields_to_extract (List|Dict): Fields to keep from the original response.
                - If List: Keep those keys.
                - If Dict: Maps {desired_name: real_field_name}.
            for more information, see: https://alphafold.ebi.ac.uk/#/public-api/get_predictions_api_prediction__qualifier__get
            output_dir (str): Directory to save downloaded files. If None, defaults to the cache directory.
        """
        cache_dir = ALPHAFOLD.CACHE_DIR if ALPHAFOLD.CACHE_DIR is not None else ""
        super().__init__(cache_dir=cache_dir, **kwargs)
        self.structures = structures
        self.fields_to_extract = fields_to_extract
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_single(self, query, parse: bool = False):
        result = super().fetch_single(query, parse=parse)

        if self.structures:
            if isinstance(result, list):
                for res in result:
                    self.download_structures(res)
            elif isinstance(result, dict):
                self.download_structures(result)

        return result
    
    def fetch_batch(self, queries: List[Union[str, tuple, dict]], parse: bool = False) -> List:
        results = super().fetch_batch(queries, parse=parse)

        if self.structures:
            for result in results:
                if isinstance(result, list):
                    for res in result:
                        self.download_structures(res)
                elif isinstance(result, dict):
                    self.download_structures(result)
        return results


    def fetch(
            self, 
            query: Union[str, tuple, dict],
            **kwargs
        ) -> Dict:
        """
        Get prediction for a given UniProt ID.
        Args:
            query (str): UniProt ID to fetch prediction for.
        Returns:
            Dict: Prediction data.
        """
        if not isinstance(query, str):
            raise ValueError("Query must be a string representing a UniProt ID.")
        
        url = f"{ALPHAFOLD.API_URL}{query}"
        
        try:
            response = self.session.get(url)
            self._delay()
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching prediction for {query}: {e}")
            return {}

    def download_structures(self, parsed: Dict):
        """
        Download structure files based on parsed prediction info.

        Args:
            parsed (Dict): Parsed data containing URLs for structures.
        """
        if not self.structures:
            return

        for ext in self.structures:
            url_key = f"{ext}Url"
            if url_key not in parsed:
                print(f"Warning: {url_key} not found in parsed data. {parsed}")
                continue
            
            structure_url = parsed[url_key]
            file_name = structure_url.split("/")[-1]
            file_path = os.path.join(self.output_dir, file_name)

            # Check if the file already exists
            if os.path.exists(file_path):
                continue

            try:
                response = self.session.get(structure_url)
                with open(file_path, "wb") as f:
                    f.write(response.content)
            except Exception as e:
                print(f"Error downloading structure {file_name}: {e}")

    def parse(
            self, 
            raw_data: Union[List, Dict],
            **kwargs
        ) -> Union[List, Dict]:
        """
        Parse data by extracting specified fields or returning the entire structure.
        Args:
            data (Union[List, Dict]): Data to parse.
        Returns:
            Union[List, Dict]: Parsed data with specified fields or the entire structure.
        """
        
        # Check input data type
        if not isinstance(raw_data, (list, dict)):
            raise ValueError("Data must be a list or a dictionary.")

        parsed = {}
        if self.fields_to_extract is None:
            parsed = get_nested(raw_data, "")

        elif isinstance(self.fields_to_extract, list):
            parsed = {key: get_nested(raw_data, key) for key in self.fields_to_extract}

        elif isinstance(self.fields_to_extract, dict):
            parsed = {new_key: get_nested(raw_data, path) for new_key, path in self.fields_to_extract.items()}
        
        return parsed
    
    def get_dummy(self) -> dict:
        """
        Get a dummy response.
        Useful for knowing the structure of the data returned by the API.
        Returns:
            Dict: Dummy response with example fields.
        """
        return super().get_dummy(
            query="P02666",
            parse=False
        )
    
    def query_usage(self):
        """
        Get usage information for the Alphafold API.
        Returns:
            str: Usage information.
        """
        usage = """Usage: To fetch predictions, use the UniProt ID as the query.
        Example: 
            - fetch_single("P02666")
            - fetch_batch(["P02666", "P12345"])

        Also you can download structures by setting the `structures` parameter in the constructor.
        Example:
            - alphafold = AlphafoldInterface(structures=["pdb", "cif"])
            - prediction = alphafold.fetch_single("P02666")

        Available structures to download:
            - pdb: Protein Data Bank format
            - cif: Crystallographic Information File format
            - bcif: Binary Crystallographic Information File format
        """
        dummy = self.get_dummy()

        usage += "\n\nExample fields in the response:\n"
        for key in dummy.keys():
            usage += f"\t- {key}: {dummy[key]}\n"
        return usage
    
    def save(self, data: Union[List, Dict], filename: str, extension: str = "csv"):
        """
        Save the parsed data to a file.
        Args:
            data (Union[List, Dict]): Data to save.
            file_name (str): Name of the file to save the data to.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        if extension not in ["csv", "tsv", "json"]:
            raise ValueError("Unsupported file extension. Use 'csv', 'tsv', or 'json'.")
        
        if extension == "csv":
            df = pd.DataFrame(data)
            df.to_csv(os.path.join(self.output_dir, f"{filename}.{extension}"), index=False)
        
        elif extension == "tsv":
            df = pd.DataFrame(data)
            df.to_csv(os.path.join(self.output_dir, f"{filename}.{extension}"), sep="\t", index=False)
           
        elif extension == "json":
            with open(os.path.join(self.output_dir, f"{filename}.{extension}"), 'w') as f:
                json.dump(data, f, indent=4)
            
        return os.path.join(self.output_dir, f"{filename}.{extension}")

