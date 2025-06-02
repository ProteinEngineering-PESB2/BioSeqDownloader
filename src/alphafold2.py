import requests, os
from typing import Union, List, Dict, Optional

import pandas as pd

from .base import BaseAPIInterface
from .constants import ALPHAFOLD
from .utils import get_nested

# TODO output_dir should be a path to a directory relative to the running script no to src

# TODO Incluir un outputfmt

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
        super().__init__(cache_dir=ALPHAFOLD.CACHE_DIR, **kwargs)
        self.structures = structures
        self.fields_to_extract = fields_to_extract
        self.output_dir = output_dir or ALPHAFOLD.CACHE_DIR
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


    def fetch(self, uniprot_id) -> Dict:
        """
        Get prediction for a given UniProt ID.
        Args:
            uniprot_id (str): UniProt ID to fetch prediction for.
        Returns:
            Dict: Prediction data.
        """
        url = f"{ALPHAFOLD.API_URL}{uniprot_id}"
        
        try:
            response = self.session.get(url)
            self._delay()
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching prediction for {uniprot_id}: {e}")
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
    
    def parse(self, prediction: Dict) -> Dict:
        """
        Extract fields from prediction JSON.
        """
        parsed = {}

        if self.fields_to_extract is None:
            parsed = get_nested(prediction, "")

        elif isinstance(self.fields_to_extract, list):
            for key in self.fields_to_extract:
                parsed[key] = get_nested(prediction, key)

        elif isinstance(self.fields_to_extract, dict):
            for new_key, path in self.fields_to_extract.items():
                parsed[new_key] = get_nested(prediction, path)

        return parsed