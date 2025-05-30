import requests, re, os, time, threading, random
from pathlib import Path
from typing import List, Dict, Optional, Union, List, Dict
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from .constants import ALPHAFOLD
from .utils import get_nested

# TODO output_dir should be a path to a directory relative to the running script no to src
class AlphafoldInterface():
    def __init__(
            self, 
            total_retries: int = 5, 
            max_workers: int = 5, 
            min_wait: float = 0, 
            max_wait: float = 2, 
            structures: List[str] = ["pdb"],
            fields_to_extract: Optional[Union[List, Dict]] = None,
            output_dir: Optional[str] = ""
        ):
        """
        Initialize the AlphafoldInterface.
        Args:
            total_retries (int): Total number of retries for requests.
            max_workers (int): Maximum number of parallel requests.
            min_wait (float): Minimum wait time between requests.
            max_wait (float): Maximum wait time between requests.
            structures (List[str]): List of structures extensions to download. Available options are ["pdb", "cif", "bcif"] or none.
            self.fields_to_extract (List|Dict): Fields to keep from the original response.
                - If List: Keep those keys.
                - If Dict: Maps {desired_name: real_field_name}.
            for more information, see: https://alphafold.ebi.ac.uk/#/public-api/get_predictions_api_prediction__qualifier__get
            output_dir (str): Directory to save downloaded files. If None, defaults to the cache directory.
        """
        self.retries = Retry(total=total_retries, backoff_factor=0.25, status_forceList=[ 500, 502, 503, 504 ])
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=self.retries))
        self.session.headers.update({"Content-Type": "application/json"})
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.structures = structures
        self.fields_to_extract = fields_to_extract
        self.output_dir = os.path.join(
            Path.cwd(),
            output_dir
        ) if output_dir else ALPHAFOLD.CACHE_DIR

    def fetch_prediction(self, uniprot_id) -> Dict:
        """
        Get prediction for a given UniProt ID.
        Args:
            uniprot_id (str): UniProt ID to fetch prediction for.
        Returns:
            Dict: Prediction data.
        """
        url = f"{ALPHAFOLD.API_URL}{uniprot_id}"
        
        # Try and retry if necesary
        try:
            response = self.session.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching prediction for {uniprot_id}: {e}")
            return {}
        return response.json()

    
    def parse_prediction(self, prediction: Dict) -> Dict:
        """
        Parse the prediction data.
        Args:
            prediction (Dict): Prediction data.
            structures (bool): Whether to download the structure.

        Returns:
            Dict: Parsed prediction data.
        """
        parsed = {}

        # Determine which fields to include 
        if self.fields_to_extract is None:
            parsed = get_nested(prediction, "")

        elif isinstance(self.fields_to_extract, List):
            for key in self.fields_to_extract:
                parsed[key] = get_nested(prediction, key)

        elif isinstance(self.fields_to_extract, Dict):
            for new_key, nested_path in self.fields_to_extract.items():
                parsed[new_key] = get_nested(prediction, nested_path)
        
        # Download structures if requested
        if self.structures:
            for ext in self.structures:
                structure_url = parsed[f"{ext}_url"] if f"{ext}_url" in parsed else None
                if structure_url:
                    file_name = parsed[f"{ext}_url"].split("/")[-1]
                    # Check if file already exists
                    if os.path.exists(self.output_dir + "/" + file_name):
                        continue
                    else:
                        response = self.session.get(structure_url)
                        with open(self.output_dir + "/" + file_name, "wb") as f:
                            f.write(response.content)
        return parsed

    def fetch_and_parse_predictions(self, uniprot_id: str) -> List:
        """
        Get and parse predictions for a given UniProt ID.
        Args:
            uniprot_id (str): UniProt ID to fetch predictions for.
            structures (bool): Whether to download structure files.
        Returns:
            List: List of parsed predictions.
        """
        print(f"Fetching prediction for {uniprot_id}")
        prediction = self.fetch_prediction(uniprot_id)
        time.sleep(random.uniform(self.min_wait, self.max_wait))
        if not prediction:
            return []
        
        return [self.parse_prediction(p) for p in prediction]
    
    def download_from_uniprot_ids(self, ids: List) -> pd.DataFrame:
        """
        Download predictions for a List of UniProt IDs.
        Args:
            ids (List): List of UniProt IDs to fetch predictions for.
            structures (bool): Whether to download structure files.
            max_workers (int): Maximum number of parallel requests.
        Returns:
            pd.DataFrame: DataFrame containing parsed predictions.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        predictions = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for result in executor.map(lambda uid: self.fetch_and_parse_predictions(uid), ids):
                if result:
                    predictions.extend(result)

        return pd.DataFrame(predictions)