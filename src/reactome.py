import os, time, random
import pandas as pd
import requests
import threading
from typing import Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

from .utils import get_nested
from .constants import REACTOME

class ReactomeInstance():
    def __init__(
            self, 
            max_workers: int = 5,
            min_wait: float = 0,
            max_wait: float = 2,
            fields_to_extract: Optional[Union[list, dict]] = None,
        ):
        """
        Initialize the ReactomeInstance.
        Args:
            max_workers (int): Maximum number of parallel requests.
            min_wait (float): Minimum wait time between requests.
            max_wait (float): Maximum wait time between requests.
            fields_to_extract (list|dict): Fields to keep from the original response.
                - If list: Keep those keys.
                - If dict: Maps {desired_name: real_field_name}.
        """
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.fields_to_extract = fields_to_extract
        

    def fetch(self, pathway_id: str) -> dict:
        """
        Download pathways from a given Reactome pathway ID.
        Args:
            pathway_id (str): Reactome pathway ID.
        Returns:
            dict: Pathway data.
        """
        url = f"{REACTOME.API_URL}data/pathway/{pathway_id}/containedEvents"

        try:
            response = requests.get(url)
            time.sleep(random.uniform(self.min_wait, self.max_wait))
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to fetch pathway data for {pathway_id}: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {pathway_id}: {e}")
            return None
        
    
    def parse(self, result: dict) -> dict:
        """
        Parse the pathway data.
        Args:
            result (dict): Raw pathway data.
        Returns:
            dict: Parsed pathway data.
        """
        parsed = {}

        # Determine which fields to include 
        if self.fields_to_extract is None:
            parsed = get_nested(result, "")

        elif isinstance(self.fields_to_extract, list):
            for key in self.fields_to_extract:
                parsed[key] = get_nested(result, key)

        elif isinstance(self.fields_to_extract, dict):
            for new_key, nested_path in self.fields_to_extract.items():
                parsed[new_key] = get_nested(result, nested_path)
        
        return parsed
    