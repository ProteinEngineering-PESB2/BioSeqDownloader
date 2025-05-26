import os, time, random
from typing import Optional, Union
import pandas as pd
from rcsbapi.data import DataQuery as Query
import requests
from concurrent.futures import ThreadPoolExecutor


from .constants import PDB
from .utils import get_nested

# Check https://data.rcsb.org/rest/v1/core/entry/4HHB for more attributes
# rcsbapi package usage tutorial at: https://pdb101.rcsb.org/train/training-events/apis-python

# TODO output_dir should be a path to a directory relative to the running script no to src
class PDBInterface():
    def __init__(
            self, 
            max_workers: int = 5, 
            min_wait: float = 0,
            max_wait: float = 2,
            batch_size: int = 5000, 
            return_data_list: Optional[list] = None,
            fields_to_extract: Optional[Union[list, dict]] = None,
            output_dir: Optional[str] = None
    ):
        """
        Initialize the PDBInterface.
        Args:
            max_workers (int): Maximum number of parallel requests.
            batch_size (int): Number of entries to process in each batch.
            return_data_list (list): List of data fields to return. by default includes "rcsb_entry_info". return_data_list (list): List of data fields to return. by default includes "rcsb_entry_info".
            more info: https://data.rcsb.org/rest/v1/schema/entry
            fields_to_extract (list|dict): Fields to keep from the original response.
                - If list: Keep those keys.
                - If dict: Maps {desired_name: real_field_name}.
            output_dir (str): Directory to save downloaded files. If None, defaults to the cache directory.
        """
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.batch_size = batch_size
        self.fields_to_extract = fields_to_extract
        self.return_data_list = return_data_list if return_data_list else ["rcsb_entry_info"]
        self.output_dir = os.path.join(
            os.getcwd(),
            output_dir
        ) if output_dir else PDB.CACHE_DIR
    
    def download_structure(self, pdb_id: str, file_format: str = "pdb") -> str:
        """
        Download the structure file for a given PDB ID.
        Args:
            pdb_id (str): PDB ID to download.
            file_format (str): Format of the file to download. Default is "pdb".
        Returns:
            str: Path to the downloaded file.
        """
        print(f"Downloading {pdb_id} in {file_format} format...")
        url = f"{PDB.API_URL}{pdb_id}.{file_format}"
        response = requests.get(url)
        time.sleep(random.uniform(self.min_wait, self.max_wait))
        
        if response.status_code == 200:
            file_path = os.path.join(self.output_dir, f"{pdb_id}.{file_format}")
            with open(file_path, "wb") as f:
                f.write(response.content)
            return file_path
        else:
            print(f"Failed to download {pdb_id}: {response.status_code}")
            return None
        
    def download_structures(self, pdb_ids: list, file_format: str = "pdb") -> list:
        """
        Download structure files in parallel
        Args:
            pdb_ids (list): List of PDB IDs to download.
            file_format (str): Format of the files to download. Default is "pdb".
        Returns:

            list: List of paths to the downloaded files.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        downloaded_files = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for result in executor.map(lambda uid: self.download_structure(uid, file_format), pdb_ids):
                if result:
                    downloaded_files.append(result)

        return downloaded_files
    
    
    def fetch(self, pdb_ids: list) -> dict:
        """
        Run a query to fetch data from the PDB database.
        Args:
            pdb_ids (list): List of PDB IDs to query.
        Returns:
            dict: Query results.
        """
        query = Query(
            input_type="entries",
            input_ids=pdb_ids,
            return_data_list=self.return_data_list
        )

        results = query.exec(
            batch_size=self.batch_size,
            progress_bar=True
        )

        return results
    
    def parse(self, results: dict) -> dict:
        """
        Parse the query results and convert them to a DataFrame.
        Args:
            results (dict): Query results.
        Returns:
            dict: Parsed data.
        """
        # Check if the results contain the expected data
        if "data" not in results or "entries" not in results["data"]:
            raise ValueError("Invalid query results format")
        
        parsed_list = []
        for result in results["data"]["entries"]:
            parsed = {}

            # Determine which fields to include
            if self.fields_to_extract is None:
                parsed = get_nested(result, "")

            elif isinstance(self.fields_to_extract, list):
                for key in self.fields_to_extract:
                    parsed[key] = get_nested(result, key)
                parsed_list.append(parsed)

            elif isinstance(self.fields_to_extract, dict):
                for new_key, nested_path in self.fields_to_extract.items():
                    parsed[new_key] = get_nested(result, nested_path)
                
            parsed_list.append(parsed)

        return parsed_list

    def fetch_to_datafame(self, pdb_ids: list) -> pd.DataFrame:
        """
        Query the PDB database and convert the results to a DataFrame.
        Args:
            pdb_ids (list): List of PDB IDs to query.
        Returns:
            pd.DataFrame: DataFrame containing the query results.
        """
        results = self.fetch(pdb_ids)
        parsed_data = self.parse(results)
        
        # Convert the parsed data to a DataFrame
        df = pd.DataFrame(parsed_data)
        
        return df