import os
import requests
from typing import Optional, Union, List, Dict



from .base import BaseAPIInterface

from .constants import PDB
from .utils import get_nested

# Check https://data.rcsb.org/rest/v1/core/entry/4HHB for more attributes
# rcsbapi package usage tutorial at: https://pdb101.rcsb.org/train/training-events/apis-python

class PDBInterface(BaseAPIInterface):
    def __init__(
            self,
            batch_size: int = 5000, 
            download_structures: bool = False,
            return_data_list: Optional[List] = None,
            fields_to_extract: Optional[Union[List, Dict]] = None,
            output_dir: Optional[str] = None,
            **kwargs
    ):
        """
        Initialize the PDBInterface.
        Args:.
            batch_size (int): Number of entries to process in each batch.
            download_structures (bool): Whether to download structure files. Default is False.
            return_data_list (list): List of data fields to return. by default includes "rcsb_entry_info". return_data_list (list): List of data fields to return. by default includes "rcsb_entry_info".
            more info: https://data.rcsb.org/rest/v1/schema/entry
            fields_to_extract (list|dict): Fields to keep from the original response.
                - If list: Keep those keys.
                - If dict: Maps {desired_name: real_field_name}.
            output_dir (str): Directory to save downloaded files. If None, defaults to the cache directory.
        """
        cache_dir = PDB.CACHE_DIR if PDB.CACHE_DIR is not None else ""
        super().__init__(cache_dir=cache_dir, **kwargs)
        self.batch_size = batch_size
        self.download_structures = download_structures
        self.fields_to_extract = fields_to_extract
        self.return_data_list = return_data_list if return_data_list else ["rcsb_entry_info"]
        self.output_dir = output_dir or cache_dir
       
    def fetch(self, pdb_id: str) -> dict:
        """
        Run a query to fetch data from the PDB database.
        Args:
            pdb_id (str): PDB ID to fetch data for.
        Returns:
            dict: Fetched data for the given PDB ID.
        """
        url = f"{PDB.API_URL}entry/{pdb_id}"
        
        try:
            response = self.session.get(url)
            self._delay()
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching prediction for {pdb_id}: {e}")
            return {}
        
    def fetch_structure(self, pdb_id: str, file_format: str = "pdb") -> str:
        """
        Download the structure file for a given PDB ID.
        Args:
            pdb_id (str): PDB ID to download.
            file_format (str): Format of the file to download. Default is "pdb".
        Returns:
            str: Path to the downloaded file.
        """
        if os.path.exists(self.output_dir + "/pdb_files/" + f"{pdb_id}.{file_format}"):
            print(f"Info: Structure for {pdb_id} already exists in {file_format} format.")
            return self.output_dir + "/pdb_files/" + f"{pdb_id}.{file_format}"
        
        print(f"Info: Downloading {pdb_id} in {file_format} format...")

        if not os.path.exists(self.output_dir + "/pdb_files"):
            os.makedirs(self.output_dir + "/pdb_files")

        url = f"{PDB.STRUCTURE_URL}{pdb_id}.{file_format}"

        try:
            response = self.session.get(url)
            self._delay()
            response.raise_for_status()
            file_path = os.path.join(self.output_dir + "/pdb_files/", f"{pdb_id}.{file_format}")
            with open(file_path, "wb") as f:
                f.write(response.content)
            return file_path
        except requests.exceptions.RequestException as e:
            print(f"Error downloading structure for {pdb_id}: {e}")
            return ""
        
    def fetch_single(self, query: str | tuple | Dict, parse: bool = False):

        if self.download_structures and query and isinstance(query, str):
            self.fetch_structure(query)
        return super().fetch_single(query, parse)
    
    def fetch_batch(self, queries: List[Union[str, tuple, dict]], parse: bool = False) -> List:
        results = super().fetch_batch(queries, parse)
        if self.download_structures:
            for query in queries:
                if isinstance(query, str):
                    self.fetch_structure(query)
        return results

    def parse(
            self, 
            data: Union[List, Dict]
        ) -> Union[List, Dict]:
        """
        Parse data by extracting specified fields or returning the entire structure.
        Args:
            data (Union[List, Dict]): Data to parse.
        Returns:
            Union[List, Dict]: Parsed data with specified fields or the entire structure.
        """
        
        # Check input data type
        if not isinstance(data, (list, dict)):
            raise ValueError("Data must be a list or a dictionary.")

        parsed = {}
        if self.fields_to_extract is None:
            parsed = get_nested(data, "")

        elif isinstance(self.fields_to_extract, list):
            parsed = {key: get_nested(data, key) for key in self.fields_to_extract}

        elif isinstance(self.fields_to_extract, dict):
            parsed = {new_key: get_nested(data, path) for new_key, path in self.fields_to_extract.items()}
        
        return parsed
    
    def get_dummy(self, *args, **kwargs) -> Dict:
        """
        Get a dummy response.
        Useful for knowing the structure of the data returned by the API.
        Returns:
            Dict: Dummy response with example fields.
        """
        return super().get_dummy(
            query="4HHB",
            parse=False
        )