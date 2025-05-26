import time, random
from typing import Optional, Union
import pandas as pd
from Bio import Entrez, SeqIO
from Bio.Entrez.Parser import ListElement, DictionaryElement, StringElement
from concurrent.futures import ThreadPoolExecutor

from .utils import get_nested, flatten_keys

class RefSeqInterface():
    def __init__(
            self,
            email: str = "",
            max_workers: int = 5,
            min_wait: int = 1,
            max_wait: int = 5,
            fields_to_extract: Optional[Union[list, dict]] = None
    ):
        """
        Initialize the RefSeqInterface class.
        Args:
            email (str): Email address for NCBI Entrez.
            max_workers (int): Maximum number of parallel requests.
            min_wait (int): Minimum wait time between requests.
            max_wait (int): Maximum wait time between requests.
            self.fields_to_extract (list|dict): Fields to keep from the original response.
                - If list: Keep those keys.
                - If dict: Maps {desired_name: real_field_name}.
        """
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.fields_to_extract = fields_to_extract

        # Set Entrez email
        Entrez.email = email

    def to_native(self, obj):
        """
        Convert EntrezDict to native Python types.
        Args:
            obj (EntrezDict): EntrezDict object to convert.
        Returns:
            dict: Converted object.
        """
        if isinstance(obj, DictionaryElement):
            return {k: self.to_native(v) for k, v in obj.items()}
        elif isinstance(obj, ListElement):
            return [self.to_native(item) for item in obj]
        elif isinstance(obj, StringElement):
            return str(obj)
        else:
            return obj

    def fetch(self, id: str, db: str = "protein", retmode: str = "xml") -> list:
        """
        Fetch data from NCBI Entrez for a given ID.
        Args:
            id (str): ID to fetch data for.
            db (str): Database to query (default: "protein").
            retmode (str): Return mode (default: "xml").
        Returns:
            list: Fetched data.
        """
        handle = Entrez.efetch(db=db, id=id, retmode=retmode)
        records = Entrez.read(handle)
        handle.close()

        return self.to_native(records)

    
    def parse(self, data: list) -> list:
        """
        Parse the fetched data into a DataFrame.
        Args:
            data (dict): Fetched data from NCBI Entrez.
        Returns:
            dict: Parsed data.
        """
        parsed_list = []

        # Check if data is empty
        if not data:
            return {}
        
        for record in data:
            parsed = {}
            # Determine which fields to include
            if self.fields_to_extract is None:
                pasrsed = get_nested(record, "")

            elif isinstance(self.fields_to_extract, list):
                for key in self.fields_to_extract:
                    parsed[key] = get_nested(record, key)
            
            elif isinstance(self.fields_to_extract, dict):
                for new_key, nested_path in self.fields_to_extract.items():
                    parsed[new_key]  = get_nested(record, nested_path)
            
            # Add the parsed record to the list
            parsed_list.append(parsed)
        
        return parsed_list

    def fetch_and_parse(self, id: str) -> list:
        """
        Fetch and parse data for a given ID.
        Args:
            id (str): ID to fetch data for.
        Returns:
            dict: Parsed data.
        """
        data = self.fetch(id)
        if not data:
            return {}
        # Parse the data
        parsed_data = self.parse(data)
        # Introduce a random wait time between requests
        # to avoid overwhelming the server
        time.sleep(random.uniform(self.min_wait, self.max_wait))
        return parsed_data
    

    def download_from_refseq_ids(self, ids: list) -> pd.DataFrame:
        """
        Get data in parallel for a list of RefSeq IDs.
        Args:
            ids (list): List of RefSeq IDs to fetch predictions for.
        Returns:
            pd.DataFrame: DataFrame containing parsed predictions.
        """
        export_data = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for result in executor.map(lambda uid: self.fetch_and_parse(uid), ids):
                if result:
                    export_data.extend(result)
        
        return pd.DataFrame(export_data)