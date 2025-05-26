import string, os, time, random
from typing import Optional, Union
import hashlib
from zeep import Client
from zeep.helpers import serialize_object
import pandas as pd
from typing import List, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from .constants import BRENDA

# For aditional implementations see: https://www.brenda-enzymes.org/soap.php
# TODO add more functions
func_map = {
    #'getReference': ["ecNumber", "organism"],
    # 'getEcNumbersFromSequence': ,
    # 'getOrganismsFromSequence': ,
    'getKmValue': ["ecNumber", "kmValue", "kmValueMaximum", "substrate", "commentary", "organism", "ligandStructureId", "literature"],
    # 'getGeneralInformation': ,
    # 'getExpression': ,
    'getIc50Value': ["ecNumber", "ic50Value", "ic50ValueMaximum", "inhibitor", "commentary", "organism", "ligandStructureId", "literature"],
    'getKcatKmValue': ["ecNumber", "kcatKmValue", "kcatKmValueMaximum", "substrate", "commentary", "organism", "ligandStructureId", "literature"],
    'getPhStability': ["ecNumber", "phStability", "phStabilityMaximum", "commentary", "organism", "literature"]
    # 'getOxidationStability': ,
    # 'getNaturalSubstratesProducts': ,
    # 'getEngineering': ,
    # 'getNaturalProduct': ,
    # 'getMetalsIons': ,
    # 'getActivatingCompound': ,
    # 'getInhibitors': ,
    # 'getCofactor': ,
    # 'getGeneralStability': ,
    # 'getNaturalSubstrate': ,
    # 'getMolecularWeight': ,
    # 'getCrystallization': ,
    # 'getSubstratesProducts': ,
    # 'getReactionType': ,
    # 'getOrganismSynonyms': ,
    # 'getEnzymeNames': ,
    # 'getOrganicSolventStability': ,
    # 'getApplication': ,
    # 'getSynonyms': ,
    # 'getTemperatureOptimum': ,
    # 'getTemperatureStability': ,
    # 'getPiValue': ,
    # 'getTemperatureRange': ,
    # 'getRecommendedName': ,
    # 'getProduct': ,
    # 'getCasRegistryNumber': ,
    # 'getLocalization': ,
    # 'getPosttranslationalModification': ,
    # 'getSystematicName': ,
    # 'getCloned': ["ecNumber", "organism"],
    # 'getSpecificActivity': ,
    # 'getSubunits': ,
    # 'getLigands': ,
    # 'getTurnoverNumber': ,
    # 'getReaction': ,
    # 'getSourceTissue': ,
    # 'getSubstrate': ,
    # 'getPhRange': ,
    # 'getStorageStability': ,
    # 'getPhOptimum': ,
    # 'getDisease': ,
    # 'getPurification': ,
    # 'getRenatured': ,
    # 'getKiValue': ,
    # 'getPathway': ,
    # 'getPdb': ,
}

class BrendaInstance():
    def __init__(
            self, 
            email: str, 
            password: str, 
            max_workers: int = 5,
            min_wait: float = 0,
            max_wait: float = 2,
            functions: Optional[Union[list, dict]] = None,
        ):
        """
        Initialize the BrendaInstance.
        Args:
            email (str): Email address for BRENDA API.
            password (str): Password for BRENDA API.
            max_workers (int): Maximum number of parallel requests.
            min_wait (float): Minimum wait time between requests.
            max_wait (float): Maximum wait time between requests.
            functions (list|dict): Functions to keep from the original response.
        """
        self.email = email
        self.password = hashlib.sha256(password.encode("utf-8")).hexdigest()
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.functions = functions if functions else list(func_map.keys())
        self.client = Client(BRENDA.API_URL)

    def show_all_operations(self):
        print("Available operations:")
        for service in self.client.wsdl.services.values():
            for port in service.ports.values():
                for operation_name in port.binding._operations.keys():
                    print(f"- {operation_name}")
    
    def fetch(self, ec_number: str, organism: str, operation: str) -> list:
        """
        Fetch data from BRENDA for a given EC number and organism.
        Args:
            ec_number (str): EC number.
            organism (str): Organism name.
            operation (str): Operation to perform.
        Returns:
            dict: Dictionary containing parsed predictions.
        """
        input_parameters = {
            "ecNumber": ec_number,
            "organism": organism
        }

        if operation in func_map:
            print(f"Executing {operation}")
            try:
                # Get the fields required for this function
                field_names = func_map[operation]

                # Build parameters in order
                param_list = [f"{key}*{input_parameters.get(key, '')}" for key in field_names]

                # Add credentials
                parameters = [self.email, self.password] + param_list

                func = getattr(self.client.service, operation)
                results = serialize_object(func(*parameters))

                time.sleep(random.uniform(self.min_wait, self.max_wait))

            except Exception as e:
                print(f"Error fetching data for {ec_number} in {organism}: {e}")
                return None
        else:
            print(f"Operation {operation} not found.")
            return None

        return [dict(result) for result in results]
    
    def fetch_to_dataframe(self, parameters: List[Tuple[str, str]]) -> pd.DataFrame:
        """
        Fetch data from BRENDA for a list of EC numbers and organisms.
        Args:
            parameters (list): List of tuples containing EC number and organism.
        Returns:
            pd.DataFrame: DataFrame containing parsed predictions.
        """
        # Make sure the cache directory exists
        if not os.path.exists(BRENDA.CACHE_DIR):
            os.makedirs(BRENDA.CACHE_DIR)

        for ec_number, organism in parameters:
            if not isinstance(ec_number, str) or not isinstance(organism, str):
                raise ValueError("EC number and organism must be strings.")
            if not ec_number or not organism:
                raise ValueError("EC number and organism cannot be empty strings.")
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for result, operation in zip(executor.map(lambda op: self.fetch(ec_number, organism, op), self.functions), self.functions):
                    df = pd.DataFrame(result)
                    if not df.empty:
                        # Load cache if it exists
                        export_df = pd.read_csv(os.path.join(BRENDA.CACHE_DIR, f"{operation}.csv")) if os.path.exists(os.path.join(BRENDA.CACHE_DIR, f"{operation}.csv")) else pd.DataFrame()
                        # Append new data to the cache
                        export_df = pd.concat([export_df, df], ignore_index=True)

                        # Remove duplicates
                        # Convert unhashable types (e.g., lists) to hashable types (e.g., strings)
                        export_df = export_df.map(lambda x: str(x) if isinstance(x, list) else x)
                        export_df = export_df.drop_duplicates()

                        # Save the updated cache
                        export_df.to_csv(os.path.join(BRENDA.CACHE_DIR, f"{operation}.csv"), index=False)
                        print(f"Saved {operation} data for {ec_number} in {organism} to cache.")

        
    def save_results(self, directory: str):
        """
        Save the results to the specified directory.
        Args:
            directory (str): Directory to save the results.
        """
        if not os.path.exists(directory):
            os.makedirs(directory)

        for key, df in self.result.items():
            if df.empty:
                print(f"No results for {key}.")
                continue
            df.to_csv(os.path.join(directory, f"{key}.csv"), index=False)
            print(f"Saved {key} to {directory}/{key}.csv")