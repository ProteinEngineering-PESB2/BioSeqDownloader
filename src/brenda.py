import os, time, random
from typing import Optional, List, Dict, Any, Union
import hashlib
from zeep import Client
from zeep.helpers import serialize_object


from .base import BaseAPIInterface
from .constants import BRENDA

# For aditional implementations see: https://www.brenda-enzymes.org/soap.php
# TODO add more functions
# TODO What do i do with fields_to_extract?
func_map = {
    'getKmValue': ["ecNumber", "kmValue", "kmValueMaximum", "substrate", "commentary", "organism", "ligandStructureId", "literature"],
    'getIc50Value': ["ecNumber", "ic50Value", "ic50ValueMaximum", "inhibitor", "commentary", "organism", "ligandStructureId", "literature"],
    'getKcatKmValue': ["ecNumber", "kcatKmValue", "kcatKmValueMaximum", "substrate", "commentary", "organism", "ligandStructureId", "literature"],
    'getKiValue': ["ecNumber", "kiValue", "kiValueMaximum", "inhibitor", "commentary", "organism", "ligandStructureId", "literature"],
    'getPhRange': ["ecNumber", "phRange", "phRangeMaximum", "commentary", "organism", "literature"],
    'getPhOptimum': ["ecNumber", "phOptimum", "phOptimumMaximum", "commentary", "organism", "literature"],
    'getPhStability': ["ecNumber", "phStability", "phStabilityMaximum", "commentary", "organism", "literature"],
    'getCofactor': ["ecNumber", "cofactor", "commentary", "organism", "ligandStructureId", "literature"],
    'getTemperatureOptimum': ["ecNumber", "temperatureOptimum", "temperatureOptimumMaximum", "commentary", "organism", "literature"],
    'getTemperatureStability': ["ecNumber", "temperatureStability", "temperatureStabilityMaximum", "commentary", "organism", "literature"],
    'getTemperatureRange': ["ecNumber", "temperatureRange", "temperatureRangeMaximum", "commentary", "organism", "literature"]
}

class BrendaInstance(BaseAPIInterface):
    def __init__(
            self, 
            email: str, 
            password: str,
            output_dir: Optional[str] = None,
            **kwargs
        ):
        """
        Initialize the BrendaInstance.
        Args:
            email (str): Email address for BRENDA API.
            password (str): Password for BRENDA API.
            functions (list|dict): Functions to keep from the original response.
        """

        cache_dir = BRENDA.CACHE_DIR if BRENDA.CACHE_DIR is not None else ""
        super().__init__(cache_dir=cache_dir, **kwargs, min_wait=2.0, max_wait=5.0)
        self.email = email
        self.password = hashlib.sha256(password.encode("utf-8")).hexdigest()
        self.client = Client(BRENDA.API_URL)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def show_all_operations(self):
        print("Available operations:")
        for service in self.client.wsdl.services.values():
            for port in service.ports.values():
                for operation_name in port.binding._operations.keys():
                    print(f"- {operation_name}")
    
    def fetch(self, query: Union[str, tuple, dict], **kwargs) -> list:
        """
        Fetch data from BRENDA for a given EC number and organism.
        Args:
            query (dict): Query parameters to filter the results.
                - `ecNumber`: Enzyme Commission number (e.g., '1.1.1.1').
                - `organism`: Organism name (e.g., 'Escherichia coli').
            **kwargs: Additional parameters for the request.
                - `operation`: Name of the operation to perform (e.g., 'getKmValue').
        Returns:
            list: List of results from the BRENDA API.
        """
        operation = kwargs.get("operation")

        if operation not in func_map:
            print(f"Operation {operation} is not supported. Available operations: {list(func_map.keys())}")
            return []
        if not isinstance(query, dict):
            print("Query must be a dictionary with keys matching the operation parameters.")
            return []
        
        results = []
        try:
            # Get the fields required for this function
            field_names = func_map[operation]

            # Build parameters in order
            param_list = [f"{key}*{query.get(key, '')}" for key in field_names]

            # Add credentials
            parameters = [self.email, self.password] + param_list

            func = getattr(self.client.service, operation)
            result = serialize_object(func(*parameters))
            result = [dict(entry) for entry in result] if isinstance(result, list) else dict(result)

            self._delay()


            results.extend(result if isinstance(result, list) else [result])
        
        except Exception as e:
            print(f"Error fetching data for {operation} with parameters {query}: {e}")
            return []
        
        return results
    
    def get_dummy(self, *, operation: Optional[str] = None, **kwargs) -> dict:
        """
        Get a dummy object for the BRENDA API interface.
        Args:
            operation (str, optional): Name of the operation to get a dummy for.
                If None, return dummy data for all operations.
        Returns:
            dict: Dummy object containing example data for the specified operation.
        """
        # return super().get_dummy(
        #     query={
        #         "ecNumber": "1.1.1.1",
        #         "organism": "Escherichia coli",
        #     },
        #     operations=list(func_map.keys())

        # )
        dummy_results = {}

        query = {
            "ecNumber": "1.1.1.1",
            "organism": "Escherichia coli"
        }

        if operation:
            dummy_results[operation] = super().get_dummy(query=query, operation=operation, **kwargs)
        else:
            for operation in func_map.keys():
                dummy_results[operation] = super().get_dummy(query=query, operation=operation, **kwargs)
        return dummy_results
    
    def get_operations(self) -> List[str]:
        """
        Get the list of available operations.
        Returns:
            List[str]: List of operation names.
        """
        return list(func_map.keys())
    
    def query_usage(self) -> str:
        """
        Get the usage of the BRENDA API.
        Returns:
            str: Usage information.
        """
        usage = """Usage: To fetch data from BRENDA, use the following parameters.
        Example:
            - fetch(query={}, operations=["getKmValue", "getIc50Value"])
        Available operations: """ + ", ".join(func_map.keys()) + "\n\n"
        usage += "For more information about each operation, please refer to the BRENDA documentation."
        usage += "\nOr use `show_operation({operation_name})` to see the parameters required for each operation."
        return usage
    
    
    def show_operation(self, operation_name: str) -> str:
        """
        Show the parameters required for a specific operation.
        Args:
            operation_name (str): Name of the operation.
        Returns:
            str: Parameters required for the operation.
        """
        if operation_name not in func_map:
            return f"Operation {operation_name} is not supported."
        
        params = func_map[operation_name]
        return f"Parameters for {operation_name}: {', '.join(params)}"
    
    def parse(self, raw_data: Any, **kwargs) -> Dict:
        # TODO implement parsing logic based on the fields_to_extract
        print("Parsing response from BRENDA...")
        return response
    