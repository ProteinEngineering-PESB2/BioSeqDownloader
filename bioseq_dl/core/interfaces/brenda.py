import os, time, random
from typing import Optional, List, Dict, Any, Union
from requests import Request, Response
from requests.exceptions import RequestException
import hashlib
from zeep import Client
from zeep.helpers import serialize_object


from .base import BaseAPIInterface
from ...constants.databases import BRENDA
from ..utils.base_auxiliary_methods import validate_parameters

# For aditional implementations see: https://www.brenda-enzymes.org/soap.php
class BrendaInterface(BaseAPIInterface):
    METHODS = {
        "getKmValue": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "kmValue": (str, None, True),
                "kmValueMaximum": (str, None, True),
                "substrate": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "ligandStructureId": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getIc50Value": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "ic50Value": (str, None, True),
                "ic50ValueMaximum": (str, None, True),
                "inhibitor": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "ligandStructureId": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getKcatKmValue": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "kcatKmValue": (str, None, True),
                "kcatKmValueMaximum": (str, None, True),
                "substrate": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "ligandStructureId": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getKiValue": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "kiValue": (str, None, True),
                "kiValueMaximum": (str, None, True),
                "inhibitor": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "ligandStructureId": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getPhRange": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "phRange": (str, None, True),
                "phRangeMaximum": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getPhOptimum": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "phOptimum": (str, None, True),
                "phOptimumMaximum": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getPhStability": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "phStability": (str, None, True),
                "phStabilityMaximum": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getCofactor": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "cofactor": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "ligandStructureId": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getTemperatureOptimum": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "temperatureOptimum": (str, None, True),
                "temperatureOptimumMaximum": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getTemperatureStability": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "temperatureStability": (str, None, True),
                "temperatureStabilityMaximum": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        },
        "getTemperatureRange": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "ecNumber": (str, None, True),
                "temperatureRange": (str, None, True),
                "temperatureRangeMaximum": (str, None, True),
                "commentary": (str, None, True),
                "organism": (str, None, True),
                "literature": (str, None, True),
            },
            "group_queries": [None],
            "separator": None
        }
    }

    def __init__(
            self, 
            email: str, 
            password: str,
            cache_dir: Optional[str] = None,
            config_dir: Optional[str] = None,
            output_dir: Optional[str] = None,
            **kwargs
        ):
        """
        Initialize the BrendaInstance.
        Args:
            email (str): Email address for BRENDA API.
            password (str): Password for BRENDA API.
            cache_dir (str): Directory to cache results.
            config_dir (str): Directory for configuration files.
            output_dir (str): Directory to save output files.
        """
        if cache_dir:
            cache_dir = os.path.abspath(cache_dir)
        else:
            cache_dir = BRENDA.CACHE_DIR if BRENDA.CACHE_DIR is not None else ""

        if config_dir is None:
            config_dir = BRENDA.CONFIG_DIR if BRENDA.CONFIG_DIR is not None else ""

        super().__init__(cache_dir=cache_dir, config_dir=config_dir, **kwargs, min_wait=2.0, max_wait=5.0)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.email = email
        self.password = hashlib.sha256(password.encode("utf-8")).hexdigest()
        self.client = Client(BRENDA.API_URL)


    def show_all_methods(self):
        print("Available methods:")
        for service in self.client.wsdl.services.values():
            for port in service.ports.values():
                for method_name in port.binding._methods.keys():
                    print(f"- {method_name}")
    
    def fetch(self, query: Union[str, dict, list], *, method: str = "getKmValue", **kwargs):
        """
        Fetch data from BRENDA for a given EC number and organism.
        Args:
            query (dict): Query parameters to filter the results.
                - `ecNumber`: Enzyme Commission number (e.g., '1.1.1.1').
                - `organism`: Organism name (e.g., 'Escherichia coli').
            method (str): Name of the method to perform (e.g., 'getKmValue').
        Returns:
            list: List of results from the BRENDA API.
        """
        if method not in self.METHODS.keys():
            print(f"method {method} is not supported. Available methods: {list(self.METHODS.keys())}")
            return []
        if not isinstance(query, dict):
            print("Query must be a dictionary with keys matching the method parameters.")
            return []
        
        _, _, parameters, inputs = self.initialize_method_parameters(query, method, self.METHODS, **kwargs)

        # Validate and clean parameters
        try:
            validated_params = validate_parameters(inputs, parameters)
        except ValueError as e:
            raise ValueError(f"Invalid parameters for method '{method}': {e}")


        results = []
        try:
            params = self.METHODS[method]["parameters"].keys()

            # Build parameters in order
            param_list = [f"{k}*{validated_params.get(k, '')}" for k in params]

            # Add credentials
            parameters = [self.email, self.password] + param_list

            func = getattr(self.client.service, method)
            result = serialize_object(func(*parameters))
            result = [dict(entry) for entry in result] if isinstance(result, list) else dict(result)

            self._delay()


            results.extend(result if isinstance(result, list) else [result])
        
        except Exception as e:
            print(f"Error fetching data for {method} with parameters {query}: {e}")
            return []
        
        return results

    
    def get_dummy(self, *, method: Optional[str] = None, **kwargs) -> dict:
        """
        Get a dummy object for the BRENDA API interface.
        Args:
            method (str, optional): Name of the method to get a dummy for.
                If None, return dummy data for all methods.
        Returns:
            dict: Dummy object containing example data for the specified method.
        """
        # return super().get_dummy(
        #     query={
        #         "ecNumber": "1.1.1.1",
        #         "organism": "Escherichia coli",
        #     },
        #     methods=list(methods.keys())

        # )
        dummy_results = {}

        query = {
            "ecNumber": "1.1.1.1",
            "organism": "Escherichia coli"
        }

        if method:
            dummy_results[method] = super().get_dummy(query=query, method=method, **kwargs)
        else:
            for method in self.METHODS.keys():
                dummy_results[method] = super().get_dummy(query=query, method=method, **kwargs)
        return dummy_results
    
    def get_methods(self) -> List[str]:
        """
        Get the list of available methods.
        Returns:
            List[str]: List of method names.
        """
        return list(self.METHODS.keys())

    def query_usage(self) -> str:
        """
        Get the usage of the BRENDA API.
        Returns:
            str: Usage information.
        """
        usage = """Usage: To fetch data from BRENDA, use the following parameters.
        Example:
            - fetch(query={}, methods=["getKmValue", "getIc50Value"])
        Available methods: """ + ", ".join(self.METHODS.keys()) + "\n\n"
        usage += "For more information about each method, please refer to the BRENDA documentation."
        usage += "\nOr use `show_method({method_name})` to see the parameters required for each method."
        return usage
    
    
    def show_method(self, method_name: str) -> str:
        """
        Show the parameters required for a specific method.
        Args:
            method_name (str): Name of the method.
        Returns:
            str: Parameters required for the method.
        """
        if method_name not in self.METHODS.keys():
            return f"method {method_name} is not supported."

        params = self.METHODS[method_name]
        return f"Parameters for {method_name}: {', '.join(params)}"
    
    def parse(self, data: Any, fields_to_extract: Optional[Union[list, dict]], **kwargs):
        """
        Parse the response from the BioGRID API.
        Args:
            data (dict): The fetched data.
            fields_to_extract (List|Dict): Fields to keep from the original response.
                - If List: Keep those keys.
                - If Dict: Maps {desired_name: real_field_name}.
        Returns:
            any: Parsed data from the response.
        """
        if not data:
            return {}

        if not isinstance(data, (dict, list)):
            raise ValueError("Response must be a dictionary or a list.")

        return self._extract_fields(data, fields_to_extract)
    