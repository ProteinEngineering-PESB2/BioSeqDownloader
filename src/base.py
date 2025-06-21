import time, random, os, hashlib, json
import requests
import yaml
from typing import Any
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Union, List, Dict, Optional

from .utils import get_feature_keys, get_nested

class BaseAPIInterface(ABC):

    def __init__(
        self,
        cache_dir: str = "./cache",
        config_dir: Optional[str] = None,
        max_workers: int = 5,
        min_wait: float = 1.0,
        max_wait: float = 2.0,
        total_retries: int = 5,
        headers: Optional[Dict] = None,
        use_config: bool = True
    ):
        """
        Initialize the BaseAPIInterface class.
        
        Args:
            cache_dir (str): Directory to store cached data.
            config_dir (Optional[str]): Directory to load configuration files.
            max_workers (int): Maximum number of parallel requests.
            min_wait (float): Minimum wait time between requests.
            max_wait (float): Maximum wait time between requests.
            total_retries (int): Total number of retries for requests.
            headers (Dict, optional): Headers to include in requests.
            use_config (bool): Whether to use a configuration file for initialization.
        """
        self.cache_dir = os.path.abspath(cache_dir)
        self.config_dir = config_dir
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.total_retries = total_retries
        self.headers = headers or {}
        self.use_config = use_config

        self.configs: Dict[str, dict] = {}

        if self.use_config and self.config_dir:
            self._load_all_configs(self.config_dir)

        os.makedirs(self.cache_dir, exist_ok=True)

        # Init session
        self.session = requests.Session()
        retrues = Retry(
            total=self.total_retries,
            backoff_factor=0.25,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retrues)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.session.headers.update(self.headers or {"Content-Type": "application/json"})

    def get_config(self, key: str) -> dict:
        """
        Return the configuration dictionary for a given key (config filename without extension).
        """
        return self.configs.get(key, {})

    def _load_all_configs(self, config_dir: str) -> None:
        """
        Load all configuration files from the specified directory.
        
        Args:
            config_dir (str): Directory containing configuration files.
        """
        if not os.path.exists(config_dir):
            raise FileNotFoundError(f"Configuration directory not found: {config_dir}")
        
        self.configs = {}

        for fname in os.listdir(config_dir):
            path = os.path.join(config_dir, fname)
            if os.path.isfile(path):
                name, ext = os.path.splitext(fname)
                with open(path, "r") as f:
                    try:
                        if ext == ".json":
                            self.configs[name] = json.load(f)
                        elif ext in [".yaml", ".yml"]:
                            self.configs[name] = yaml.safe_load(f)
                    except Exception as e:
                        print(f"Error loading config {fname}: {e}")

    def _delay(self):
        """
        Introduce a random delay between min_wait and max_wait.
        """
        time.sleep(random.uniform(self.min_wait, self.max_wait))

    def _make_cache_key(self, input_obj: Union[str, tuple, dict, list], **kwargs) -> str:
        """Generate a strin key from the input object."""
        if isinstance(input_obj, str):
            base = input_obj
        elif isinstance(input_obj, tuple):
            base = "_".join(map(str, input_obj))
        elif isinstance(input_obj, dict):
            base = json.dumps(input_obj, sort_keys=True)
        elif isinstance(input_obj, list):
            base = "_".join(map(str, input_obj))
        else:
            raise ValueError("Input must be a string, tuple, list or dictionary.")
        
        # Include relevant kwargs (like 'operation') in the cache key
        extra = json.dumps({k: kwargs[k] for k in sorted(kwargs)}, sort_keys=True)
        return f"{base}_{extra}"

    def _hash_key(self, key: str) -> str:
        return hashlib.md5(key.encode("utf-8")).hexdigest()
    
    def _get_cache_path(self, identifier: str) -> str:
        """
        Generate a cache file path based on the identifier.
        """
        hashed_key = self._hash_key(identifier)
        return os.path.join(self.cache_dir, f"{hashed_key}.json")
    
    def has_results(self, identifier: str) -> bool:
        """
        Check if results for a given identifier are cached.
        """
        cache_path = self._get_cache_path(identifier)
        return os.path.exists(cache_path)
    
    def load_cache(self, identifier: str) -> Optional[Dict|pd.DataFrame]:
        """
        Load cached results for a given identifier.
        """
        cache_path = self._get_cache_path(identifier)
        if os.path.exists(cache_path):
            if cache_path.endswith('.csv'):
                return pd.read_csv(cache_path)
            else:
                with open(cache_path, 'r') as f:
                    return json.load(f)
        return None

    def save_cache(self, identifier: str, data: Union[List, Dict, pd.DataFrame]) -> None:
        """Save results to cache."""
        path = self._get_cache_path(identifier)

        if isinstance(data, pd.DataFrame):
            data.to_csv(path, index=False)
        else:
            with open(path, 'w') as f:
                json.dump(data, f)

    def _maybe_parse(self, data, parse: bool, **kwargs) -> Union[List, Dict]:
        if not parse:
            return data
        config_key = kwargs.pop("config_key", "default")
        fields_to_extract = kwargs.pop("fields_to_extract", None)

        if not fields_to_extract and self.use_config and config_key:
                fields_to_extract = self.get_config(config_key) or None

        #print(f"Parsing data with fields: {fields_to_extract}")
        if isinstance(data, list):
            return [self.parse(data=d, fields_to_extract=fields_to_extract, **kwargs) for d in data]
        elif isinstance(data, dict):
            return self.parse(data=data, fields_to_extract=fields_to_extract, **kwargs)
        elif isinstance(data, str):
            # This is the case of KEGG API, which returns a string, parse method should handle it
            return self.parse(data=data, fields_to_extract=fields_to_extract, **kwargs)
        return data
    
    def fetch_single(self, query: Union[str, dict, list[str]], parse: bool = False, *args, **kwargs) -> Union[List, Dict]:
        """
        General-purpose fetch method with optional parsing and cache handling.

        Args:
            query (Union[str, dict, list[str]]): Query to fetch data for.
            parse (bool): Whether to parse the fetched data.
        
        kwargs:
            config_key (str): Key to use for configuration settings.
            fields_to_extract (Optional[Union[list, dict]]): Fields to extract from the fetched data.
        Returns:
            Any: Fetched data, parsed if requested.
        """
        cache_key = self._make_cache_key(query, **kwargs)

        if self.has_results(cache_key):
            cached = self.load_cache(cache_key)
            result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
        else:
            result = self.fetch(query=query, *args, **kwargs)
            if result:
                self.save_cache(cache_key, result)
        
        # if parse:
        #     if isinstance(result, list):
        #         return [self.parse(data=r, **kwargs) for r in result]
        #     elif isinstance(result, dict) or isinstance(result, str):
        #         return self.parse(data=result, **kwargs)
        
        result = self._maybe_parse(data=result, parse=parse, **kwargs) if parse else result

        return result if result is not None else {}
    
    def fetch_batch(self, queries: List[Union[str, dict]], parse: bool = False, *args, **kwargs) -> List[Dict]:
        """
        Fetch data in parallel for a batch of queries.
        Args:
            queries (List[Union[str, dict, list[str]]]): List of queries to fetch data for.
            parse (bool): Whether to parse the fetched data.

        kwargs:
            config_key (str): Key to use for configuration settings.
            fields_to_extract (Optional[Union[list, dict]]): Fields to extract from the fetched data.
        Returns:
            List: List of fetched data, parsed if requested.
        """
        results: List[Any] = [None] * len(queries)

        # Separate queries in cache and not in cache
        queries_to_fetch = []
        index_query_map = {}

        for i, query in enumerate(queries):
            cache_key = self._make_cache_key(query, **kwargs)
            if self.has_results(cache_key):
                cached = self.load_cache(cache_key)
                result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
                results[i] = self._maybe_parse(data=result, parse=parse, **kwargs)
            else:
                index_query_map[i] = query
                queries_to_fetch.append(query)

        # Fetch missing ones in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self.fetch_single, query, parse, *args, **kwargs): i
                for i, query in index_query_map.items()
            }
            for future in future_to_index:
                i = future_to_index[future]
                try:
                    result = future.result()
                    results[i] = result
                except Exception as e:
                    print(f"Error fetching query at index {i} ({queries[i]}): {e}")
                self._delay()

        # Probably this is not needed, but keeping it for reference
        # flattened_results = []
        # for result in results:
        #     if isinstance(result, dict):
        #         flattened_results.append(result)
        #     elif isinstance(result, list):
        #         flattened_results.extend(r for r in result if isinstance(r, dict))
        
        return results
    
    def get_dummy(self, *args, **kwargs) -> dict:
        """
        Get a dummy object for the API interface.
        This is useful for knowing the structure of the data returned by the API.
        """
        query = kwargs.pop('query', None)
        parse = kwargs.pop('parse', False)

        if not query:
            raise ValueError("Query must be provided to get dummy data.")

        response = self.fetch_single(query=query, parse=parse, *args, **kwargs)

        if isinstance(response, list):
            return get_feature_keys(response[0] if response else {})
        elif isinstance(response, dict):
            return get_feature_keys(response)
        else:
            raise ValueError("Response must be a list or a dictionary.")

    def _extract_fields(self, data: Union[dict, list], fields_to_extract: Optional[Union[list, dict]] = None) -> Union[dict, list]:
        """
        Extract specified fields from the data.
        
        Args:
            data (Union[List, Dict]): Data to parse.
            fields_to_extract (List|Dict): Fields to keep from the original response.
                - If List: Keep those keys.
                - If Dict: Maps {desired_name: real_field_name}.
        Returns:
            Union[dict, list]: Data with only the specified fields.
        """
        parsed = {}
        if isinstance(fields_to_extract, List):
            if isinstance(data, List):
                parsed = [
                    {key: get_nested(item, key) for key in fields_to_extract}
                    for item in data
                ]
            elif isinstance(data, Dict):
                parsed = {key: get_nested(data, key) for key in fields_to_extract}
        elif isinstance(fields_to_extract, Dict):
            if isinstance(data, List):
                parsed = [
                    {new_key: get_nested(item, path) for new_key, path in fields_to_extract.items()}
                    for item in data
                ]
            elif isinstance(data, Dict):
                parsed = {new_key: get_nested(data, path) for new_key, path in fields_to_extract.items()}
        # If no fields to extract, return the entire structure
        elif fields_to_extract is None and isinstance(data, List):
            parsed = [get_nested(item, "") for item in data]
        elif fields_to_extract is None and isinstance(data, Dict):
            parsed = get_nested(data, "")
        
        return parsed
            
    @abstractmethod
    def query_usage(self) -> str:
        raise NotImplementedError
    
    @abstractmethod
    def fetch(self, query: Union[str, dict, list], **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def parse(self, data: Any, fields_to_extract: Optional[Union[list, dict]], **kwargs):
        raise NotImplementedError
    
        