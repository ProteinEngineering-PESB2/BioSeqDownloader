import time, random, os, hashlib, json
import requests
from typing import Any
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Union, List, Dict, Optional

from .utils import get_feature_keys

class BaseAPIInterface(ABC):

    def __init__(
        self,
        cache_dir: str = "./cache",
        max_workers: int = 5,
        min_wait: float = 1.0,
        max_wait: float = 2.0,
        total_retries: int = 5,
        headers: Optional[Dict] = None
    ):
        """
        Initialize the BaseAPIInterface class.
        
        Args:
            cache_dir (str): Directory to store cached data.
            max_workers (int): Maximum number of parallel requests.
            min_wait (float): Minimum wait time between requests.
            max_wait (float): Maximum wait time between requests.
            total_retries (int): Total number of retries for requests.
            headers (Dict, optional): Headers to include in requests.
        """
        self.cache_dir = os.path.abspath(cache_dir)
        self.max_workers = max_workers
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.total_retries = total_retries
        self.headers = headers or {}

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

    def _delay(self):
        """
        Introduce a random delay between min_wait and max_wait.
        """
        time.sleep(random.uniform(self.min_wait, self.max_wait))

    def _make_cache_key(self, input_obj: Union[str, tuple, dict], **kwargs) -> str:
        """Generate a strin key from the input object."""
        if isinstance(input_obj, str):
            base = input_obj
        elif isinstance(input_obj, tuple):
            base = "_".join(map(str, input_obj))
        elif isinstance(input_obj, dict):
            base = json.dumps(input_obj, sort_keys=True)
        else:
            raise ValueError("Input must be a string, tuple, or dictionary.")
        
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

    def _maybe_parse(self, raw_data, parse: bool, **kwargs) -> Union[List, Dict]:
        if not parse:
            return raw_data
        if isinstance(raw_data, list):
            return [self.parse(raw_data=d, **kwargs) for d in raw_data]
        elif isinstance(raw_data, dict):
            return self.parse(raw_data=raw_data, **kwargs)
        return raw_data
    
    def fetch_single(self, query: Union[str, tuple, dict], parse: bool = False, *args, **kwargs) -> Union[List, Dict]:
        """
        General-purpose fetch method with optional parsing and cache handling.

        Args:
            query (Union[str, tuple, dict]): Query to fetch data for.
            parse (bool): Whether to parse the fetched data.
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
        
        if parse:
            if isinstance(result, list):
                return [self.parse(raw_data=r, **kwargs) for r in result]
            elif isinstance(result, dict):
                return self.parse(raw_data=result, **kwargs)
        return result
    
    def fetch_batch(self, queries: List[Union[str, tuple, dict]], parse: bool = False, *args, **kwargs) -> List:
        """
        Fetch data in parallel for a batch of queries.
        Args:
            queries (List[Union[str, tuple, dict]]): List of queries to fetch data for.
            parse (bool): Whether to parse the fetched data.
        Returns:
            List: List of fetched data, parsed if requested.
        """
        results = [None] * len(queries)

        # Separate queries in cache and not in cache
        queries_to_fetch = []
        index_query_map = {}

        for i, query in enumerate(queries):
            cache_key = self._make_cache_key(query, **kwargs)
            if self.has_results(cache_key):
                cached = self.load_cache(cache_key)
                result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
                results[i] = self._maybe_parse(raw_data=result, parse=parse, **kwargs)
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

        # Flatten results if they are lists with np.concatenate
        flattened_results = []
        for result in results:
            if isinstance(result, dict):
                flattened_results.append(result)
            elif isinstance(result, list):
                flattened_results.extend(r for r in result if isinstance(r, dict))
        
        return flattened_results
    
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
                
    @abstractmethod
    def query_usage(self) -> str:
        raise NotImplementedError
    
    @abstractmethod
    def fetch(self, query: Union[str, tuple, dict], **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def parse(self, raw_data: Any, **kwargs):
        raise NotImplementedError
    
        