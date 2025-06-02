import time, random, os, hashlib, json
import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Union, List, Dict, Optional

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
            cahce_dir (str): Directory to store cached data.
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

    def _make_cache_key(self, input_obj: Union[str, tuple, dict]) -> str:
        """Generate a strin key from the input object."""
        if isinstance(input_obj, str):
            return input_obj
        elif isinstance(input_obj, tuple):
            return "_".join(map(str, input_obj))
        elif isinstance(input_obj, dict):
            return json.dumps(input_obj, sort_keys=True)
        else:
            raise ValueError("Input must be a string, tuple, or dictionary.")

    def _hash_key(self, key: str) -> str:
        return hashlib.md5(key.encode("utf-8")).hexdigest()
    
    def _get_cache_path(self, identifier: str) -> str:
        """
        Generate a cache file path based on the identifier.
        """
        hashed_key = self._hash_key(identifier)
        return os.path.join(self.cache_dir, f"{hashed_key}.json")
        hashed = self._hash_key(identifier)
        return os.path.join(self.cache_dir, f"{hashed}.json")
    
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
        """SAve results to cache."""
        path = self._get_cache_path(identifier)

        if isinstance(data, pd.DataFrame):
            data.to_csv(path, index=False)
        else:
            with open(path, 'w') as f:
                json.dump(data, f)

    def _maybe_parse(self, data, parse: bool):
        if not parse:
            return data
        if isinstance(data, list):
            return [self.parse(d) for d in data]
        elif isinstance(data, dict):
            return self.parse(data)
        return data
    
    def fetch_single(self, query: Union[str, tuple, dict], parse: bool = False):
        """
        General-purpose fetch method with optional parsing and cache handling.

        Args:
            query (Union[str, tuple, dict]): Query to fetch data for.
            parse (bool): Whether to parse the fetched data.
        Returns:
            Any: Fetched data, parsed if requested.
        """
        cache_key = self._make_cache_key(query)

        if self.has_results(cache_key):
            cached = self.load_cache(cache_key)
            result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
        else:
            result = self.fetch(query)
            if result:
                self.save_cache(cache_key, result)
        
        if parse:
            if isinstance(result, list):
                return [self.parse(r) for r in result]
            elif isinstance(result, dict):
                return self.parse(result)
        return result
    
    def fetch_batch(self, queries: List[Union[str, tuple, dict]], parse: bool = False) -> List:
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
            cache_key = self._make_cache_key(query)
            if self.has_results(cache_key):
                cached = self.load_cache(cache_key)
                result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
                results[i] = self._maybe_parse(result, parse)
            else:
                index_query_map[i] = query
                queries_to_fetch.append(query)

        # Fetch missing ones in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self.fetch_single, query, parse): i
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

        return results
    
    @abstractmethod
    def fetch(self, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def parse(self, *args, **kwargs):
        raise NotImplementedError