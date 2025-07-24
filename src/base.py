import time, random, os, hashlib, json, re
import inspect
import requests
import itertools
import yaml
from typing import Any, Set, Dict, List, Tuple, Union
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Union, List, Dict, Optional
from itertools import permutations

from .utils import get_feature_keys, get_nested

class BaseAPIInterface(ABC):

    cache_key_ignore_args: Set[str] = {
        "parse", "to_dataframe", "fields_to_extract", "config_key", "pages_to_fetch", "outfmt", "format"}
    subquery_match_keys: Set[str] = set()

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
        self.fields_config: Dict[str, dict] = {}

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

    def get_cache_ignore_keys(self) -> Set[str]:
        """
        Get the set of keys to ignore when generating cache keys.
        
        Returns:
            Set[str]: Set of keys to ignore.
        """
        return self.cache_key_ignore_args
    
    # Higly Encouraged to override this method in subclasses that can handle
    # multiple queries at once, such as BioGRID or KEGG.
    def get_subquery_match_keys(self) -> Set[str]:
        """
        Get the set of keys used for matching queries.
        This is used to determine which keys in the query should be used for generating subqueries.
        Returns:
            Set[str]: Set of keys used for matching queries.
        """
        return self.subquery_match_keys
    
    def _filter_dict_keys(self, input_dict: dict, sort_lists: bool = True) -> dict:
        """
        Filters out keys from a dictionary based on `get_cache_ignore_keys()`.

        Args:
            input_dict (dict): The dictionary to filter.
            sort_lists (bool): If True, sort values that are lists.
        Returns:
            dict: Filtered and optionally transformed dictionary.
        """
        result = {
            k: sorted(v) if sort_lists and isinstance(v, list) else v
            for k, v in sorted(input_dict.items())
            if k not in self.get_cache_ignore_keys()
        }
        return result
    
    def _make_cache_key(self, input_obj: Union[str, dict], **kwargs) -> str:
        """Generate a string key from the input object."""
        # Serialize input_obj based on its type
        if isinstance(input_obj, dict):
            base = json.dumps(self._filter_dict_keys(input_obj), sort_keys=True)
        elif isinstance(input_obj, str):
            base = input_obj
        else:
            base = json.dumps(input_obj, sort_keys=True)

        # Filter kwargs to exclude cache_key_ignore_args
        relevant_kwargs = self._filter_dict_keys(kwargs)
        extra = ""

        # Include relevant kwargs (like 'operation') in the cache key
        if relevant_kwargs:
            #extra = json.dumps(relevant_kwargs, sort_keys=True)
            extra = "_".join(f"{v}" for _, v in relevant_kwargs.items())
        
        if base and extra:
            print(f"Cache_key: {base}_{extra}")
            return f"{base}_{extra}"
        elif base:
            print(f"Cache_key: {base}")
            return base
        # A rarer case where input_obj is empty or None
        else:
            return extra
            

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
    
    def _load_file(self, path: str) -> Union[Dict, pd.DataFrame]:
        """
        Load a file from the cache path.
        Supports both JSON and CSV formats.
        """
        if path.endswith('.csv'):
            return pd.read_csv(path)
        else:
            with open(path, 'r') as f:
                return json.load(f)
    
    def load_cache(self, identifier: str) -> Optional[Dict|pd.DataFrame]:
        """
        Load cached results for a given identifier.
        """
        # Try directly loading from the cache
        cache_path = self._get_cache_path(identifier)
        if os.path.exists(cache_path):
            return self._load_file(cache_path)

        return None

    def save_cache(self, identifier: str, data: Union[List, Dict, pd.DataFrame]) -> None:
        """Save results to cache."""
        path = self._get_cache_path(identifier)

        if isinstance(data, pd.DataFrame):
            data.to_csv(path, index=False)
        else:
            with open(path, 'w') as f:
                json.dump(data, f)

    
    def get_config(self, key: str) -> dict:
        """
        Return the configuration dictionary for a given key (config filename without extension).
        """
        return self.configs.get(key, {})

    def _resolve_fields_from_kwargs(self, include_defaults_from=None, **kwargs) -> Optional[Dict]:
        """
        Resolve fields_to_extract by matching kwargs against keys in fields.yml.

        Automatically checks if any value in kwargs or combination like f"{value}_{mode}" matches a known config key.
        """
        fields_config = self.get_config("fields") if self.use_config else {}
        known_keys = set(fields_config.keys())

        values = [str(v) for v in kwargs.values() if isinstance(v, str)]

        #print(f"Values from kwargs: {values}")

        if include_defaults_from is not None:
            sig = inspect.signature(include_defaults_from)
            for name, param in sig.parameters.items():
                if param.default is not inspect.Parameter.empty and name not in kwargs:
                    default_val = param.default
                    if isinstance(default_val, str):
                        values.append(default_val)

        # 1. Check if any value in kwargs matches a known key
        for v in values:
            #print(f"Trying to resolve fields from kwargs: {v}")
            #print(f"Known keys: {known_keys}")
            if v in known_keys:
                return fields_config[v]
    
        # 2. Check combinations of values in kwargs
        for v1, v2 in permutations(values, 2):
            composite = f"{v1}_{v2}"
            if composite in known_keys:
                return fields_config[composite]
        
        return None

    def _maybe_parse(self, data, parse: bool, to_dataframe: bool = False, **kwargs) -> Union[List, Dict, pd.DataFrame]:
        config_key = kwargs.pop("config_key", None)
        fields_to_extract = kwargs.pop("fields_to_extract", None)

        if parse:
            if not fields_to_extract and self.use_config:
                # 1. Try to resolve fields from kwargs
                fields_to_extract = self._resolve_fields_from_kwargs(include_defaults_from=self.fetch, **kwargs)

                # 2. If not found, try to get from config
                if not fields_to_extract and config_key:
                    fields_to_extract = self.get_config(config_key) or None

            if isinstance(data, list):
                result = [self.parse(data=d, fields_to_extract=fields_to_extract, **kwargs) for d in data]
            elif isinstance(data, (dict, str)):
                # str is the case of KEGG API, which returns a string, parse method should handle it
                result =  self.parse(data=data, fields_to_extract=fields_to_extract, **kwargs)
            else:
                raise ValueError("Data must be a list, dictionary, or string for parsing.")
        else:
            result = data
        
        # Convert to DataFrame if requested
        if to_dataframe:
            # TODO: Make sure parse method returns a consistent structure
            if isinstance(result, list):
                return pd.DataFrame(result)
            elif isinstance(result, dict):
                return pd.DataFrame([result])
            else:
                raise ValueError(f"Cannot convert to DataFrame: unsupported type {type(result)}")

        return result
    
    ##################
    # These 3 methods, decompose_query, get_matching_values, and split_results_by_subquery
    # are used to handle complex queries that can be decomposed into subqueries.
    # They allow the API to handle queries that can be split into smaller parts,
    # fetch results for each part, and then combine them back.

    # These are used by special cases of APIs that can handle complex queries,
    # such as BioGRID, which can take a list of genes and return interactions for each
    # gene separately.
    ##################
        
    def decompose_query(self, query: dict) -> Optional[List[Tuple[str, dict]]]:
        """
        Decompose a query into multiple subqueries if any of the identity keys contain lists.
        Returns:
            List of (identifier, subquery) tuples or None if no decomposition is needed.
        """
        keys = self.get_subquery_match_keys()

        # Determine which keys contain lists
        list_keys = [k for k in keys if isinstance(query.get(k), list)]
        scalar_keys = [k for k in keys if k in query and not isinstance(query[k], list)]

        if not list_keys:
            return None  # No decomposition needed
        
        # Collect values for product
        value_combinations = list(itertools.product(*(query[k] for k in list_keys)))

        subqueries = []
        for combo in value_combinations:
            subquery = query.copy()
            identifier_parts = []

            # Set values from the list_keys
            for key, value in zip(list_keys, combo):
                subquery[key] = value
                identifier_parts.append(str(value))

            # Include scalar_keys in identifier
            for key in scalar_keys:
                identifier_parts.append(str(query[key]))

            identifier = "_".join(identifier_parts)
            subqueries.append((identifier, subquery))

        return subqueries
            
    def get_matching_values(self, query: dict) -> List[str]:
        """
        Extract values from the subquery that will be used for matching items
        in the full result. Relies on self.subquery_match_keys defined in subclass.
        """
        keys = self.get_subquery_match_keys()

        if not keys:
            keys = [k for k in query if k not in self.get_cache_ignore_keys()]

        return [
            str(query[k]).lower() for k in keys if k in query and query[k] is not None
        ]


    def split_results_by_subquery(
        self, full_result: Any, subqueries: List[Tuple[str, dict]]
    ) -> Dict[str, List[dict]]:
        """
        Generic implementation: for each result, check if any subquery's values appear in the result
        using regex-based partial matching.

        Returns a mapping {id_: [results]}.
        """
        print(full_result)
        if not isinstance(full_result, list):
            raise ValueError("Could not split the query into subqueries. Expected full_result to be a list of dicts")

        mapping = {identifier: [] for identifier, _ in subqueries}

        # Auxiliary function to normalize values, special case for KEGG
        def normalize(val):
            if not val:
                return []
            if isinstance(val, (list, tuple)):
                tokens = val
            else:
                tokens = re.split(r"[\s:\-/|]", str(val))
            
            return [t.lower() for t in tokens if t]

        # Preprocess values to search for each subquery
        subquery_values = {
            identifier: sum((normalize(v) for v in self.get_matching_values(query)), [])
            for identifier, query in subqueries
        }


        for item in full_result:
            item_str = item if isinstance(item, str) else json.dumps(item)
            item_str = item_str.lower()  # Ensure case-insensitive matching

            for identifier, values in subquery_values.items():
                if all(
                    re.search(re.escape(str(v).lower()), item_str)
                    for v in values if v
                ):
                    mapping[identifier].append(item)

        return mapping

    ###################
    # General-purpose fetch methods
    # These methods are used to fetch data from the API, either for a single query or
    # a batch of queries. They handle caching, parsing, and optional DataFrame conversion.
    ###################     
    
    def fetch_single(self, query: Union[str, dict], parse: bool = False, *args, **kwargs) -> Union[List, Dict, pd.DataFrame]:
        """
        General-purpose fetch method with optional parsing and cache handling.

        Args:
            query (Union[str, dict]): Query to fetch data for.
            parse (bool): Whether to parse the fetched data.
        
        kwargs:
            config_key (str): Key to use for configuration settings.
            fields_to_extract (Optional[Union[list, dict]]): Fields to extract from the fetched data.
            to_dataframe (bool): Whether to convert the result to a DataFrame.
        Returns:
            Any: Fetched data, parsed if requested.
        """
        to_dataframe = kwargs.get("to_dataframe", False)
        
        if isinstance(query, dict):
            subqueries = self.decompose_query(query)
        elif isinstance(query, list) and all(isinstance(q, str) for q in query):
            # If query is a list of strings, treat it as a single query with identifiers
            subqueries = [(q, {"identifiers": [q]}) for q in query]
        else:
            subqueries = None

        if subqueries:

            raw_results = {}

            # Determine which subqueries are already cached
            uncached_subqueries = []
            for identifier, subquery in subqueries:
                cache_key = self._make_cache_key(identifier, **kwargs)
                if self.has_results(cache_key):
                    cached = self.load_cache(cache_key)
                    data = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
                    parsed = self._maybe_parse(data=data, parse=parse, **kwargs)
                    raw_results[identifier] = parsed
                else:
                    uncached_subqueries.append((identifier, subquery))

            # If any are missing, fetch full result and split
            if uncached_subqueries:
                full_result = self.fetch(query=query, *args, **kwargs)
                # Mapping contains the results split by subquery identifiers
                if not full_result:
                    print("No results found for the provided query. Returning empty results.")
                    return {}
                mapping = self.split_results_by_subquery(full_result, uncached_subqueries)

                for identifier, _ in uncached_subqueries:
                    partial_result = mapping.get(identifier, [])
                    if not partial_result:
                        print(f"No results found for identifier {identifier}. Skipping.")
                        continue  # Skip if no results for this identifier
                    cache_key = self._make_cache_key(identifier, **kwargs)
                    self.save_cache(cache_key, partial_result)
                    parsed = self._maybe_parse(data=partial_result, parse=parse, **kwargs)
                    raw_results[identifier] = parsed

            if to_dataframe and raw_results:
                return pd.concat(raw_results.values(), ignore_index=True) if all(isinstance(r, pd.DataFrame) for r in raw_results.values()) else pd.DataFrame(raw_results)
            else:
                return list(raw_results.values())

        else:
            # No decomposition needed
            cache_key = self._make_cache_key(query, **kwargs)

            if self.has_results(cache_key):
                cached = self.load_cache(cache_key)
                result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
            else:
                result = self.fetch(query=query, *args, **kwargs)
                if result:
                    self.save_cache(cache_key, result)

            result = self._maybe_parse(data=result, parse=parse, **kwargs)
            return result if result is not None else {}
        
        # cache_key = self._make_cache_key(query, **kwargs)
        # print(f"Cache key for fetch_single: {cache_key}")

        # if self.has_results(cache_key):
        #     cached = self.load_cache(cache_key)
        #     result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
        # else:
        #     result = self.fetch(query=query, *args, **kwargs)
        #     if result:
        #         self.save_cache(cache_key, result)
        
        # result = self._maybe_parse(data=result, parse=parse, **kwargs)

        # return result if result is not None else {}
    
    def fetch_batch(self, queries: List[Union[str, dict]], parse: bool = False, *args, **kwargs) -> Union[List, pd.DataFrame]:
        """
        Fetch data in parallel for a batch of queries.
        Args:
            queries (List[Union[str, dict, list[str]]]): List of queries to fetch data for.
            parse (bool): Whether to parse the fetched data.

        kwargs:
            config_key (str): Key to use for configuration settings.
            fields_to_extract (Optional[Union[list, dict]]): Fields to extract from the fetched data.
            to_dataframe (bool): Whether to convert the result to a DataFrame.
        Returns:
            List: List of fetched data, parsed if requested.
        """
        results: List[Any] = [None] * len(queries)

        # Separate queries in cache and not in cache
        queries_to_fetch = []
        index_query_map = {}

        ###############################
        ## Cache handling
        ###############################
        for i, query in enumerate(queries):
            if isinstance(query, dict):
                subqueries = self.decompose_query(query)
            elif isinstance(query, list) and all(isinstance(q, str) for q in query):
                subqueries = [(q, {"identifiers": [q]}) for q in query]
            else:
                subqueries = None

            if subqueries:
                all_cached = True
                partial_results = {}
                for identifier, subquery in subqueries:
                    cache_key = self._make_cache_key(identifier, **kwargs)
                    if self.has_results(cache_key):
                        cached = self.load_cache(cache_key)
                        result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
                        parsed = self._maybe_parse(data=result, parse=parse, **kwargs)
                        partial_results[identifier] = parsed
                    else:
                        all_cached = False

                if all_cached:
                    # All results are available in cache
                    results[i] = list(partial_results.values())
                else:
                    # Some results are not cached, need to fetch
                    index_query_map[i] = query
                    queries_to_fetch.append(query)
            else:
                # No subqueries, use the classic key
                cache_key = self._make_cache_key(query, **kwargs)
                if self.has_results(cache_key):
                    cached = self.load_cache(cache_key)
                    result = cached.to_dict(orient='records') if isinstance(cached, pd.DataFrame) else cached
                    results[i] = self._maybe_parse(data=result, parse=parse, **kwargs)
                else:
                    index_query_map[i] = query
                    queries_to_fetch.append(query)

        #############################
        # If all queries are cached, return the results
        # It's important to note that creating threads takes time, so if all queries are cached,
        # it's better to return the results directly.
        # If there is an incorrect cache key handling then it's better to do a better implementation
        #############################
        # Fetch missing ones in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self.fetch_single, query, parse, *args, **kwargs): i
                # Change it to index_query_map.items() if That part is needed
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

        # Patch solution. Make sure that it works as intended
        # If it's a list of dataframes, concatenate them
        if all(isinstance(r, pd.DataFrame) for r in results) and len(results) > 0:
            batch_data = pd.concat(results, ignore_index=True)
        else:
            batch_data = results
        
        return batch_data
    
    ###################
    # Auxiliary methods
    ###################

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
    def fetch(self, query: Union[str, dict, list], *, method: str, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def parse(self, data: Any, fields_to_extract: Optional[Union[list, dict]], **kwargs):
        raise NotImplementedError
      
        