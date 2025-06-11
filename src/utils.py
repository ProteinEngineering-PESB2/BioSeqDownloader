from typing import Any, Dict, List, Union
import re
import pandas as pd

# Specific extraction functions
def extract_simple(value: Any) -> Any:
    """Extracts a simple value from the data"""
    return value

def extract_ec_numbers(ec_data: List) -> List[str]:
    """Extracts EC numbers"""
    return [ec['value'] for ec in ec_data] if isinstance(ec_data, list) else []

def extract_database_terms(xrefs: List, database: str) -> List[str]:
    """Extracts database terms"""
    return [x['id'] for x in xrefs if isinstance(x, dict) and x.get('database') in database]

def extract_references(refs: List) -> List[Dict]:
    """Extracts references"""
    extracted = []
    for ref in refs if isinstance(refs, list) else []:
        citation = ref.get('citation', {})
        extracted.append({
            'title': citation.get('title'),
            'authors': citation.get('authors', []),
            'journal': citation.get('journal'),
            'pub_date': citation.get('publicationDate'),
            'pmid': next((x['id'] for x in citation.get('citationCrossReferences', []) 
                        if x.get('database') == 'PubMed'), None)
        })
    return extracted

def extract_features(features: List) -> List[Dict]:
    """Extracts protein features"""
    return [{
        'type': f.get('type'),
        'description': f.get('description', ''),
        'location': f.get('location', {})
    } for f in features if isinstance(features, list)]

def extract_keywords(keywords: List) -> List[str]:
    """Extracts keywords"""
    return [kw.get('name', '') for kw in keywords if isinstance(keywords, list)]

### Useful functions ###

def camel_to_snake(name: str) -> str:
    """
    Convert str from camelCase to snake_case.
    Args:
        name (str): String in camelCase.
    Returns:
        str: String in snake_case.
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def get_nested(data: Union[dict, list], path: str, sep: str = ".") -> Any:
    """
    Get a nested value from a dictionary or list given a specific path.
    Args:
        data (Union[dict, list]): Dictionary or list to search.
        path (str): path to the desired value.
        sep (str): Separator used in the path. Default is '.'.
    Returns:
        Any: Value at the specified path, or None if not found.
    """
    if not path:
        return data

    search_key = path.split(".")[0]
    
    for key, value in data.items():
        if key == search_key:
            if isinstance(value, dict):
                return get_nested(value, ".".join(path.split(".")[1:]))
            elif isinstance(value, list):
                lst = [get_nested(item, ".".join(path.split(".")[1:])) for item in value] 
                if len(lst) == 1:
                    return lst[0]
                else:
                    return lst
            else:
                return value

    return None

def get_feature_keys(data: dict, sep: str = ".") -> dict:
    """
    Recursively get all keys in a nested dictionary and get the type of the value.
    Use dot notation for nested keys.
    Args:
        data (dict): The dictionary to extract keys from.
        sep (str): The separator to use for nested keys. Default is '.'.
    Returns:
        list: List of keys with their types.
    """
    keys = {}
    if data is None:
        return keys
    
    
    if isinstance(data, list):
        data = data[0]  # Use the first element of the list to determine the type
    

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                nested_keys = get_feature_keys(value, sep=sep)
                for nested_key, nested_value in nested_keys.items():
                    keys[f"{key}{sep}{nested_key}"] = f"{type(value).__name__}({nested_value})"   
            if isinstance(value, list):
                # Use the first element of the list to determine the type
                if isinstance(value[0], dict):
                    nested_keys = get_feature_keys(value[0], sep=sep)
                    for nested_key, nested_value in nested_keys.items():
                        keys[f"{key}{sep}{nested_key}"] = nested_value
                else:
                    keys[key] = f"list({type(value[0]).__name__})"
            else:
                keys[key] = type(value).__name__
    else:
        keys["value"] = type(data).__name__
    return keys
