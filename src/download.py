import re
from typing import List, Dict, Any
import pandas as pd 
import argparse
from tqdm import tqdm
from uniprot import UniprotInterface
import math


def identify_id_type(id_str: str) -> str:
    """Identifica el tipo de ID basado en patrones regex"""
    if not isinstance(id_str, str):
        return None
        
    for db_type, config in db_config.items():
        for pattern in config['patterns']:
            if re.fullmatch(pattern, id_str):
                return db_type
            
            return None

def group_ids_by_type(ids: List[str]) -> Dict[str, List[str]]:
    """Agrupa IDs por su tipo detectado"""
    grouped = {db_type: [] for db_type in db_config}
    grouped['unknown'] = []
    
    for id_str in ids:
        if not isinstance(id_str, str):
            continue
            
        id_type = identify_id_type(id_str)
        if id_type in grouped:
            grouped[id_type].append(id_str)
        else:
            grouped['unknown'].append(id_str)
    return grouped


def download_batch(
        dataset: pd.DataFrame, 
        column_ids: str, 
        auto_db: bool, 
        from_db: str, 
        to_db: str, 
        batch_size: int
        ):
    ids = dataset[column_ids].dropna().unique().tolist()

    results = []

    if auto_db:
        # Automatically detect and group IDs
        id_groups = group_ids_by_type(ids)
        
        for db_type, id_list in id_groups.items():
            if not id_list or db_type == 'unknown':
                continue
                
            config = db_config[db_type]
            results = process_id_batch(
                ids=id_list,
                from_db=config['from_db'],
                to_db=config['to_db'],
                batch_size=batch_size,
                db_type=db_type
            )
    else:
        # Manually use the provided from_db/to_db parameters
        results = process_id_batch(
            ids=ids,
            from_db=from_db,
            to_db=to_db,
            batch_size=batch_size,
            db_type='manual'
        )
    
    return results

def process_id_batch(
        
        ids: List[str], 
        from_db: str, 
        to_db: str, 
        batch_size: int, 
        db_type: str
    ):
    """Procesa un lote de IDs de un tipo específico"""
    downloader = UniprotInterface()
    results = []
    progress_bar = tqdm(
        range(0, len(ids)), 
        desc=f"Processing {db_type} IDs", 
        total=len(ids),
        dynamic_ncols=True,
        ncols=0,
        bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {desc}"
    )
    
    for start in range(0, len(ids), batch_size):
        batch = ids[start:start+batch_size]
        job_id = downloader.submit_id_mapping(from_db, to_db, batch)
        
        if downloader.check_id_mapping_results_ready(job_id):
            link = downloader.get_id_mapping_results_link(job_id)
            search = downloader.get_id_mapping_results_search(link)
            
            # Add information about the source to the results
            if isinstance(search, dict):
                for result in search.get('results', []):
                    result['source_db'] = db_type
                results.append(search)
                
        progress_bar.update(len(batch))
    
    return results

def show_results(
        results: List[Dict],
        raw=False
    ):
    if results:
        if raw:
            for result in results:
                print(result)
        else:
            print(f"{len(results)} results to show")
    else:
        print("No results to show")

def parse_results(results: List[Dict]) -> pd.DataFrame:
    export_df = pd.DataFrame()

    for result in results:
        parsed_results = parse(result)
        export_df = pd.concat([export_df, parsed_results], ignore_index=True)

    return export_df

def parse(results: Dict) -> pd.DataFrame:
    """Parse UniProt JSON results into a DataFrame"""
    parsed_data = []
    
    # Process successful results
    for result in results.get('results', []):
        parsed = _parse_result(result)
        parsed['source_db'] = results.get('source_db', 'unknown')
        parsed_data.append(parsed)
        
    # Process failed IDs
    for failed_id in results.get('failedIds', []):
        parsed_data.append({
            'uniprot_id': failed_id,
            'source_db': results.get('source_db', 'unknown'),
            'status': 'failed'
        })
        
    return pd.DataFrame(parsed_data)

def _parse_result(result: Dict) -> Dict:
    """Parse a single UniProt result"""
    parsed = {}
    
    for field, (path, extractor) in field_map.items():
        try:
            # Navigate through the path (e.g. 'to.proteinDescription...')
            data = result
            for key in path.split('.'):
                if key.isdigit():  # For array indices
                    key = int(key)
                data = data.get(key, {})
            
            # Extract the value using the specific function
            parsed[field] = extractor(data) if data else None
        except (KeyError, AttributeError, IndexError):
            parsed[field] = None
            
    return parsed

# Specific extraction functions
def extract_simple(value: Any) -> Any:
    """Extracts a simple value from the data"""
    return value

def extract_ec_numbers(ec_data: List) -> List[str]:
    """Extracts EC numbers"""
    return [ec['value'] for ec in ec_data] if isinstance(ec_data, list) else []

def extract_go_terms(xrefs: List) -> List[str]:
    """Extracts GO terms"""
    return [x['id'] for x in xrefs if isinstance(x, dict) and x.get('database') == 'GO']

def extract_pfam_ids(xrefs: List) -> List[str]:
    """Extracts Pfam IDs"""
    return [x['id'] for x in xrefs if isinstance(x, dict) and x.get('database') == 'Pfam']

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

db_config = {
    'uniprot': {
        'patterns': [r'^[A-N,R-Z][0-9][A-Z][A-Z, 0-9][A-Z, 0-9][0-9]$',
                    r'^[A-N,R-Z][0-9][A-Z][A-Z, 0-9][A-Z, 0-9][0-9][A-Z][A-Z, 0-9][A-Z, 0-9][0-9]$',
                    r'^[OPQ][0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9]$'],
        'from_db': 'UniProtKB_AC-ID',
        'to_db': 'UniProtKB'
    },
    'pdb': {
        'patterns': [r'^[0-9][A-Z0-9]{3}$'],
        'from_db': 'PDB',
        'to_db': 'UniProtKB'
    }
}

field_map = {
    'uniprot_id': ('from', extract_simple),
    'entry_type': ('to.entryType', extract_simple),
    'protein_name': ('to.proteinDescription.recommendedName.fullName.value', extract_simple),
    'ec_numbers': ('to.proteinDescription.recommendedName.ecNumbers', extract_ec_numbers),
    'organism': ('to.organism.scientificName', extract_simple),
    'taxon_id': ('to.organism.taxonId', extract_simple),
    'sequence': ('to.sequence.value', extract_simple),
    'length': ('to.sequence.length', extract_simple),
    'go_terms': ('to.uniProtKBCrossReferences', extract_go_terms),
    'pfam_ids': ('to.uniProtKBCrossReferences', extract_pfam_ids),
    'references': ('to.references', extract_references),
    'features': ('to.features', extract_features),
    'keywords': ('to.keywords', extract_keywords),
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download data from the web')
    parser.add_argument('-i', '--input', help='CSV file with UniProt IDs')
    parser.add_argument('-c', '--column', help='column name with UniProt IDs', default="accession")
    parser.add_argument('-o', '--output', help='Output file')
    parser.add_argument('-f', '--from_db', help='Database to convert from. Default is UniProtKB_AC-ID (UniProtKB_AC-ID, PDB)', default='UniProtKB_AC-ID')
    parser.add_argument('-t', '--to_db', help='Database to convert to', default='UniProtKB')
    parser.add_argument('-b', '--batch_size', help='Batch size for downloading', default=5000)
    parser.add_argument('-a', '--auto_db', help='Automatically detect database type', action='store_true')
    parser.add_argument("--min_identity", type=float, default=90.0, help="Minimum identity threshold for BLAST search.")
    args = parser.parse_args()
    
    df = pd.read_csv(args.input)

    # Filter by identity
    df = df[df['identity'] >= args.min_identity]

    print("Downloading data in batches of", args.batch_size)
    results = download_batch(df, args.column, args.auto_db, args.from_db, args.to_db, args.batch_size)

    # Save raw results
    with open(args.output + ".json", 'w') as f:
        for result in results:
            f.write(str(result) + '\n')

    export_df = parse_results(results)
    export_df.to_csv(args.output, index=False)
