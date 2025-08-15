import ast, argparse
import os
import yaml
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd 

# TODO revisar imports y ver si se pueden optimizar
from src.uniprot import UniprotInterface
from src.alphafold import AlphafoldInterface
from src.biodbnet import BioDBNetInterface
from src.brenda import BrendaInstance
from src.biogrid import BioGRIDInterface
from src.chembl import ChEMBLInterface
from src.chebi import ChEBIInterface
from src.interpro import InterproInstance
from src.genontology import GenOntologyInterface
from src.kegg import KEGGInterface
from src.pathwaycommons import PathwayCommonsInterface
from src.panther import PantherInterface
from src.pathwaycommons import PathwayCommonsInterface
from src.proteindatabank import PDBInterface
from src.pubchem import PubChemInterface
from src.reactome import ReactomeInstance
from src.rhea import RheaInterface
from src.refseq import RefSeqInterface
from src.stringdb import StringInterface

biogrid_api_key = None
brenda_email = None
brenda_password = None

INTERFACE_CLASSES = {
    "alphafold": AlphafoldInterface,
    "biogrid": BioGRIDInterface,
    "biodbnet": BioDBNetInterface,
    "brenda": BrendaInstance,
    "chembl": ChEMBLInterface,
    "chebi": ChEBIInterface,
    "genontology": GenOntologyInterface,
    "interpro": InterproInstance,
    "kegg": KEGGInterface,
    "panther": PantherInterface,
    "pathwaycommons": PathwayCommonsInterface,
    "pdb": PDBInterface,
    "pubchem": PubChemInterface,
    "reactome": ReactomeInstance,
    "rhea": RheaInterface,
    "refseq": RefSeqInterface,
    "string": StringInterface
}


##########################################
# Query Builders
###########################################
QUERY_BUILDERS = {}

def register_query_builder(database, method, option=None):
    """
    Registra una función constructora de queries (query builder) en QUERY_BUILDERS.

    Args:
        database (str): Nombre de la base de datos (e.g., 'biodbnet').
        method (str): Método/endpoint principal (e.g., 'db2db').
        option (str, optional): Subopción del endpoint, si aplica (e.g., 'full', 'summary').

    Uso:
        @register_query_builder("biodbnet", "db2db")
        def build_biodbnet_db2db_query(...):
            ...

        @register_query_builder("uniprot", "search", "reviewed")
        def build_uniprot_search_reviewed_query(...):
            ...
    """
    def decorator(func):
        key = "_".join([part for part in (database, method, option) if part])
        QUERY_BUILDERS[key] = func
        return func
    return decorator

def get_query_builder(database, method, option=None):
    """
    Obtiene el query builder registrado para una base de datos y método dados.
    """
    key = "_".join([part for part in (database, method, option) if part])
    builder = QUERY_BUILDERS.get(key)
    if builder is None:
        raise ValueError(f"No query builder registered for endpoint '{key}'")
    return builder


@register_query_builder("biodbnet", "db2db")
def build_query_biodbnet_db2db(row, params):
    if pd.isna(row.get("gene_primary")) or pd.isna(row.get("taxon_id")):
        return []
    
    gene_primary = (
        ast.literal_eval(row["gene_primary"])
        if isinstance(row["gene_primary"], str) and row["gene_primary"].startswith("[")
        else [row["gene_primary"]]
    )
    return [{
        "inputValues": gene_primary,
        "taxonId": row["taxon_id"],
        **params
    }]


@register_query_builder("biodbnet", "getpathways")
def build_query_biodbnet_getpathways(row, params):
    if pd.isna(row.get("taxon_id")):
        return []
    
    return [{
        "taxonId": row["taxon_id"],
        **params
    }]

@register_query_builder("biogrid", "interactions")
def build_query_biogrid_interactions(row, params):
    if not pd.isna(row.get("gene_primary")) and not pd.isna(row.get("taxon_id")):
        gene_primary = (
            ast.literal_eval(row["gene_primary"])
            if isinstance(row["gene_primary"], str) and row["gene_primary"].startswith("[")
            else [row["gene_primary"]]
        )
        return_param = {
            "accessKey": params["accessKey"],
            "geneList": gene_primary,
            "taxId": str(row["taxon_id"]),
            **params,
        }
    elif not pd.isna(row.get("biogrid_ids")):
        biogrid_ids = (
            ast.literal_eval(row["biogrid_ids"])
            if isinstance(row["biogrid_ids"], str) and row["biogrid_ids"].startswith("[")
            else [row["biogrid_ids"]]
        )
        return_param = [
            {
                "accessKey": params["accessKey"],
                "id": biogrid_id,
                **params,
            } for biogrid_id in biogrid_ids
        ]
    else:
        return_param = []
    
    return return_param


@register_query_builder("brenda", "getKmValue")
@register_query_builder("brenda", "getIc50Value")
@register_query_builder("brenda", "getKcatKmValue")
@register_query_builder("brenda", "getKiValue")
@register_query_builder("brenda", "getPhRange")
@register_query_builder("brenda", "getPhOptimum")
@register_query_builder("brenda", "getPhStability")
@register_query_builder("brenda", "getCofactor")
@register_query_builder("brenda", "getTemperatureOptimum")
@register_query_builder("brenda", "getTemperatureStability")
@register_query_builder("brenda", "getTemperatureRange")
def build_query_brenda(row, params):
    # Check if column brenda_ids is not empty 
    if not pd.isna(row.get("brenda_ids")):
        ec_list = ast.literal_eval(row["brenda_ids"]) if row["brenda_ids"].startswith("[") else [row["brenda_ids"]]
        return [{
            "ecNumber": ec,
            "organism": row["organism_name"]
            , **params
        } for ec in ec_list]
    else:
        return []
    
@register_query_builder("chembl", "activity")
@register_query_builder("chembl", "binding_site")
def build_query_chembl(row, params):
    if not pd.isna(row.get("chembl_ids")):
        ids = ast.literal_eval(row["chembl_ids"]) if row["chembl_ids"].startswith("[") else [row["chembl_ids"]]
        return [{
            "target_chembl_id": chembl_id,
            **params
        } for chembl_id in ids]
    else:
        return []
    
@register_query_builder("chebi", "compounds")
def build_query_chebi_compounds(row, params):
    group_of = 5
    if not pd.isna(row.get("chebi_ids")):
        ids = ast.literal_eval(row["chebi_ids"]) if row["chebi_ids"].startswith("[") else [row["chebi_ids"]]
        return [{
            "chebi_ids": ids[i: i + group_of],
            **params
        } for i in range(0, len(ids), group_of)]
    else:
        return []

@register_query_builder("chebi", "ontology-children")
@register_query_builder("chebi", "ontology-parents")
def build_query_chebi_ontology(row, params):
    if not pd.isna(row.get("chebi_ids")):
        ids = ast.literal_eval(row["chebi_ids"]) if row["chebi_ids"].startswith("[") else [row["chebi_ids"]]
        return [{
            "chebi_id": chebi_id,
            **params
        } for chebi_id in ids]
    else:
        return []

@register_query_builder("genontology", "bioentity-function")
@register_query_builder("genontology", "ontology-term")
def build_query_genontology(row, params):
    if not pd.isna(row.get("go_terms")):
        go_terms = ast.literal_eval(row["go_terms"]) if row["go_terms"].startswith("[") else [row["go_terms"]]
        return go_terms

@register_query_builder("interpro", "entry")
def build_query_interpro(row, params):
    if not pd.isna(row.get("interpro_ids")):
        interpro_ids = ast.literal_eval(row["interpro_ids"]) if row["interpro_ids"].startswith("[") else [row["interpro_ids"]]
        return [{
            "id": interpro_id,
            "db": "InterPro",
            "modifiers": {},
            **params
        } for interpro_id in interpro_ids]
    elif not pd.isna(row.get("accession")) and not pd.isna(row.get("taxon_id")):
        # If accession and taxon_id are present, use them to fetch InterPro entries
        return [{
            "db": "InterPro",
            "modifiers": {},
            "filters": [
                {
                    "type": "protein",
                    "db": "reviewed",
                    "value": row["accession"]
                },
                {
                    "type": "taxonomy",
                    "db": "uniprot",
                    "value": str(row["taxon_id"])
                }
            ],
            **params
        }]
    else:
        return []

@register_query_builder("kegg", "get")
def build_query_kegg(row, params):
    if not pd.isna(row.get("kegg_ids")):
        kegg_ids = ast.literal_eval(row["kegg_ids"]) if row["kegg_ids"].startswith("[") else [row["kegg_ids"]]
        return [{
            "entries": kegg_id,
            **params
        } for kegg_id in kegg_ids]
    else:
        return []
    
@register_query_builder("panther", "familymsa")
def build_query_panther_familymsa(row, params):
    if not pd.isna(row.get("panther_ids")):
        panther_ids = ast.literal_eval(row["panther_ids"]) if row["panther_ids"].startswith("[") else [row["panther_ids"]]
        return [{
            "family": panther_id,
            **params
        } for panther_id in panther_ids]
    else:
        return []
    
@register_query_builder("panther", "geneinfo")
def build_query_panther_geneinfo(row, params):
    if not pd.isna(row.get("gene_primary")) and not pd.isna(row.get("taxon_id")):
        gene_primary = (
            ast.literal_eval(row["gene_primary"])
            if isinstance(row["gene_primary"], str) and row["gene_primary"].startswith("[")
            else [row["gene_primary"]]
        )
        return {
            "geneInputList": gene_primary,
            "organism": str(row["taxon_id"]),
            **params
        }
    else:
        return []

@register_query_builder("pathwaycommons", "fetch")
def build_query_pathwaycommons_fetch(row, params):
    if not pd.isna(row.get("reactome_ids")):
        reactome_ids = ast.literal_eval(row["reactome_ids"]) if row["reactome_ids"].startswith("[") else [row["reactome_ids"]]
        return [{
            "uri": [reactome_id],
            **params
        } for reactome_id in reactome_ids]

@register_query_builder("pathwaycommons", "top_pathways")
def build_query_pathwaycommons_top_pathways(row, params):
    if not pd.isna(row.get("gene_primary")) and not pd.isna(row.get("taxon_id")):
        gene_primary = (
            ast.literal_eval(row["gene_primary"])
            if isinstance(row["gene_primary"], str) and row["gene_primary"].startswith("[")
            else [row["gene_primary"]]
        )
        return [{
            "q": gene,
            "organism": [str(row["taxon_id"])],
            **params
        } for gene in gene_primary]

@register_query_builder("pathwaycommons", "neighborhood")
def build_query_pathwaycommons_neighborhood(row, params):
    if not pd.isna(row.get("accession")) and not pd.isna(row.get("taxon_id")):
        return {
            "source": [row["accession"]],
            "organism": [str(row["taxon_id"])],
            **params
        }
    else:
        return []
    
@register_query_builder("pdb", "entry")
def build_query_pdb(row, params):
    if not pd.isna(row.get("pdb_ids")):
        pdb_ids = ast.literal_eval(row["pdb_ids"]) if row["pdb_ids"].startswith("[") else [row["pdb_ids"]]
        return pdb_ids
    else:
        return []

@register_query_builder("pubchem", "compound", "summary")
def build_query_pubchem_compound_summary(row, params):
    if not pd.isna(row.get("gene_primary")) and not pd.isna(row.get("taxon_id")):
        gene_primary = (
            ast.literal_eval(row["gene_primary"])
            if isinstance(row["gene_primary"], str) and row["gene_primary"].startswith("[")
            else [row["gene_primary"]]
        )
        return [{
            "genesymbol": gene,
            "taxid": str(row["taxon_id"]),
            **params
        } for gene in gene_primary]
    else:
        return []

@register_query_builder("pubchem", "protein", "summary")
@register_query_builder("pubchem", "protein", "concise")
def build_query_pubchem_protein(row, params):
    if not pd.isna(row.get("accession")):
        return [{
            "accession": row["accession"],
            **params
        }]
    else:
        return []

@register_query_builder("reactome", "data-discover")
def build_query_reactome(row, params):
    if not pd.isna(row.get("reactome_ids")):
        reactome_ids = ast.literal_eval(row["reactome_ids"]) if row["reactome_ids"].startswith("[") else [row["reactome_ids"]]
        return reactome_ids
    else:
        return []
    

@register_query_builder("rhea", "rhea")
def build_query_rhea(row, params):
    if not pd.isna(row.get("rhea_ids")):
        rhea_ids = ast.literal_eval(row["rhea_ids"]) if row["rhea_ids"].startswith("[") else [row["rhea_ids"]]
        return [{
            "query": rhea_id,
            **params
        } for rhea_id in rhea_ids]

@register_query_builder("refseq", "protein")
def build_query_refseq(row, params):
    if not pd.isna(row.get("refseq_ids")):
        refseq_ids = ast.literal_eval(row["refseq_ids"]) if row["refseq_ids"].startswith("[") else [row["refseq_ids"]]
        return refseq_ids
    else:
        return []

@register_query_builder("string", "interaction_partners")
@register_query_builder("string", "get_string_ids")
def build_query_stringdb(row, params):
    if not pd.isna(row.get("string_ids")) and not pd.isna(row.get("taxon_id")):
        string_ids = ast.literal_eval(row["string_ids"]) if row["string_ids"].startswith("[") else [row["string_ids"]]
        return [{
            "identifiers": string_id,
            "species": row["taxon_id"],
            **params
        } for string_id in string_ids]
    elif not pd.isna(row.get("gene_primary")) and not pd.isna(row.get("taxon_id")):
        gene_primary = (
            ast.literal_eval(row["gene_primary"])
            if isinstance(row["gene_primary"], str) and row["gene_primary"].startswith("[")
            else [row["gene_primary"]]
        )
        return [{
            "identifiers": gene,
            "species": row["taxon_id"],
            **params
        } for gene in gene_primary]
    else:
        return []

##########################################
# Generic Fetch
##########################################

def search_and_merge(row, instance, database_name, endpoint, params, option=None):
    # Construir clave de búsqueda: {database}_{method}_{option}
    if option:
        key = f"{database_name}_{endpoint}_{option}"
    else:
        key = f"{database_name}_{endpoint}"

    # Buscar builder
    query_builder = QUERY_BUILDERS.get(key)
    if not query_builder:
        raise ValueError(f"No query builder registered for key '{key}'")

    # Construir query
    query_params = query_builder(row, params)

    method_params = {
        "method": endpoint,
        "parse": True,
        "to_dataframe": True

    }
    # Previene el error en genontology al receibir option
    if option:
        method_params["option"] = option

    if isinstance(query_params, dict) \
        or ( isinstance(query_params, list) and len(query_params) == 1):
        # Si es una lista de un solo dict, usar ese dict directamente
        query_params = query_params[0] if isinstance(query_params, list) else query_params
        result = instance.fetch_single(
            query=query_params,
            **method_params
        )
    elif isinstance(query_params, list) and len(query_params) > 1:
        # Si es una lista de varios dicts, usarla como está
        result = instance.fetch_batch(
            queries=query_params,
            **method_params
        )
    else:
        # Si no es una lista, retornar DataFrame vacío
        return pd.DataFrame()

    # Unir con la fila original
    row_expanded = pd.concat([pd.DataFrame([row] * len(result)).reset_index(drop=True),
                            result.reset_index(drop=True)], axis=1)

    return row_expanded


def process_dataframe(df, database_name, endpoint, instance, params, option=None):
    all_results = df.apply(
        lambda row: search_and_merge(
            row,
            instance,
            database_name,
            endpoint,
            params,
            option=option
        ),
        axis=1
    )

    if all_results.empty:
        print(f"No results found for {database_name} with endpoint {endpoint} and option {option}.")
        return pd.DataFrame()

    # Combinar todos los resultados
    return pd.concat(all_results.tolist(), ignore_index=True)

##########################################
# Main Interface for Cross-references
##########################################
def fetch_crossref(
        database_name: str, 
        df: pd.DataFrame, 
        endpoint: str, 
        option: str = None,
    ):
    if database_name not in INTERFACE_CLASSES:
        raise ValueError(f"Unsupported database: {database_name}")

    # Crear la instancia correcta
    if database_name == "brenda":
        instance = INTERFACE_CLASSES[database_name](
            email=os.getenv("brenda_email"),
            password=os.getenv("brenda_password")
        )
    else:
        instance = INTERFACE_CLASSES[database_name]()

    # Obtener params
    params = get_params(database_name, endpoint)
    if option:
        params["option"] = option

    if database_name == "biogrid":
        biogrid_api_key = os.getenv("biogrid_api_key")
        params["accessKey"] = biogrid_api_key
        
    return process_dataframe(df, database_name, endpoint, instance, params, option=option)

##########################################
# Auxiliary functions
##########################################

def is_enabled(api_name, endpoint_name=None, option=None):
    api_conf = config.get(api_name, {})
    if not api_conf.get("enabled", False):
        return False
    if endpoint_name:
        if option:
            return api_conf.get("endpoints", {}).get(endpoint_name, {}).get("options", {}).get(option, {}).get("enabled", False)
        return api_conf.get("endpoints", {}).get(endpoint_name, {}).get("enabled", False)
    return True

def get_params(api_name, endpoint_name):
    return (
        config.get(api_name, {})
        .get("endpoints", {})
        .get(endpoint_name, {})
        .get("params", {})
    )

def save_to_file(df, out_dir, filename, db, endpoint, option):
    # Make folder with filename
    os.makedirs(os.path.join(out_dir, filename), exist_ok=True)
    # Save the DataFrame to a CSV file
    if option is None:
        output_file = os.path.join(out_dir, f"{filename}/{db}_{endpoint}_results.csv")
    else:
        output_file = os.path.join(out_dir, f"{filename}/{db}_{endpoint}_{option}_results.csv")
    df.to_csv(output_file, index=False)
    print(f"Results for {db} with option {option} saved to {output_file}")

if __name__ == "__main__":
    # Parse arguments from the command line
    parser = argparse.ArgumentParser(description="Run cross-references for UniProt.")
    parser.add_argument("-i", "--input", type=str, required=True, help="Input file with UniProt IDs.")
    parser.add_argument("-o", "--out_dir", type=str, required=True, help="Output directory for results.")
    parser.add_argument("-c", "--config", type=str, default="config/uniprot_crossref/config_endpoints.yml", help="Path to the configuration file for endpoints.")
    parser.add_argument("-d", "--download_structures", action="store_true", help="Download PDB structures.")
    parser.add_argument("--no-concat", action="store_true", help="Do not concatenate results into a single DataFrame.")

    args = parser.parse_args()

    # Check if input file exists
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file {args.input} does not exist.")

    # Load input file into a DataFrame
    try:
        df = pd.read_csv(args.input)
    except Exception as e:
        raise ValueError(f"Error reading input file {args.input}: {e}")

    # Create output directory if it doesn't exist
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    # Load environment variables
    load_dotenv()

    # Load configuration file
    config_path = Path(args.config)
    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    

    filename, _ = os.path.splitext(os.path.basename(args.input))

    if is_enabled("biogrid"):
        biogrid_api_key = os.getenv("biogrid_api_key")
        if not biogrid_api_key:
            raise ValueError("Please set the 'biogrid_api_key' environment variable.")
        
    
    if is_enabled("brenda"):
        brenda_email = os.getenv("brenda_email")
        brenda_password = os.getenv("brenda_password")

    export_df = pd.DataFrame()

    for db in list(config.keys()):
        if not is_enabled(db):
            continue
        print(f"Processing database: {db}")
        for endpoint in config[db].get("endpoints", {}).keys():
            if not is_enabled(db, endpoint):
                continue
            print(f"Using endpoint: {endpoint}")

            if "options" in config[db].get("endpoints", {}).get(endpoint, {}):
                for option in config[db]["endpoints"][endpoint].get("options", {}).keys():
                    if not is_enabled(db, endpoint, option):
                        continue
                    print(f"Using option: {option}")
                    # Fetch data from the database with the specified option
                    tmp_df = fetch_crossref(
                        database_name=db,
                        df=df,
                        endpoint=endpoint,
                        option=option
                    )
                    if isinstance(tmp_df, pd.DataFrame) and not tmp_df.empty:
                        if not args.no_concat:
                            export_df = pd.concat([export_df, tmp_df], axis=1)
                        else:
                            save_to_file(tmp_df, args.out_dir, filename, db, endpoint, option=option)
                    else:
                        print(f"No data fetched for {db} with option {option} or the result is not a DataFrame.")
            else:
                # Fetch data from the database
                tmp_df = fetch_crossref(
                    database_name=db,
                    df=df,
                    endpoint=endpoint
                )
                if not args.no_concat:
                    export_df = pd.concat([export_df, tmp_df], axis=1)
                else:
                    save_to_file(tmp_df, args.out_dir, filename, db, endpoint, option=None)

    if not args.no_concat:
        # Save the concatenated DataFrame to a CSV file
        output_file = os.path.join(args.out_dir, f"{filename}_crossref_results.csv")
        export_df.to_csv(output_file, index=False)
        print(f"Concatenated results saved to {output_file}")