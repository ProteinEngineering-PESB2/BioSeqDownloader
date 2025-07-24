import ast, argparse
import os

from dotenv import load_dotenv
import pandas as pd 

# TODO revisar imports y ver si se pueden optimizar
from src.uniprot import UniprotInterface
from src.alphafold import AlphafoldInterface
from src.brenda import BrendaInstance
from src.brenda import methods as brenda_methods
from src.biogrid import BioGRIDInterface
from src.chembl import ChEMBLInterface
from src.interpro import InterproInstance
from src.genontology import GenOntologyInterface
from src.kegg import KEGGInterface
from src.proteindatabank import PDBInterface
from src.reactome import ReactomeInstance
from src.refseq import RefSeqInterface
from src.stringdb import StringInterface

outside_db = {
    "alphafold": "xref_alphafolddb",
    "biogrid": "xref_biogrid",
    "brenda": "xref_brenda",
    "go": "go_id",
    "interpro": "xref_interpro",
    "kegg": "xref_kegg",
    "pfam": "xref_pfam",
    "pdb": "xref_pdb",
    "reactome": "xref_reactome",
    "refseq": "xref_refseq",   
    "string": "xref_string",
}

biogrid_api_key = None
brenda_email = None
brenda_password = None

def parse_ids(df, db_name):
    raw_ids = df.get(f"{db_name}_ids", pd.Series(dtype=str)).dropna().unique().tolist()
    ids = []
    for entry in raw_ids:
        parsed = ast.literal_eval(entry) if isinstance(entry, str) and entry.startswith("[") else [entry]
        ids.extend(parsed)

    # Cleaning: remove "[]"
    return [id for id in ids if id != "[]"]

# Fetch data from alphafold given a ID from xref_alphafolddb
# Those without alphafold_ids are skipped
def fetch_alphafold(df, method: str):
    instance = AlphafoldInterface(
        structures=['pdb'],
        output_dir="results"
    )

    ids = parse_ids(df, "alphafold")
    if not ids:
        return []
    
    # TODO: Remove Limit
    print(f"{len(ids)} IDs to fetch from alphafold")
    return instance.fetch_batch(queries=ids[:4], parse=True, to_dataframe=True)

# Fetch data from biogrid given a list of IDs from xref_biogrid or gene_primary and taxon_id fields
def fetch_biogrid(df, method: str):
    instance = BioGRIDInterface()
    queries = []

    # Part 1: by ID if they exist
    ids = parse_ids(df, "biogrid")
    if ids:
        queries.extend([{
            "id": id_,
            "accessKey": biogrid_api_key
        } for id_ in ids])

    # Part 2: build queries by taxId and geneList
    tmp_df = df[df["biogrid_ids"].astype(str).isin(["[]", "nan", "NaN", ""])]
    tmp_df = tmp_df.dropna(subset=["gene_primary", "taxon_id"])
    tmp_df = tmp_df[["gene_primary", "taxon_id"]].drop_duplicates()

    tmp_df["gene_primary"] = tmp_df["gene_primary"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else [x]
    )

    tmp_df = tmp_df.explode("gene_primary")
    grouped = tmp_df.groupby("taxon_id")["gene_primary"].agg(list).reset_index()

    queries.extend([
        {
            "accessKey": biogrid_api_key,
            "geneList": row["gene_primary"],
            "taxId": row["taxon_id"],
            "format": "json"
        }
        for _, row in grouped.iterrows()
    ])

    print(f"{len(queries)} queries to fetch from biogrid")

    return instance.fetch_batch(
        queries=queries[:4],  # TODO: Remove Limit
        method=method,
        parse=True,
        to_dataframe=True
    )

# Those without brenda_ids are skipped
def fetch_brenda(df, method: str):
    instance = BrendaInstance(
        email=brenda_email,
        password=brenda_password
    )

    queries = []

    ids = df[~df["brenda_ids"].astype(str).isin(["[]", "nan", "NaN", ""])]
    ids.loc[:, 'brenda_ids'] = ids["brenda_ids"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else [x]
    )
    ids = ids.explode("brenda_ids").drop_duplicates(subset=["brenda_ids"])
    # TODO Remove Limit
    ids = ids[:4]  # Limit to 4 for testing purposes

    if not ids.empty:
        queries.extend([{
            "ecNumber": id["brenda_ids"],
            "organism": id["organism_name"]
        } for _, id in ids.iterrows() if isinstance(id["brenda_ids"], str) and id["brenda_ids"] != "[]"])

    return instance.fetch_batch(
        queries=queries,
        method=method,
        parse=True,
        to_dataframe=True
    )

def fetch_chembl(df, method: str):
    instance = ChEMBLInterface()

    queries = []

    ids = df[~df["chembl_ids"].astype(str).isin(["[]", "nan", "NaN", ""])]
    ids.loc[:, 'chembl_ids'] = ids["chembl_ids"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else [x]
    )
    ids = ids.explode("chembl_ids").drop_duplicates(subset=["chembl_ids"])
    # TODO Remove Limit
    ids = ids[:4]  # Limit to 4 for testing purposes

    if not ids.empty:
        queries.extend([{
            "target_chembl_id": id["chembl_ids"]
        } for _, id in ids.iterrows() if isinstance(id["chembl_ids"], str) and id["chembl_ids"] != "[]"])

    return instance.fetch_batch(
        queries=queries,
        method=method,
        pages_to_fetch=1,
        parse=True,
        to_dataframe=True
    )
    
# Those without go_terms are skipped
# TODO: Retorna una lista anidada y por eso no puedo hacerlo un dataframe ver que pasa
def fetch_genontology(df, method: str):
    interface = GenOntologyInterface()
    queries = []
    ids = df["go_terms"].dropna().unique().tolist()
    ids = [ast.literal_eval(id) if isinstance(id, str) and id.startswith("[") else [id] for id in ids]
    ids = [item for sublist in ids for item in sublist]  # Flatten
    ids = list(set(ids))  # Remove duplicates

    # TODO Remove Limit
    ids = ids[:4]  # Limit to 4 for testing purposes
    
    # TODO It can be also added more methods to fetch
    return interface.fetch_batch(
        method=method,
        queries=ids,
        option=None,
        parse=True,
        to_dataframe=True
    )

def fetch_interpro(df, method: str):
    instance = InterproInstance()

    queries = []

    ids = df[~df["interpro_ids"].astype(str).isin(["[]", "nan", "NaN", ""])]
    ids.loc[:, 'interpro_ids'] = ids["interpro_ids"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else [x]
    )
    ids = ids.explode("interpro_ids").drop_duplicates(subset=["interpro_ids"])

    if not ids.empty:
        queries.extend([{
            "id": id["interpro_ids"],
            "db": "InterPro",
            "modifiers": {},
        } for _, id in ids.iterrows() if isinstance(id["interpro_ids"], str) and id["interpro_ids"] != "[]"])


    tmp_df = df[df["interpro_ids"].astype(str).isin(["[]", "nan", "NaN", ""])]
    tmp_df = df[["accession", "taxon_id"]].drop_duplicates()
    tmp_df = tmp_df.dropna(subset=["accession", "taxon_id"])

    if not tmp_df.empty:
        queries.extend([
            {
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
                        "value": row["taxon_id"]
                    }
                ]

            }
            for _, row in tmp_df.iterrows()
        ])
    
    print(len(queries), "queries to fetch from interpro")

    return instance.fetch_batch(
        queries=queries[:4],  # TODO: Remove Limit
        method=method,
        pages_to_fetch=1,
        parse=True,
        to_dataframe=True
    )

# Those without kegg_ids are skipped
def fetch_kegg(df, method: str):
    instance = KEGGInterface()
    
    queries = []
    ids = parse_ids(df, "kegg")

    if not ids:
        return []
    
    for id in ids:
        queries.append({
            "entries": [id]
        })

    return instance.fetch_batch(
        method=method,
        queries=queries[:4],  # TODO Remove Limit
        parse=True,
        to_dataframe=True
    )

# TODO: PDB retorna una lista anidada y por eso no puedo hacerlo un dataframe ver que pasa
def fetch_pdb(df, method: str):
    instance = PDBInterface(
        download_structures=True,
        return_data_list=["rcsb_id", "rcsb_comp_model_provenance"],
        output_dir="results"
    )
    queries = []
    ids = parse_ids(df, "pdb")
    
    if not ids:
        return []
    
    queries = ids[:4]  # TODO Remove Limit

    return instance.fetch_batch(
        queries=queries,
        parse=True,
        to_dataframe=True
    )

def fetch_reactome(df, method: str):
    instance = ReactomeInstance()
    queries = []
    ids = parse_ids(df, "reactome")

    if not ids:
        return []

    return instance.fetch_batch(
        queries=ids[:4],  # TODO Remove Limit
        parse=True,
        method=method,
        option="",
        to_dataframe=True
    )

def fetch_refseq(df, method: str):
    instance = RefSeqInterface()
    ids = parse_ids(df, "refseq")

    if not ids:
        return []

    # TODO Remove Limit
    queries = ids[:4]

    return instance.fetch_batch(
        queries=queries,
        method=method,
        parse=True,
        to_dataframe=True
    )

# No string_ids this time, only gene_primary and taxon_id
# TODO make sure string_ids are working
def fetch_string(df, method: str):
    instance = StringInterface()
    queries = []
    
    # 1. Fetch by string_ids if they exist
    tmp_df = df[~df["string_ids"].astype(str).isin(["[]", "nan", "NaN", ""])]
    if not tmp_df.empty:
        tmp_df = tmp_df.dropna(subset=["string_ids"])
        tmp_df = tmp_df[["string_ids", "taxon_id"]].drop_duplicates()
        tmp_df["string_ids"] = tmp_df["string_ids"].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else [x]
        )
        tmp_df = tmp_df.explode("string_ids")
        grouped = tmp_df.groupby("taxon_id")["string_ids"].agg(list).reset_index()
        queries.extend([
            {
                "identifiers": row["string_ids"],
                "species": row["taxon_id"]
            }
            for _, row in grouped.iterrows()
        ])
    
    # 2. Build queries by taxon_id and gene_primary
    tmp_df = df[df["string_ids"].astype(str).isin(["[]", "nan", "NaN", ""])]
    tmp_df = tmp_df.dropna(subset=["gene_primary", "taxon_id"])
    tmp_df = tmp_df[["gene_primary", "taxon_id"]].drop_duplicates()

    tmp_df["gene_primary"] = tmp_df["gene_primary"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else [x]
    )

    tmp_df = tmp_df.explode("gene_primary")
    grouped = tmp_df.groupby("taxon_id")["gene_primary"].agg(list).reset_index()

    queries.extend([
        {
            "identifiers": row["gene_primary"],
            "species": row["taxon_id"]
        }
        for _, row in grouped.iterrows()
    ])

    return instance.fetch_batch(
        outfmt="json",
        method=method,
        queries=queries[:4],  # TODO Remove Limit
        parse=True,
        to_dataframe=True
    )

def fetch_crossref(database_name: str, df: pd.DataFrame):
    match database_name:
        case "alphafold":
            return fetch_alphafold(df, "prediction")
        case "biogrid":
            return fetch_biogrid(df, "interactions")
        # Starts with "brenda_" to handle different methods
        case brenda if brenda.startswith("brenda_"):
            method = brenda.split("_")[1]
            return fetch_brenda(df, method=method)
        case "chembl":
            return fetch_chembl(df, "activity")
        case "genontology":
            return fetch_genontology(df, "ontology-term")
        case "interpro":
            return fetch_interpro(df, "entry")
        case "kegg":
            return fetch_kegg(df, "get")
        case "pdb":
            return fetch_pdb(df, "")
        case "reactome":
            return fetch_reactome(df, "data/discover")
        case "refseq":
            return fetch_refseq(df, "protein")
        case "string":
            return fetch_string(df, "interaction_partners")
        case _:
            raise ValueError(f"Unsupported database: {database_name}")

if __name__ == "__main__":
    # Parse arguments from the command line
    parser = argparse.ArgumentParser(description="Run cross-references for UniProt.")
    parser.add_argument("-i", "--input", type=str, required=True, help="Input file with UniProt IDs.")
    parser.add_argument("-o", "--out_dir", type=str, required=True, help="Output directory for results.")
    parser.add_argument("-dbs", "--databases", type=str, default="alphafold,brenda,biogrid,chembl,genontology,interpro,kegg,pdb,reactome,refseq,string")
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

    filename, _ = os.path.splitext(os.path.basename(args.input))
    databases = args.databases.split(",")

    if "biogrid" in databases:
        biogrid_api_key = os.getenv("biogrid_api_key")
        if not biogrid_api_key:
            raise ValueError("Please set the 'biogrid_api_key' environment variable.")
        
    
    if "brenda" in databases:
        brenda_email = os.getenv("brenda_email")
        brenda_password = os.getenv("brenda_password")
        databases.remove("brenda")
        # Add all brenda methods to the databases list
        databases.extend([f"brenda_{method}" for method in brenda_methods.keys()])
        if not brenda_email or not brenda_password:
            raise ValueError("Please set the 'brenda_email' and 'brenda_password' environment variables.")

    export_dfs = {}


    for db in databases:
        print(f"Processing database: {db}")
        tmp_df = fetch_crossref(
            database_name=db,
            df=df
        )
        if isinstance(tmp_df, pd.DataFrame) and not tmp_df.empty:
            if not args.no_concat:
                df = pd.concat([df, tmp_df], axis=1)
                df.to_csv(os.path.join(args.out_dir, f"{filename}_crossref.csv"), index=False)
                print(f"Results for {db} concatenated into {filename}_crossref.csv")
            else:
                # Make folder with filename
                os.makedirs(os.path.join(args.out_dir, filename), exist_ok=True)
                # Save the DataFrame to a CSV file
                output_file = os.path.join(args.out_dir, f"{filename}/{db}_results.csv")
                tmp_df.to_csv(output_file, index=False)
                print(f"Results for {db} saved to {output_file}")
                export_dfs[db] = tmp_df
        else:
            print(f"No data fetched for {db} or the result is not a DataFrame.")