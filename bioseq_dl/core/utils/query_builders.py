import pandas as pd
import ast

from bioseq_dl import (
    AlphafoldInterface,
    BioGRIDInterface,
    BioDBNetInterface,
    BrendaInterface,
    ChEMBLInterface,
    ChEBIInterface,
    GenOntologyInterface,
    InterproInterface,
    KEGGInterface,
    PantherInterface,
    PathwayCommonsInterface,
    PDBInterface,
    PubChemInterface,
    ReactomeInterface,
    RheaInterface,
    RefSeqInterface,
    StringInterface
)


INTERFACE_CLASSES = {
    "alphafold": AlphafoldInterface,
    "biogrid": BioGRIDInterface,
    "biodbnet": BioDBNetInterface,
    "brenda": BrendaInterface,
    "chembl": ChEMBLInterface,
    "chebi": ChEBIInterface,
    "genontology": GenOntologyInterface,
    "interpro": InterproInterface,
    "kegg": KEGGInterface,
    "panther": PantherInterface,
    "pathwaycommons": PathwayCommonsInterface,
    "pdb": PDBInterface,
    "pubchem": PubChemInterface,
    "reactome": ReactomeInterface,
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
