from dataclasses import dataclass
import os
from typing import Optional

POLLING_INTERVAL = 3

DATABASES = {
    'alphafold_ids': 'AlphaFoldDB',
    'biogrid_ids': 'BioGRID',
    'brenda_ids': 'BRENDA',
    'go_terms': 'GO',
    'interpro_ids': 'InterPro',
    'kegg_ids': 'KEGG',
    'pdb_ids': 'PDB',
    'pfam_ids': 'Pfam',
    'reactome_ids': 'Reactome',
    'refseq_ids': 'RefSeq',
    'string_ids': 'STRING',
}

BASE_CACHE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.cache"))
BASE_CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config"))

@dataclass(frozen=True)
class DBConfig:
    API_URL: str = ""
    STRUCTURE_URL: Optional[str] = None
    CACHE_DIR: Optional[str] = None
    CONFIG_DIR: Optional[str] = None

ALPHAFOLD = DBConfig(
    API_URL="https://alphafold.ebi.ac.uk/api/prediction/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "alphafold"),
    CONFIG_DIR=os.path.join(BASE_CONFIG_DIR, "alphafold")
)

BIOGRID = DBConfig(
    API_URL="https://webservice.thebiogrid.org/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "biogrid"),
    CONFIG_DIR=os.path.join(BASE_CONFIG_DIR, "biogrid")
)

BRENDA = DBConfig(
    API_URL = "https://www.brenda-enzymes.org/soap/brenda_zeep.wsdl",
    CACHE_DIR= os.path.join(BASE_CACHE_DIR, "brenda"),
    CONFIG_DIR=os.path.join(BASE_CONFIG_DIR, "brenda")
)

GENONTOLOGY = DBConfig(
    API_URL="https://api.geneontology.org/api/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "genontology"),
    CONFIG_DIR=os.path.join(BASE_CONFIG_DIR, "genontology")
)

KEGG = DBConfig(
    API_URL="https://rest.kegg.jp/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "KEGG"),
    CONFIG_DIR=os.path.join(BASE_CONFIG_DIR, "kegg")
)


PDB = DBConfig(
    API_URL="https://data.rcsb.org/rest/v1/core/",
    STRUCTURE_URL="https://files.rcsb.org/download/",
    CACHE_DIR = os.path.join(BASE_CACHE_DIR, "pdb"),
    CONFIG_DIR= os.path.join(BASE_CONFIG_DIR, "pdb")
)

REACTOME = DBConfig(
    API_URL = "https://reactome.org/ContentService/",
    CACHE_DIR = os.path.join(BASE_CACHE_DIR, "reactome"),
    CONFIG_DIR = os.path.join(BASE_CONFIG_DIR, "reactome")
)

INTERPRO = DBConfig(
    API_URL = "https://www.ebi.ac.uk:443/interpro/api/",
    CACHE_DIR = os.path.join(BASE_CACHE_DIR, "interpro"),
    CONFIG_DIR = os.path.join(BASE_CONFIG_DIR, "interpro")
)

STRING = DBConfig(
    API_URL = "https://string-db.org/api/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "string"),
    CONFIG_DIR=os.path.join(BASE_CONFIG_DIR, "string")
)

REFSEQ = DBConfig(
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "refseq"),
    CONFIG_DIR=os.path.join(BASE_CONFIG_DIR, "refseq")
)