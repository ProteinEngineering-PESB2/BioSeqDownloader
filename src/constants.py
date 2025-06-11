from dataclasses import dataclass
import os
from typing import Optional

POLLING_INTERVAL = 3

DATABASES = {
    'go_terms': 'GO',
    'pfam_ids': 'Pfam',
    'alphafold_ids': 'AlphaFoldDB',
    'pdb_ids': 'PDB',
    'kegg_ids': 'KEGG',
    'brenda_ids': 'BRENDA',
    'reactome_ids': 'Reactome',
    'refseq_ids': 'RefSeq',
    'interpro_ids': 'InterPro',
    'string_ids': 'STRING',
}

BASE_CACHE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.cache"))

@dataclass(frozen=True)
class DBConfig:
    API_URL: str
    STRUCTURE_URL: Optional[str] = None
    CACHE_DIR: Optional[str] = None

ALPHAFOLD = DBConfig(
    API_URL="https://alphafold.ebi.ac.uk/api/prediction/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "alphafold")
)

PDB = DBConfig(
    API_URL="https://data.rcsb.org/rest/v1/core/",
    STRUCTURE_URL="https://files.rcsb.org/download/",
    CACHE_DIR = os.path.join(BASE_CACHE_DIR, "pdb")
)

REACTOME = DBConfig(
    API_URL = "https://reactome.org/ContentService/",
    CACHE_DIR = os.path.join(BASE_CACHE_DIR, "reactome")
)

BRENDA = DBConfig(
    API_URL = "https://www.brenda-enzymes.org/soap/brenda_zeep.wsdl",
    CACHE_DIR= os.path.join(BASE_CACHE_DIR, "brenda")
)

INTERPRO = DBConfig(
    API_URL = "https://www.ebi.ac.uk:443/interpro/api/",
    CACHE_DIR = os.path.join(BASE_CACHE_DIR, "interpro")
)

STRING = DBConfig(
    API_URL = "https://string-db.org/api/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "string")
)

KEGG = DBConfig(
    API_URL="https://rest.kegg.jp/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "KEGG")
)

GENONTOLOGY = DBConfig(
    API_URL="https://api.geneontology.org/api/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "genontology")
)

BIOGRID = DBConfig(
    API_URL="https://webservice.thebiogrid.org/",
    CACHE_DIR=os.path.join(BASE_CACHE_DIR, "biogrid")
)