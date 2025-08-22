# pwaccs: Pathway Accession Codes
# Falta Pwaccs de gene
OPTIONS = {
    "protein": ["summary", "aids", "concise", "pwaccs"],
    "compound": ["default", "record", "synonyms", "sids", "cids", "aids", "assaysummary", "description"],
    "gene": ["summary","aids","concise","pwaccs"]
}

COMPOUND_TEMPLATE = {
    "http_method": "GET",
    "path_param": None,
    "parameters": {
        "cid": (str, None, True),
        "name": (str, None, True),
        "smiles": (str, None, True),
        "property": (str, None, True),
    },
    "group_queries": ["cid", "property"],
    "separator": ","
}

PROTEIN_TEMPLATE = {
    "http_method": "GET",
    "path_param": None,
    "parameters": {
        "accession": (str, None, True),
    },
    "group_queries": [None],
    "separator": None 
}

GENE_TEMPLATE = {
    "http_method": "GET",
    "path_param": None,
    "parameters": {
        "genesymbol": (str, None, True),
        "geneid": (str, None, True),
        "synonym": (str, None, True),
        "taxid": (str, None, True),
    },
    "group_queries": ["genesymbol"],
    "separator": ","
}