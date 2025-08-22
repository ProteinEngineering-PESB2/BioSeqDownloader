from bioseq_dl import AlphafoldInterface, BioDBNetInterface
from bioseq_dl.constants.biodbnet import inputs as biodbnet_inputs, outputs as biodbnet_outputs

REGISTRY = {
    "AlphaFold": {
        "class": AlphafoldInterface,
        "label": "AlphaFold",
        "methods": {
            "prediction": {
                "input_type": "list",
                "inputs": [
                    {
                        "name": "ids", 
                        "type": "list[str]", 
                        "label": "UniProt IDs (comma separated)",
                        "default": "P02666, Q9TSI0"
                    }
                ],
            }
        }
    },
    "BioDBNet": {
        "class": BioDBNetInterface,
        "label": "BioDBNet",
        "methods": {
            "db2db": {
                "input_type": "dict",
                "inputs": [
                    {
                        "name": "input", 
                        "type": "str", 
                        "choices": biodbnet_inputs, 
                        "label": "Input Field",
                        "default": "genesymbol"
                    },
                    {
                        "name": "inputValues", 
                        "type": "list[str]", 
                        "label": 
                        "Input Values (comma separated)",
                        "default": "APP"
                    },
                    {
                        "name": "outputs", 
                        "type": "list[str]", 
                        "checkboxgroup": biodbnet_outputs, 
                        "label": "Output Fields"
                    },
                    {
                        "name": "taxonId", 
                        "type": "str", 
                        "label": "Taxon ID",
                        "default": "9606"
                    }
                ]
            },
            "getpathways": {
                "input_type": "dict",
                "inputs": [
                    {"name": "pathways", "type": "str", "label": "Pathways"},
                    {"name": "taxonId", "type": "str", "label": "Taxon ID"}
                ]
            }
        }
    }
}