import os

from typing import Union, List, Dict, Set, Optional

from requests import Request
from requests.exceptions import RequestException

import pandas as pd

from .base import BaseAPIInterface
# Add the import for your database in constants
from .constants import BIODBNET
from .utils import validate_parameters

# More inputs can be added from
# https://biodbnet.abcc.ncifcrf.gov/webServices/rest.php/biodbnetRestApi.json?method=getinputs
inputs = [
    "ecnumber",
    "geneid",
    "genesymbol",
    "genesymbolandsynonyms",
    "genesymbolorderedlocus",
    "genesymbolorf",
    "goid",
    "interproid",
    "keggcompoundid",
    "keggcompoundname",
    "keggdiseaseid",
    "keggdrugid",
    "keggdrugname",
    "kegggeneid",
    "keggpathwayid",
    "pdbid",
    "pfamid",
    "pubchemid",
    "reactomepathwayname",
    "refseqgenomicaccession",
    "refseqmrnaaccession",
    "refseqproteinaccession",
    "taxonid",
    "uniprotaccession",
    "uniprotentryname",
    "uniprotproteinname",
]

outputs = [
    "affyid",
    "agilentid",
    "allergomecode",
    "apldb_cryptodbid",
    "biocartapathwayname",
    "biocycid",
    "ccdsid",
    "chromosomallocation",
    "cleanexid",
    "codelinkid",
    "cosmicid",
    "cpdbproteininteractor",
    "ctddiseaseinfo",
    "ctddiseasename",
    "cygdid",
    "dbsnpid",
    "dictybaseid",
    "dipid",
    "disprotid",
    "drugbankdrugid",
    "drugbankdruginfo",
    "drugbankdrugname",
    "echobaseid",
    "ecogeneid",
    "ensemblbiotype",
    "ensemblgeneid",
    "ensemblgeneinfo",
    "ensemblproteinid",
    "ensembltranscriptid",
    "flybasegeneid",
    "flybaseproteinid",
    "flybasetranscriptid",
    "gaddiseaseinfo",
    "gaddiseasename",
    "genbanknucleotideaccession",
    "genbanknucleotidegi",
    "genbankproteinaccession",
    "genbankproteingi",
    "geneid",
    "geneinfo",
    "genesymbol",
    "genesymbolandsynonyms",
    "genesymbolorderedlocus",
    "genesymbolorf",
    "genesynonyms",
    "genefarmid",
    "go-biologicalprocess",
    "go-cellularcomponent",
    "go-molecularfunction",
    "goid",
    "gseastandardname",
    "h-invlocusid",
    "hamapid",
    "hgncid",
    "hmdbmetabolite",
    "homolog-allensgeneid",
    "homolog-allensproteinid",
    "homolog-allgeneid",
    "homolog-humanensgeneid",
    "homolog-humanensproteinid",
    "homolog-humangeneid",
    "homolog-mouseensgeneid",
    "homolog-mouseensproteinid",
    "homolog-mousegeneid",
    "homolog-ratensgeneid",
    "homolog-ratensproteinid",
    "homolog-ratgeneid",
    "homologeneid",
    "hpaid",
    "hprdid",
    "hprdproteincomplex",
    "hprdproteininteractor",
    "illuminaid",
    "imgt/gene-dbid",
    "interproid",
    "ipiid",
    "keggdiseaseid",
    "kegggeneid",
    "keggorthologyid",
    "keggpathwayid",
    "keggpathwayinfo",
    "keggpathwaytitle",
    "legiolistid",
    "lepromaid",
    "locustag",
    "maizegdbid",
    "meropsid",
    "mgc(zgc/xgc)id",
    "mgc(zgc/xgc)imageid",
    "mgc(zgc/xgc)info",
    "mgiid",
    "mimid",
    "miminfo",
    "mirbaseid",
    "ncipidpathwayname",
    "ncipidproteincomplex",
    "ncipidproteininteractor",
    "ncipidptm",
    "orphanetid",
    "pantherid",
    "paralog-ensgeneid",
    "pbrid",
    "pdbid",
    "peroxibaseid",
    "pfamid",
    "pharmgkbdruginfo",
    "pharmgkbgeneid",
    "pirid",
    "pirsfid",
    "pptasedbid",
    "printsid",
    "prodomid",
    "prositeid",
    "pseudocapid",
    "pubmedid",
    "reactomeid",
    "reactomepathwayname",
    "rebaseid",
    "refseqgenomicaccession",
    "refseqgenomicgi",
    "refseqmrnaaccession",
    "refseqncrnaaccession",
    "refseqnucleotidegi",
    "refseqproteinaccession",
    "refseqproteingi",
    "rfamid",
    "rgdid",
    "sgdid",
    "smartid",
    "stringproteininteractor",
    "tairid",
    "taxonid",
    "tcdbid",
    "tigrfamsid",
    "tuberculistid",
    "ucscid",
    "unigeneid",
    "uniprotaccession",
    "uniprotentryname",
    "uniprotinfo",
    "uniprotproteinname",
    "unistsid",
    "vectorbasegeneid",
    "vegageneid",
    "vegaproteinid",
    "vegatranscriptid",
    "wormbasegeneid",
    "wormpepproteinid",
    "xenbasegeneid",
    "zfinid"
]

class BioDBNetInterface(BaseAPIInterface):
    METHODS = {
        "getpathways": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "pathways": (str, "1", True),
                "taxonId": (str, None, True)
            },
            "group_queries": [None],
            "separator": None
        },
        "db2db": {
            "http_method": "GET",
            "path_param": None,
            "parameters": {
                "input": (str, None, True),
                "inputValues": (str, None, True),
                "outputs": (str, "genesymbol,affyid,go-biologicalprocess,go-cellularcomponent,go-molecularfunction,goid", True),
                "taxonId": (str, None, True)
            },
            "group_queries": ["inputValues"],
            "separator": ","
        }
    }
    def __init__(
            self,  
            cache_dir: Optional[str] = None,
            config_dir: Optional[str] = None,
            output_dir: Optional[str] = None,
            **kwargs
        ):

        if cache_dir:
            cache_dir = os.path.abspath(cache_dir)
        else:
            cache_dir = BIODBNET.CACHE_DIR if BIODBNET.CACHE_DIR is not None else ""

        if config_dir is None:
            config_dir = BIODBNET.CONFIG_DIR if BIODBNET.CONFIG_DIR is not None else ""

        super().__init__(cache_dir=cache_dir, config_dir=config_dir, **kwargs)
        self.output_dir = output_dir or cache_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch(
            self, 
            query: Union[str, dict, list], 
            *, 
            method: str = "getpathways", 
            **kwargs
        ):
        if method not in self.METHODS.keys():
            raise ValueError(f"Method {method} is not supported. Available methods: {list(self.METHODS.keys())}")
        
        http_method, _, parameters, inputs = self.initialize_method_parameters(query, method, self.METHODS, **kwargs)

        inputs.update({"method": method})

        inputs["outputs"] = ",".join(inputs.get("outputs", [])) if isinstance(inputs.get("outputs"), list) else inputs.get("outputs", "")

        req = Request(
            method=http_method,
            url=BIODBNET.API_URL,
            params=inputs
        )
        prepared = self.session.prepare_request(req)
        print(f"Prepared request: {prepared.url}")

        try:
            response = self.session.send(prepared)
            self._delay()
            response.raise_for_status()
            
            match method:
                case "db2db":
                    response = response.json()
                    response = [
                        v["outputs"] for k, v in response.items() if isinstance(v, dict) and k not in inputs
                    ]
                    return response
                case _:
                    return response.json()
        except RequestException as e:
            print(f"Error fetching {query} for method '{method}': {e}")
            print("Response:", response.text)
            return {}


    def parse(
            self, 
            data: Union[List, Dict],
            fields_to_extract: Optional[Union[list, dict]],
            **kwargs
        ) -> Union[List, Dict]:
        
        if not data:
            return {}

        elif isinstance(data, (dict, list)):
            data = data
        else:
            raise ValueError("Response must be a requests.Response object, list or a dictionary.")

        return self._extract_fields(data, fields_to_extract)
    
    def get_dummy(self, **kwargs) -> Dict:
        return {
            "message": "This is a dummy response.",
            "status": "success"
        }
    
    def query_usage(self) -> str:
        return """
        This is a dummy query usage for YourDatabaseInterface.
        """