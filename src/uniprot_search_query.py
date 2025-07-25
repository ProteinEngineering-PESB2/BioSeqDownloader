
import pandas as pd 
import argparse
from src.uniprot import UniprotInterface

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download data from the web')
    parser.add_argument('-o', '--output', help='Output file', required=True)
    parser.add_argument('-q', '--query', help='Query to search for', required=True)
    parser.add_argument('-f', '--fields', help='Fields to include in the output', default="accession,protein_name,sequence,ec,lineage,organism_name,gene_primary,xref_pfam,xref_kegg,xref_alphafolddb,xref_chembl,xref_refseq,xref_brenda,xref_reactome,xref_pdb,xref_interpro,xref_string,go_id")
    parser.add_argument('-s', '--sort', help='Sort order for the results', default="accession asc")
    parser.add_argument('-fmt', '--format', help='Format of the output', default="json")
    parser.add_argument('--include_isoform', action='store_true', help='Include isoforms in the results')
    parser.add_argument('--download', action='store_true', help='Download the results')
    args = parser.parse_args()
    
    instance = UniprotInterface()
    print(f"Downloading data using query {args.query} and fields {args.fields}")
    response = instance.submit_stream(
        query=args.query,
        fields=args.fields,
        sort=args.sort,
        include_isoform=args.include_isoform,
        download=args.download,
        format=args.format
    )
    with open("response.json", "w") as f:
        f.write(response.text)

    print("Parsing results...")
    export_df = instance.parse_stream_response(
        query=args.query,
        response=response
    )
    
    export_df.to_csv(args.output, index=False)
