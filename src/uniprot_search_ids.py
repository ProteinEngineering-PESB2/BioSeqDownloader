import pandas as pd 
import argparse
from src.uniprot import UniprotInterface

# TODO: Cambiar README.md esta con el antiguo nombre de carpeta 'scripts'
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download data from the web')
    parser.add_argument('-i', '--input', help='CSV file with UniProt IDs')
    parser.add_argument('-c', '--column', help='column name with UniProt IDs', default="accession")
    parser.add_argument('-o', '--output', help='Output file', required=True)
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
    instance = UniprotInterface()
    results = instance.download_batch(df, args.column, args.auto_db, args.from_db, args.to_db, args.batch_size)

    # Save raw results
    with open(args.output + ".json", 'w') as f:
        for result in results:
            f.write(str(result) + '\n')

    export_df = instance.parse_results(results)
    export_df.to_csv(args.output, index=False)
