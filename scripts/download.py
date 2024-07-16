import pandas as pd 
import argparse
from tqdm import tqdm
from uniprot import UniprotInterface
import math

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download data from the web')
    parser.add_argument('-i', '--input', help='CSV file with UniProt IDs')
    parser.add_argument('-c', '--column', help='column name with UniProt IDs')
    parser.add_argument('-o', '--output', help='Output file')
    parser.add_argument('-f', '--from_db', help='Database to convert from', default='UniProtKB_AC-ID')
    parser.add_argument('-t', '--to_db', help='Database to convert to', default='UniProtKB')
    parser.add_argument('-b', '--batch_size', help='Batch size for downloading', default=5000)
    args = parser.parse_args()
    
    df = pd.read_csv(args.input)

    downloader = UniprotInterface()
    
    ids = df[args.column].dropna().unique().tolist()

    with open(args.output, 'w') as f:
        pass

    print("[SCRIPT] Downloading data in batches of", args.batch_size)
    for start in tqdm(range(0, len(ids), args.batch_size), desc='Downloading data', total=math.ceil(len(ids)/args.batch_size)):
        end = start + args.batch_size
        job_id = downloader.submit_id_mapping(args.from_db, args.to_db, ids[start:end])
        print("[SCRIPT] UniProt ID mapping generated job ID:", job_id)

        if downloader.check_id_mapping_results_ready(job_id):
            print("[SCRIPT] UniProt ID mapping job is ready. Getting results...")
            link = downloader.get_id_mapping_results_link(job_id)
            results = downloader.get_id_mapping_results_search(link)
            
            export_data = []
            for result in results['results']:
                # Algo que puedo obtener:
                # {'from': 'A0A5P8Q188', 'to': {'entryType': 'Inactive', 'primaryAccession': 'A0A5P8Q188', 'uniProtkbId': 'A0A5P8Q188_9LACO', 'annotationScore': 0.0, 'inactiveReason': {'inactiveReasonType': 'DELETED', 'deletedReason': 'Redundant proteome'}}}
                try:
                    export_data.append([result['from'], result['to']['sequence']['value']])
                except KeyError:
                    export_data.append([result['from'], ""])

            export_df = pd.DataFrame(export_data, columns=['uniprot_id', 'sequence'])
            
            if start == 0:
                export_df.to_csv(args.output, mode='a', header=True, index=False)
            else:
                export_df.to_csv(args.output, mode='a', header=False, index=False)
