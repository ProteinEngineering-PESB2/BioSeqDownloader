import pandas as pd 
import argparse
from tqdm import tqdm
from uniprot import UniprotInterface
import math

def replace_char_at_index(s, i, new_char):
    if i < 0 or i >= len(s):
        raise IndexError("Index out of range.")
    return s[:i] + new_char + s[i+1:]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download data from the web')
    parser.add_argument('-i', '--input', help='file with UniProt IDs')
    parser.add_argument('-o', '--output', help='Output file')
    parser.add_argument('-d', '--disease', help='Disease to search for')
    parser.add_argument('-f', '--from_db', help='Database to convert from. Default is UniProtKB_AC-ID (UniProtKB_AC-ID, PDB)', default='UniProtKB_AC-ID')
    parser.add_argument('-t', '--to_db', help='Database to convert to', default='UniProtKB')
    parser.add_argument('-b', '--batch_size', help='Batch size for downloading', default=5000)
    args = parser.parse_args()
    
    downloader = UniprotInterface()
    
    with open(args.input, 'r') as f:
        ids = f.read().splitlines()

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
                sequence = result['to']['sequence']['value']
                for feature in result['to']['features']:
                    row = []
                    row.append(result['from'])
                    if feature['type'] == 'Natural variant' and args.disease in feature['description']:     
                        row.append(feature['featureId'])
                        location_start = feature['location']['start']['value']
                        location_end = feature['location']['end']['value']
                        if location_start == location_end:
                            row.append(location_start)
                            original_sequence = feature['alternativeSequence']['originalSequence']
                            new_sequence = feature['alternativeSequence']['alternativeSequences'][0]
                            row.append(f"{original_sequence}->{new_sequence}")
                            row.append(replace_char_at_index(sequence, int(location_start)-1, new_sequence))
                        else:
                            row.append(f"{location_start}-{location_end}")
                            row.append("missing")
                            row.append(sequence[:int(location_start)-1] + sequence[int(location_end)-1:])
                        export_data.append(row)

            export_df = pd.DataFrame(export_data, columns=["uniprot_id", "variant_id", "position", "change", "sequence"])
            
            if start == 0:
                export_df.to_csv(args.output, mode='w', header=True, index=False)
            else:
                export_df.to_csv(args.output, mode='w', header=False, index=False)
