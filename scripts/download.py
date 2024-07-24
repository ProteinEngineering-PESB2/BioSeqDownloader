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
    parser.add_argument('-f', '--from_db', help='Database to convert from. Default is UniProtKB_AC-ID (UniProtKB_AC-ID, PDB)', default='UniProtKB_AC-ID')
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
                row = []
                
                row.append(result['from'])
                
                if result['to'] is not None:

                    ec_ids = []
                    try:
                        for r in result['to']['proteinDescription']['recommendedName']['ecNumbers']:
                            ec_ids.append(r['value'])
                    except KeyError:
                        pass

                    row.append(ec_ids)

                    try:
                        row.append(result['to']['proteinDescription']['recommendedName']['fullName']['value'])
                    except KeyError:
                        row.append("")

                    tmp = {}
                    references_list = []
                    

                    try:
                        for r in result['to']['references']:
                            tmp["citacionCrossReferences"] = r['citation']['citationCrossReferences']
                            tmp.update({"title": r['citation']['title']})
                            references_list.append(tmp)
                    except KeyError:
                        pass

                    row.append(references_list)

                    go_ids = []
                    pfam_ids = []

                    try:
                        for r in result['to']['uniProtKBCrossReferences']:
                            if r['database'] == 'GO':
                                go_ids.append(r['id'])
                            elif r['database'] == 'pfam':
                                pfam_ids.append(r['id'])
                            else:
                                pass
                    except KeyError:
                        pass

                    row.append(go_ids)
                    row.append(pfam_ids)
                    
                    try:
                        row.append(result['to']['sequence']['value'])
                        row.append(result['to']['sequence']['length'])
                        row.append(result['to']['sequence']['molWeight'])
                    except KeyError:
                        row.extend(["", "", ""])
                else:
                    row.append([result['from'], "", "", "", "", "", "", ""])

                export_data.append(row)

            export_df = pd.DataFrame(export_data, columns=['uniprot_id', 'ec_number','protein_name', 'references', 'go_id', 'pfam_id','sequence', 'length', 'molecular_weight'])
            
            if start == 0:
                export_df.to_csv(args.output, mode='a', header=True, index=False)
            else:
                export_df.to_csv(args.output, mode='a', header=False, index=False)
