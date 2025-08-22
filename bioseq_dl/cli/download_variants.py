import pandas as pd
import typer
from tqdm import tqdm
from bioseq_dl import UniprotInterface
import math

app = typer.Typer(name="uniprot-search-variants", help="Search and download variants from UniProt using IDs.")

def replace_char_at_index(s, i, new_char):
    if i < 0 or i >= len(s):
        raise IndexError("Index out of range.")
    return s[:i] + new_char + s[i+1:]

@app.command()
def run(
    input: str = typer.Option(
        ..., "-i", "--input",
        help="CSV file with UniProt IDs"
    ),
    output: str = typer.Option(
        ..., "-o", "--output",
        help="Output file"
    ),
    disease: str = typer.Option(
        None, "-d", "--disease",
        help="Disease to search for in variants"
    ),
    from_db: str = typer.Option(
        'UniProtKB_AC-ID', "-f", "--from_db",
        help="Database to convert from. Default is UniProtKB_AC-ID (UniProtKB_AC-ID, PDB)"
    ),
    to_db: str = typer.Option(
        'UniProtKB', "-t", "--to_db",
        help="Database to convert to"
    ),
    batch_size: int = typer.Option(
        5000, "-b", "--batch_size",
        help="Batch size for downloading"
    )):
    downloader = UniprotInterface()
    
    with open(input, 'r') as f:
        ids = f.read().splitlines()

    print("[SCRIPT] Downloading data in batches of", batch_size)
    for start in tqdm(range(0, len(ids), batch_size), desc='Downloading data', total=math.ceil(len(ids)/args.batch_size)):
        end = start + batch_size
        job_id = downloader.submit_id_mapping(from_db, to_db, ids[start:end])
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
                    if feature['type'] == 'Natural variant' and disease in feature['description']:     
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
                export_df.to_csv(output, mode='w', header=True, index=False)
            else:
                export_df.to_csv(output, mode='w', header=False, index=False)
