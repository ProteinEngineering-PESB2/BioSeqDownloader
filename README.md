# BioSeqDownloader

**BioSeqDownloader** is a Python tool designed for downloading biological sequences. Initially focused on Uniprot, this tool aims to scale and support multiple sequences databases, providing a unified and efficient way to retrieve biological data.

This is just a programtic way of ID mapping and downloading sequences from Uniprot, the code is based on the [Uniprot API](https://www.uniprot.org/help/api) and the [Uniprot ID mapping](https://www.uniprot.org/help/id_mapping) service.

# Installation

To set up **BioSeqDownloader**, follow these steps:

1. Ensure Docker is installed on your system. If not, download and install it from [Docker's official website](https://www.docker.com/).
2. Clone the repository:
    ```bash
    git clone https://github.com/your-username/BioSeqDownloader.git
    cd BioSeqDownloader
    ```
3. Create the Conda environment using the provided `environment.yml` file:
    ```bash
    git clone https://github.com/your-username/BioSeqDownloader.git
    cd BioSeqDownloader
    conda env create -f environment.yml
    ```

# Usage

## Running Sequence Alignment with BLAST

To perform a BLAST alignment and save the results as a CSV file, use the `aligment.py` script. Below are the available parameters:

- `-d`, `--database`: The database to download (required).
- `-e`, `--extension`: File extension of the database (default: `fasta`).
- `-f`, `--sequences_file`: File containing sequences to run BLAST on.
- `-c`, `--seq_column`: Column name containing sequences (default: `sequences`).
- `-o`, `--output`: Output file for BLAST results (default: `blast_results.csv`).
- `--evalue`: E-value threshold for the BLAST search (default: `0.001`).
- `--blast_type`: Type of BLAST to run (default: `blastp`).

### Example:
```bash
python scripts/aligment.py -d uniprotkb_reviewed -f data/umami.csv -c sequence -o results/umami_blast.csv
```

---

## Downloading UniProt Data using IDS

The `uniprot_search_ids.py` script retrieves UniProt data using UniProt IDs. It can also utilize BLAST results from the previous step. Below are the available parameters:

- `-i`, `--input`: CSV file containing UniProt IDs.
- `-c`, `--column`: Column name with UniProt IDs (default: `accession`).
- `-o`, `--output`: Output file.
- `-f`, `--from_db`: Database to convert from (default: `UniProtKB_AC-ID`).
- `-t`, `--to_db`: Database to convert to (default: `UniProtKB`).
- `-b`, `--batch_size`: Batch size for downloading (default: `5000`).
- `-a`, `--auto_db`: Automatically detect database type.
- `--min_identity`: Minimum identity threshold for BLAST search (default: `90.0`).

### Example:
```bash
python scripts/download.py -i results/umami_blast.csv -o results/umami_uniprot.csv
```
## Searching UniProt with Queries

The `uniprot_search_query.py` script allows users to search UniProt using custom queries and retrieve specific fields of interest. Below are the available parameters:

- `-q`, `--query`: Query string to search for (required).
- `-o`, `--output`: Output file to save the results (required).
- `-f`, `--fields`: Fields to include in the output (default: `accession,protein_name,sequence,ec,lineage,organism_name,xref_pfam,xref_alphafolddb,xref_pdb,go_id`).
- `-s`, `--sort`: Sort order for the results (default: `accession asc`).
- `-fmt`, `--format`: Format of the output (default: `json`).
- `--include_isoform`: Include isoforms in the results (optional).
- `--download`: Download the results directly (optional).

### Example:
```bash
python src/uniprot_search_query.py -q "antibacterial AND reviewed:true" -o results/uniprot_stream.csv
```

This example searches for reviewed antibacterial proteins and saves the results in a CSV file. You can customize the query and fields to suit your research needs.

---

## Annotating Gene Ontology (GO) Terms

The `description_go.py` script annotates sequences with Gene Ontology (GO) terms. If UniProt does not provide GO IDs, it uses MetaStudent for prediction. Below are the available parameters:

- `-i`, `--input`: Input file with sequences (required).
- `-o`, `--output`: Output directory (default: `.`).
- `-d`, `--amigo_data`: AmiGO data file (default: `scripts/resources/amiGO_data.csv`).
- `-go`, `--column_go`: Column name with GO terms (default: `go_terms`).
- `-id`, `--column_id`: Column name with IDs (default: `uniprot_id`).

### Example:
```bash
python scripts/description_go.py -i results/umami_uniprot.csv -o results/umami_go.csv
```

These tools provide a streamlined workflow for downloading, aligning, and annotating biological sequences, making it easier to analyze and interpret biological data.
