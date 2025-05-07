import os, argparse, ast
import pandas as pd
from tqdm import tqdm

DOCKER_IMAGE_NAME = "metastudent"
DOCKER_CONTAINER_NAME = "metastudent_container"
HOST_INPUT_FILE = os.path.abspath("tmp/sequences.fasta")
HOST_OUTPUT_DIR = os.path.abspath("tmp/")
CONTAINER_INPUT_FILE = "/app/input.fasta"
CONTAINER_OUTPUT_FILE = "/app/output.result"

def parse_outputs(id_column: str) -> pd.DataFrame:
    files = [
        f"{HOST_OUTPUT_DIR}/output.BPO.txt",
        f"{HOST_OUTPUT_DIR}/output.CCO.txt",
        f"{HOST_OUTPUT_DIR}/output.MFO.txt"
    ]
    types = [
        "Biological Process",
        "Cellular Component",
        "Molecular Function"
    ]
    results = []

    for file, file_type in zip(files, types):
        try:
            df_go = pd.read_csv(
                    file, 
                    header=None, 
                    sep="\t", 
                    names=[id_column, "go", "probability", "term"],
            )
            df_go['go_type'] = file_type
            max_probs = df_go.groupby(id_column)["probability"].max()
            df_go["is_max"] = df_go["probability"] == max_probs[df_go[id_column]].values
            df_go = df_go[df_go["is_max"]].drop(columns=["is_max"])
            results.append(df_go)
        except FileNotFoundError:
            print(f"File '{file}' not found.")
            exit(1)
    
    if not results:
        # df = pd.DataFrame({
        #     "id_seq": ["None"],
        #     "id_go": ["No results found"],
        #     "probability": [0],
        #     "term": ["No results found"],
        #     "go": ["No results found"]
        # })
        df = pd.DataFrame(columns=[id_column, "go", "probability", "term", "go_type"])
    else:
        df = pd.concat(results)

    return df

def generate_fasta(sequences : pd.DataFrame, output_path: str, column_id: str = "uniprot_id", column_seq: str = "sequence"):
    with open(output_path, "w") as f:
        for index, row in sequences.iterrows():
            f.write(f">{row[column_id]}\n{row[column_seq]}\n")

def append_files(tmp_dir, output_dir):
    files = [
        f"{tmp_dir}/output.BPO.txt",
        f"{tmp_dir}/output.CCO.txt",
        f"{tmp_dir}/output.MFO.txt"
    ]
    for file in files:
        with open(file, "r") as f:
            data = f.read()
            # PermissionError: [Errno 13] Permission denied: '/home/diego/Documents/PythonProjects/test_plastipediaETL/parsed_data/other_tools/output.BPO.txt'
            with open(f"{output_dir}/{os.path.basename(file)}", "a") as f_out:
                f_out.write(data)

def run_metastudent(input_file, output_dir):
    uid = os.getuid()
    gid = os.getgid()
    result = os.system(
        f"docker run --rm --name {DOCKER_CONTAINER_NAME} "
        f"-v {input_file}:{CONTAINER_INPUT_FILE} "
        f"-v {output_dir}:/app/output "
        f"-u {uid}:{gid} "
        f"{DOCKER_IMAGE_NAME} -i {CONTAINER_INPUT_FILE} -o /app/output/output"
    )
    if result != 0:
        print("Error running the Docker container.")
        exit(1)
    print(f"Metastudent has finished processing. Output files are in '{HOST_OUTPUT_DIR}'.")

def run_in_batches(df, output_dir):
    # Convert and save as FASTA
    generate_fasta(df, HOST_INPUT_FILE)

    if os.path.isfile(f"{HOST_OUTPUT_DIR}/output.BPO.txt") and \
            os.path.isfile(f"{HOST_OUTPUT_DIR}/output.CCO.txt") and \
            os.path.isfile(f"{HOST_OUTPUT_DIR}/output.MFO.txt"):
        os.makedirs(f"{HOST_OUTPUT_DIR}/tmp", exist_ok=True)
        run_metastudent(HOST_INPUT_FILE, f"{HOST_OUTPUT_DIR}/tmp")
        # Append tmp into the final file
        append_files(f"{HOST_OUTPUT_DIR}/tmp", HOST_OUTPUT_DIR)
        os.system(f"rm -r {HOST_OUTPUT_DIR}/tmp")
    else:
        run_metastudent(HOST_INPUT_FILE, HOST_OUTPUT_DIR)

    os.remove(HOST_INPUT_FILE)


def install_dependencies(image_name="metastudent"):
    if not os.path.isfile("Dockerfile"):
        print("Dockerfile not found in the current directory. Please create a Dockerfile for Metastudent.")
        exit(1)

    print("Building the Docker image...")
    result = os.system(f"docker build -t {image_name} .")
    if result != 0:
        print("Failed to build the Docker image. Check your Dockerfile.")
        exit(1)

def check_dependencies(image_name="metastudent"):
    if os.system("docker --version") != 0:
        print("Docker not found. Please install Docker.")
        exit(1)
    
    if os.system(f"docker image inspect {image_name} > /dev/null 2>&1") == 0:
        return True
    return False

# Fijarse si es que saca correctamente las cosas
if "__main__" == __name__:
    parser = argparse.ArgumentParser(description="Get Gene Ontology descriptions.")
    parser.add_argument("-i", "--input", help="Input file with sequences", required=True)
    parser.add_argument("-o", "--output", help="Output directory", default=".")
    parser.add_argument("-d", "--amigo_data", help="AmiGO data file", default="scripts/resources/amiGO_data.csv")
    parser.add_argument("-go", "--column_go", help="Column name with GO terms", default="go_terms")
    parser.add_argument("-id", "--column_id", help="Column name with IDs", default="uniprot_id")

    args = parser.parse_args()

    print("Getting Gen Ontology")
    tqdm.pandas()
    
    if not check_dependencies(DOCKER_IMAGE_NAME):
        print("Metastudent not found. Installing...")
        install_dependencies(DOCKER_IMAGE_NAME)
    else:
        print("Metastudent found.")

    input_df = pd.read_csv(args.input)
    obsolete_df = pd.read_csv(args.amigo_data, sep="\t", names=["id_go", "description", "is_obsolete"])
    input_df[args.column_go] = input_df[args.column_go].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

    parsed_df = pd.DataFrame()
    if os.path.isfile(f"{HOST_OUTPUT_DIR}/output.BPO.txt") and \
            os.path.isfile(f"{HOST_OUTPUT_DIR}/output.CCO.txt") and \
            os.path.isfile(f"{HOST_OUTPUT_DIR}/output.MFO.txt"):
        print("Metastudent results found.")
        parsed_df = parse_outputs(args.column_id)

    # Filter input_df with go_terms ~= null
    input_df_with_go_terms = input_df[input_df[args.column_go].apply(lambda x: isinstance(x, list) and len(x) > 0)]
    input_df = input_df[input_df[args.column_go].apply(lambda x: isinstance(x, list) and len(x) == 0)]

    if not input_df_with_go_terms.empty:
        print("Go terms found in input data.")
        input_df_with_go_terms = input_df_with_go_terms[[args.column_id, args.column_go]]
        input_df_with_go_terms = input_df_with_go_terms.explode(args.column_go)
        parsed_df = pd.concat(
            [
                parsed_df,
                pd.merge(
                    input_df_with_go_terms, 
                    obsolete_df, 
                    left_on=args.column_go, 
                    right_on="id_go", 
                    how="left"
                )
                .drop(columns=[args.column_go])
                .rename(columns={"id_go": "go"})  
            ]
        )

    if not parsed_df.empty:
        # Check if all sequences have been processed
        parsed_ids = parsed_df[args.column_id].unique()
        input_ids = input_df[args.column_id].unique()
        if len(parsed_ids) == len(input_ids):
            print("All sequences have been processed.")
            input_df = pd.DataFrame()
        else:
            input_df = input_df[~input_df[args.column_id].isin(parsed_ids)]
            print(f"{len(input_df)} sequences have not been processed.")
    
    os.makedirs(HOST_OUTPUT_DIR, exist_ok=True)

    if not input_df.empty:
        print("Running in batches of 50...")
        for i in tqdm(range(0, len(input_df), 50)):
            run_in_batches(input_df[i:i+50], HOST_OUTPUT_DIR)

        if not parsed_df.empty:
            parsed_df = pd.concat(
                [
                    parsed_df,
                    parse_outputs(args.column_id)
                ]
            )
        else:
            parsed_df = parse_outputs(args.column_id)
    
        parsed_df = parsed_df.merge(obsolete_df, left_on="go", right_on="id_go", how="left")
        parsed_df = parsed_df.drop(columns=["id_go"])
    
    parsed_df = parsed_df.sort_values(by=args.column_id)
    parsed_df.to_csv(args.output, index=False)
    print("Done.")
