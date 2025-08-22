import typer
from dotenv import load_dotenv
import os

app = typer.Typer(help="Set BioGRID API key for bioseq-dl CLI.")
ENV_FILE = ".env"

def save_env_variable(key: str, value: str, env_file=ENV_FILE):
    # Create the .env file if it does not exist
    if not os.path.exists(env_file):
        open(env_file, "w").close()
    
    # Read the existing .env file
    lines = []
    with open(env_file, "r") as f:
        lines = f.readlines()

    # Check if the key already exists
    with open(env_file, "w") as f:
        found = False
        for line in lines:
            if line.startswith(f"{key}="):
                f.write(f"{key}={value}\n")
                found = True
            else:
                f.write(line)
        if not found:
            f.write(f"{key}={value}\n")

    # Also update the variable in the current session
    os.environ[key] = value
    typer.secho(f"{key} set successfully!", fg=typer.colors.GREEN)

@app.command()
def run(
    key: str = typer.Option(
        None,
        "--key",
        "-k",
        help="BioGRID API key.",
        case_sensitive=True,
    )
):
    if not key:
        typer.echo("Error: BioGRID API key is required.", err=True)
        raise typer.Exit(code=1)

    # Save the key to the .env file
    save_env_variable("biogrid_api_key", key)