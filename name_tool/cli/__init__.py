import typer
from name_tool.cli.get_data import app as run_get_data

app = typer.Typer(name="name_tool", add_completion=False, help="Description")

app.add_typer(
    run_get_data, 
    name="get-data", 
    help="Data")

if __name__ == "__main__":
    app()