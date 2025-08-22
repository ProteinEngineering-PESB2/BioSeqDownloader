import gradio as gr
import pandas as pd
from .registry import REGISTRY
from .components.uniprot_query_search import build_ui as build_uniprot_search_ui
from .components.uniprot_blast_search import build_ui as build_uniprot_blast_search_ui
from .components.databases import build_api_ui

def build_ui():
    """Construye toda la interfaz principal a partir del REGISTRY"""
    with gr.Blocks() as demo:
        with gr.Tab("APIs"):
            for api_name, api_info in REGISTRY.items():
                build_api_ui(api_name, api_info)
        with gr.Tab("Uniprot search"):
            build_uniprot_search_ui()
            build_uniprot_blast_search_ui()

    return demo