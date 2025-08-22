import gradio as gr
import pandas as pd

# Funciones dummy
def get_alphafold(query=""):
    return pd.DataFrame({"UniProt ID": ["P12345", "Q8N158"], "Score": [0.95, 0.88]})

def get_biodbnet(query=""):
    return pd.DataFrame({"Gene": ["APP", "SLC9A9"], "UniProt": ["P05067", "Q8N158"]})

with gr.Blocks(css="""
.sidebar {padding: 20px; height: 100%;}
.api-title {font-size: 1.5em; font-weight: bold; margin-bottom: 10px;}
""") as demo:
    with gr.Row():
        # Sidebar izquierda
        with gr.Column(scale=1, elem_classes="sidebar"):
            gr.Markdown("## 🔍 APIs")
            btn_af = gr.Button("AlphaFold")
            btn_bd = gr.Button("BioDBNet")
        
        # Contenido principal
        with gr.Column(scale=4):
            api_title = gr.Markdown("Selecciona una base de datos", elem_classes="api-title")

            alphafold_out = gr.Dataframe(visible=False, label="AlphaFold Results")
            biodbnet_out = gr.Dataframe(visible=False, label="BioDBNet Results")

            btn_fetch_af = gr.Button("Fetch AlphaFold", visible=False)
            btn_fetch_bd = gr.Button("Fetch BioDBNet", visible=False)

    # Función para cambiar pestaña
    def show_tab(api):
        if api == "af":
            return (
                "### 🧬 AlphaFold",
                gr.update(visible=True), gr.update(visible=False),
                gr.update(visible=True), gr.update(visible=False)
            )
        elif api == "bd":
            return (
                "### 📚 BioDBNet",
                gr.update(visible=False), gr.update(visible=True),
                gr.update(visible=False), gr.update(visible=True)
            )

    btn_af.click(lambda: show_tab("af"), None, [api_title, alphafold_out, biodbnet_out, btn_fetch_af, btn_fetch_bd])
    btn_bd.click(lambda: show_tab("bd"), None, [api_title, alphafold_out, biodbnet_out, btn_fetch_af, btn_fetch_bd])

    # Botones de fetch internos
    btn_fetch_af.click(get_alphafold, inputs=[], outputs=alphafold_out)
    btn_fetch_bd.click(get_biodbnet, inputs=[], outputs=biodbnet_out)

demo.launch()
