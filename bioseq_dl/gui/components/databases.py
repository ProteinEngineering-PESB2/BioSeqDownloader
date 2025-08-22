import gradio as gr
import pandas as pd

#############################
# API Related Functions
#############################

def build_api_ui(api_name, api_info):
    """Construye la pestaña de una API completa"""
    with gr.Tab(api_name):
        api_class = api_info["class"]
        for method_name, method_info in api_info["methods"].items():
            build_method_ui(api_class, method_name, method_info)


def build_method_ui(api_class, method_name, method_info):
    """Construye la pestaña de un método dentro de una API"""
    with gr.Tab(method_name):
        inputs = []
        for param in method_info["inputs"]:
            inp = create_input_component(param)
            inputs.append((param["name"], param["type"], inp))

        # Output
        df_out = gr.Dataframe(label="Resultado (DataFrame)", interactive=False, wrap=True, visible=True)
        json_out = gr.JSON(label="Resultado (JSON)", visible=True)
        run_btn = gr.Button("Ejecutar")

        run_btn.click(
            lambda *args, api_class=api_class, method_name=method_name, method_info=method_info, inputs=inputs:
                run_query(api_class, method_name, method_info, inputs, args, df_out, json_out),
            inputs=[i[2] for i in inputs],
            outputs=[df_out, json_out]
        )


def create_input_component(param):
    """Crea un input de Gradio según el tipo definido en el REGISTRY"""
    if param["type"] == "str" and "choices" not in param:
        return gr.Textbox(
            label=param.get("label", param["name"]),
            placeholder=param.get("default", ""),
            value=param.get("default", "")
            )

    if param["type"] == "list[str]" and "checkboxgroup" not in param:
        return gr.Textbox(label=f"{param.get('label', param['name'])} (comma separated)")

    if "choices" in param:
        return gr.Dropdown(
            param["choices"],
            label=param.get("label", param["name"]),
            multiselect=("list" in param["type"]),
        )
    if "checkboxgroup" in param:
        return gr.CheckboxGroup(
            param["checkboxgroup"],
            label=param.get("label", param["name"]),
        )

    return gr.Textbox(label=param.get("label", param["name"]))


def run_query(api_class, method_name, method_info, inputs, args, df_out, json_out):
    """Ejecuta el método de la API con los valores de los inputs"""
    api = api_class()

    if method_info["input_type"] == "dict":
        query = {}
        for (name, typ, inp), val in zip(inputs, args):
            if "list" in typ:
                if isinstance(val, list):
                    query[name] = val
                else:
                    query[name] = [x.strip() for x in val.split(",") if x.strip()]
            else:
                query[name] = val
    elif method_info["input_type"] == "list":
        query = [x.strip() for x in args[0].split(",") if x.strip()]
    else:
        raise ValueError(f"Unsupported input_type {method_info['input_type']}")

    if isinstance(query, dict):
        query = [query]
    try:
        result = api.fetch_batch(queries=query, method=method_name, parse=True, to_dataframe=True)
        # Si es DataFrame, mostrar en df_out
        if isinstance(result, pd.DataFrame):
            return result, gr.update(visible=False)
        else:
            return gr.update(visible=False), gr.update(value=result, visible=True)
    except Exception as e:
        return None, {"error": str(e), "method": method_name, "query": query}