import pkgutil
import inspect
import importlib
from bioseq_dl import BaseAPIInterface
import bioseq_dl.core.interfaces as interfaces_pkg

def load_interfaces():
    api_classes = []
    for _, module_name, _ in pkgutil.iter_modules(interfaces_pkg.__path__):
        if module_name.startswith("_"):
            continue  # Skip private modules
        # Import the module dynamically
        module = importlib.import_module(f"{interfaces_pkg.__name__}.{module_name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseAPIInterface) and obj is not BaseAPIInterface:
                api_classes.append(obj)
    return api_classes