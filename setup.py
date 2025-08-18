from setuptools import setup, find_packages

setup(
    name="bioclust",
    version="0.1.0",
    author="Kren AI Lab",
    author_email="krenai@umag.cl",
    description="A protein clustering library",
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "torch>=2.7.1",
        "transformers>=4.48.1",
        "numpy>=2.2.6",
        "pandas>=2.3.1",
        "scikit-learn>=1.6.1",
        "matplotlib>=3.10.3",
        "seaborn>=0.13.2",
        "tslearn>=0.6.4",
        "umap-learn>=0.5.9.post2",
        "tqdm>=4.67.1",
        "pyclustering>=0.10.1.2",
        "sklearn-som>=1.1.0",
        "clustpy>=0.0.2",
        "biopython>=1.85",
        "biotite>=1.2.0",
        "igraph>=0.11.9",
        "msgpack-numpy>=0.4.8",
        "huggingface-hub>=0.33.2",
        "safetensors>=0.5.3",
        "regex>=2024.11.6",
        "accelerate>=1.9.0",
        "esm>=3.2.0",
        "sentencepiece>=0.2.0",
        "cairocffi>=1.7.1",
        "typer>=0.16.0",
        "appdirs>=1.4.4"
    ],
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: GNU-2 License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "bioclust=bioclust.cli.main:app",
        ],
    }
)