# Bachelor Thesis

## Installation

```bash
git clone https://github.com/ladrex/bachelor-thesis
cd bachelor-thesis
uv venv venv
source venv/bin/activate

# install requirements
uv pip install -r requirements.txt

# install geoextent
git clone https://github.com/ladrex/geoextent
# gdal is required on system
uv pip install gdal=="$(gdal-config --version)"
uv pip install -r geoextent/requirements.txt
uv pip install -e geoextent
```

## Usage

[jupyter-notebook.ipynb](jupyter-notebook.ipynb)

## Results

https://doi.org/10.5281/zenodo.15706532
