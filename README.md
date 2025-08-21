# KW-AIRO-KPI-API
KPI Calculation


## Python Version

Experiments conducted in Python version 3.11.7

## Conda Environment

```
conda create -n kpi python=3.11.7 pip
conda activate kpi
```
## Requirements

```
pip install -r requirements.txt
```

## Add Model FIles
```
Add .pth model file and .npy classifications to the models folder
Change MODEL_PATH and CLASS_PATH in prediction.py to the appropriate paths of the uploaded files

To run the FastAPI application on localhost, use the following command:
fastapi run app/main.py

Or to run the application in dev mode, use
fastapi dev app/main.py

For background running:
uvicorn app.main:app --host localhost --port 8000
```
