# KW-AIRO-KPI-API
KPI Calculation


## Python Version

Experiments conducted in Python version 3.11.7

## Conda Environment

```
wget https://repo.anaconda.com/miniconda/Miniconda3-py312_25.7.0-2-Linux-x86_64.sh

bash ~/Miniconda3-py312_25.7.0-2-Linux-x86_64.sh

/home/ubuntu/miniconda3/bin/conda init bash

source ~/.bashrc
```

```
printf "accept\naccept\nyes\n" conda create -n kpi python=3.11.7 pip 
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

For production running:
tmux

conda activate kpi

uvicorn app.main:app --host 0.0.0.0 --port 8000
```


