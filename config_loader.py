# config_loader.py
import yaml

def load_db_config(path="config.yaml"):
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg['database']
