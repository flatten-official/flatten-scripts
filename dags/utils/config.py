import yaml
import os

COMPOSER_DAGS_FOLDER = "/home/airflow/gcs/dags"
CONFIG_FILE="config.yaml"


def load_config():
    """Loads the config from the YAML file.
    Auto detects the dags folder if this is being run in an airflow node, otherwise
    defaults to assuming it is being run locally from the dags/ folder in the flatten-scripts
    repository.
    """
    path = CONFIG_FILE
    if os.path.exists(COMPOSER_DAGS_FOLDER):
        path = os.path.join(COMPOSER_DAGS_FOLDER, path)

    with open(path, "r") as f:
        try:
            config_data = yaml.full_load(f)
        except AttributeError:
            # GCP has an older version of pyyaml by default; full_load does not exist.
            config_data = yaml.load(f)

    return config_data


def detect_project(config_data):
    """Detects the project (master, staging, or unknown) given the YAML config using
    the environement variables."""
    try:
        project = os.environ["GCP_PROJECT"]
    except KeyError:
        project = ""
    if project == config_data["project_id_master"]:
        return "master"
    elif project == config_data["project_id_staging"]:
        return "staging"
    else:
        return "unknown"


def load_name_config(name):
    """Load the config under `name` into a dict along with the variables under it in the approprate
    (master or staging) project."""
    config = load_config()
    name_config = config[name]
    project = detect_project(config)
    project = "master" if project is "master" else "staging"
    for k, v in name_config[project].items():
        name_config[k] = v
    return name_config


if __name__ == "__main__":
    print(load_config())