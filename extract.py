import glob
import json


def load_json(json_file):
    """Load json file

    Args:
        json_file (str): Path to json file

    Returns:
        dict: jason file in dictinary form
    """
    f = open(json_file)
    loaded_json = json.load(f)
    print("Json file loaded")
    return loaded_json

def extract_json_files(path_extention):
    """Extract json files

    Args:
        path_extention (str): path to json files with extention .json

    Returns:
        tuple: list of file_names, list of loaded json files
    """
    json_loeaded_files = []
    json_files = glob.glob(path_extention)
    for file in json_files:
        print(f"Extracting {file}")
        loaded_json = load_json(file)
        json_loeaded_files.append(loaded_json)

    return (json_files,json_loeaded_files)


