from load import load_to_json
from extract import extract_json_files
from transform import transform_json_files


if __name__ == "__main__":
    print("Begin Extraction \n")
    json_files, json_loaded_files = extract_json_files('./data/*.json')
    print("\nFinish Extraction\n")

    print("Begin Transformation \n")
    df = transform_json_files(json_files, json_loaded_files)
    print("\nFinish Transformation\n")

    print("Begin Loading \n")
    load_to_json(df)
    print("\nFinish Loading\n")

