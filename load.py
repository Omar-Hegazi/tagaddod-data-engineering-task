import pandas as pd
def load_to_json(df):
    df.to_json('output/tagaddod_datana_analysis.json',orient="records", indent=4)
    print("DataFrame loaded into json")