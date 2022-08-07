import pandas as pd
from functools import reduce

def parse_json(loaded_json):
    """Desolve the structure of json file and transform it 
    to Pandas DataFrame

    Args:
        loaded_json (dict): dictionary of the json file

    Returns:
        DataFrame: Table of the json file content
    """
    dict_list = []
    for key in loaded_json.keys():
        if key == "meta-data":
            dict_list.append({'collector_id_': loaded_json[key]})
            continue
        value = loaded_json[key]
        value['dict_key'] = key
        dict_list.append(value)
    print("Desolved the structure of json file and transform to Pandas DataFrame")
    return pd.DataFrame(dict_list)


def fix_meta_data_row(df):
    """There is a problem where there is a key named mata-data
    inside the keys of the recorded data

    Args:
        df (DataFrame): Json file data

    Returns:
        DataFrame: Json file data
    """
    if 'meta-data' not in df:
        print("No meta-data problem")
        return df
    meta_data_idx = df.index[~df['meta-data'].isna()].tolist()[0]
    df.loc[meta_data_idx,['device_id', 'latitude', 'longitude', 'snapshot_datetime']] = df['meta-data'].iloc[meta_data_idx]
    df.drop('meta-data', axis=1, inplace=True)
    print("Fixed meta-data row")
    return df

def convert_collector_id_to_meta_id(df):
    """Set the collector ID to the collector ID in meta-data
    since it is a single truth value

    Args:
        df (DataFrame): Json file data

    Returns:
        DataFrame: Json file data
    """
    df_ = pd.json_normalize(df['collector_id_'])
    df['collector_id'] = df_['collector_id'][df_['collector_id'].notna()].unique()[0]
    df.drop('collector_id_', axis=1, inplace=True)
    print("Converted collector_id to meta-data collector_id")
    return df

def change_destination_request_id_null_to_0(df):
    """Set null values to 0 in destination_request_id
    NOTE: Value 0 means null

    Args:
        df (DataFrame): Json file data

    Returns:
        DataFrame: Json file data
    """
    if 'destination_request_id' not in df:
        print("destination_request_id doesn't exist")
        df['destination_request_id'] = 0
        return df
    df.loc[df['destination_request_id'].isna(), 'destination_request_id'] = 0
    print("Changed destination_request_id null values to 0")
    return df

def drop_null(df):
    """Drop null values based on latitude and longitude since
    these columns is very important for the map

    Args:
        df (DataFrame): Json file data

    Returns:
        DataFrame: Json file data
    """
    df = df.dropna(subset=['device_id', 'latitude', 'longitude'])
    print("Drop null values based on latitude and longitude")
    return df

def drop_duplicates_based_on_snapshot(df):
    """Drop duplicates base on snapshot_datetime as 
    snapshot_datetime are the most unique colume in the dataset

    Args:
        df (DataFrame): Json file data

    Returns:
        DataFrame: Json file data
    """
    df = df.drop_duplicates(subset='snapshot_datetime', keep="last")
    print("Drop duplicates based on snapshot_datetime")
    return df

def fix_data_types(df):
    """Convert columns data types to the most suitable types

    Args:
        df (DataFrame): Json file data

    Returns:
        DataFrame: Json file data
    """
    columns = {'latitude':'float', 'longitude':'float', 'collector_id':'int'}
    if 'destination_request_id' in df: 
        columns['destination_request_id'] = 'int'
    df = df.astype(columns)
    df['snapshot_datetime'] = pd.to_datetime(df['snapshot_datetime'], format='%Y%m%d %H:%M:%S.%f')
    print("Changed data types")
    return df

def reorder_columns(df):
    """Reorder columns to the convenient order

    Args:
        df (DataFrame): Json file data

    Returns:
        DataFrame: Json file data
    """
    columns_order = ['dict_key', 'collector_id', 'device_id', 'destination_request_id', 'latitude', 'longitude','snapshot_datetime']
    df = df.reindex(columns=columns_order)
    print("Reodered columns")
    return df

def concat_json_files(df_list):
    return pd.concat(df_list, ignore_index=True)

def func_pipeline(df):
    """Pipeline for the transformation functions
    """
    df_new = reduce(
    lambda value, function: function(value),
        (
            fix_meta_data_row,
            convert_collector_id_to_meta_id,
            change_destination_request_id_null_to_0,
            drop_null,
            drop_duplicates_based_on_snapshot,
            fix_data_types,
            reorder_columns,
        ),
        df,
    )
    return df_new

def transform_json_files(json_files, json_loeaded_files):
    """Apply all specified transformations on json files

    Args:
        json_files (list): file name
        json_loeaded_file (list): Loaded json files

    Returns:
        DataFrame: Containes all data in json files tranformed
    """
    json_dfs = []
    for file, json_loeaded_file in zip(json_files, json_loeaded_files):
        print(f"Processing {file} \n")
        df = parse_json(json_loeaded_file)
        df = func_pipeline(df)
        json_dfs.append(df)
        print(f"\nFinished {file} \n\n")
    concat_df = concat_json_files(json_dfs)
    print("Concat all DataFrames")
    return concat_df

