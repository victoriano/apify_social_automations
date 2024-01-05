import os
import pandas as pd
import json

def merge_json_files(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith('.json') and os.path.isfile(os.path.join(folder_path, f))]
    merged_data = pd.DataFrame()
    for file in files:
        file_path = os.path.join(folder_path, file)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        data = pd.json_normalize(data)
        merged_data = pd.concat([merged_data, data], ignore_index=True)
    return merged_data

def merge_csv_files(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith('.csv') and os.path.isfile(os.path.join(folder_path, f))]
    merged_data = pd.DataFrame()
    for file in files:
        file_path = os.path.join(folder_path, file)
        data = pd.read_csv(file_path, sep=',', header=0, on_bad_lines='skip')
        merged_data = pd.concat([merged_data, data], ignore_index=True)
    return merged_data

def csv_to_json(csv_file_path, json_file_path):
    df = pd.read_csv(csv_file_path)
    df.to_json(json_file_path, orient='records', lines=True)
    print(f"Converted {csv_file_path} to {json_file_path}")

def json_to_csv(json_file_path, csv_file_path):
    df = pd.read_json(json_file_path, lines=True)
    df.to_csv(csv_file_path, index=False)
    print(f"Converted {json_file_path} to {csv_file_path}")

def remove_duplicates_from_file(file_path, subset=None, file_format='csv'):
    if file_format == 'csv':
        df = pd.read_csv(file_path)
    elif file_format == 'json':
        df = pd.read_json(file_path, lines=True)
    else:
        print(f"Unsupported file format: {file_format}")
        return

    df.drop_duplicates(subset=subset, keep='first', inplace=True)

    if file_format == 'csv':
        df.to_csv(file_path, index=False)
    elif file_format == 'json':
        df.to_json(file_path, orient='records', lines=True)

    print(f"Duplicates removed from {file_path}")

def infer_file_format(folder_path):
    files = os.listdir(folder_path)
    for file in files:
        if file.endswith('.json'):
            return 'json'
        elif file.endswith('.csv'):
            return 'csv'
    return None

def merge_files_in_folder(folder_path, output_format='json'):
    input_format = infer_file_format(folder_path)
    if input_format is None:
        print("No json or csv files found in the folder.")
        return

    if input_format == 'json':
        merged_data = merge_json_files(folder_path)
        if output_format == 'json':
            output_file = os.path.join(folder_path, 'merged_data.json')
            merged_data.to_json(output_file, orient='records', lines=True)
        elif output_format == 'csv':
            output_file = os.path.join(folder_path, 'merged_data.csv')
            merged_data.to_csv(output_file, index=False)
    elif input_format == 'csv':
        merged_data = merge_csv_files(folder_path)
        if output_format == 'csv':
            output_file = os.path.join(folder_path, 'merged_data.csv')
            merged_data.to_csv(output_file, index=False)
        elif output_format == 'json':
            output_file = os.path.join(folder_path, 'merged_data.json')
            merged_data.to_json(output_file, orient='records', lines=True)

    print(f"Merged file created at {output_file}")