import os
import pandas as pd
import json

def merge_files_in_folder(folder_path, output_format='json'):
    # Get all files in the folder
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f)) and not f.startswith('.')]

    # Initialize an empty DataFrame for merged data
    merged_data = pd.DataFrame()

    # Process each file in the folder
    for file in files:
        file_path = os.path.join(folder_path, file)
        print(f"Processing file: {file_path}")  # Diagnostic print

        if output_format == 'json':
            # Read JSON file
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)  # Load the entire JSON file as a JSON object
                # Convert JSON object to DataFrame
                data = pd.json_normalize(data)
            except ValueError as e:
                print(f"Error reading {file_path}: {e}")
                continue
        elif output_format == 'csv':
            # Read CSV file, handling potential encoding issues and skipping bad lines
            try:
                data = pd.read_csv(file_path, sep=',', header=0, on_bad_lines='skip')
            except UnicodeDecodeError:
                data = pd.read_csv(file_path, sep=',', header=0, on_bad_lines='skip', encoding='latin1')

        # Diagnostic: Print the first few rows of the DataFrame to check if data is read correctly
        print("First few rows of the DataFrame:")
        print(data.head())

        # Concatenate the data from the current file to the merged DataFrame
        merged_data = pd.concat([merged_data, data], ignore_index=True)

    # Diagnostic: Check the final merged DataFrame
    print("Final merged DataFrame:")
    print(merged_data.head())

    # Write the merged data to a new file
    output_file = os.path.join(folder_path, f'merged_data.{output_format}')
    if output_format == 'json':
        merged_data.to_json(output_file, orient='records', lines=True)
    elif output_format == 'csv':
        merged_data.to_csv(output_file, index=False)

    print(f"Merged file created at {output_file}")

