import requests
import os
from data_utils import merge_files_in_folder, remove_duplicates_from_file

def get_task_table_name(task_id):
    task_id = task_id.replace("/", "~")
    task_name = task_id.split("~")[-1].replace("-", "_")
    return task_name

def get_task_datasets_path(task_id):
    task_id = task_id.replace("/", "~")
    datasets_path = os.path.join('datasets', task_id)
    return datasets_path

def get_task_merged_file_path(task_id, output_format='csv'):
    datasets_path = get_task_datasets_path(task_id)
    task_name = get_task_table_name(task_id) + f".{output_format}"
    file_path = os.path.join(datasets_path, task_name)
    return file_path

def get_apify_tasks(api_token):
    url = "https://api.apify.com/v2/actor-tasks"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    tasks = response.json()

    # Extract required fields and add to a new list
    tasks_list = []
    for task in tasks['data']['items']:
        task_info = {
            'id': task['id'],
            'name': task['name'],
            'createdAt': task['createdAt'],
            'totalRuns': task['stats']['totalRuns'] if 'totalRuns' in task['stats'] else 0,
            'lastRunStartedAt': task['stats']['lastRunStartedAt'] if 'lastRunStartedAt' in task['stats'] else None
        }
        tasks_list.append(task_info)

    # Sort the list by 'lastRunStartedAt' in descending order
    tasks_list.sort(key=lambda x: x['lastRunStartedAt'] if x['lastRunStartedAt'] is not None else '', reverse=True)
    return tasks_list

def count_apify_tasks(api_token):
    tasks = get_apify_tasks(api_token)
    return len(tasks)

def get_apify_runs(api_token, task_id):
    task_id = task_id.replace("/", "~")
    url = f"https://api.apify.com/v2/actor-tasks/{task_id}/runs"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    runs = response.json()
    if 'error' in runs:
        return runs['error']['message']
    return runs['data']['items']

def extract_apify_runs_datasets_ids(runs):
    if isinstance(runs, str):
        print(f"Error in get_apify_runs: {runs}")
        return []
    result = []
    for run in runs:
        data = {
            'startedAt': run['startedAt'],
            'defaultDatasetId': run['defaultDatasetId']
        }
        result.append(data)
    return result

def download_apify_dataset(api_token, dataset_id, task_id, format='json'):
    file_path = get_task_datasets_path(task_id)
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={api_token}&format={format}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers, stream=True)
    
    if response.status_code == 200:
        with open(os.path.join(file_path, f'{dataset_id}.{format}'), 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"File downloaded at {os.path.join(file_path, f'{dataset_id}.{format}')}")
    else:
        print("Error occurred while downloading file.")

def download_all_datasets_for_task(api_token, task_id, format='json'):
    # Get all runs for the task
    runs = get_apify_runs(api_token, task_id)
    # Extract dataset ids from the runs
    datasets = extract_apify_runs_datasets_ids(runs)
    # Download each dataset
    for dataset in datasets:
        download_apify_dataset(api_token, dataset['defaultDatasetId'], task_id, format=format)

def merge_task_datasets(task_id, output_format='csv', remove_duplicates=False, subset=None):
    task_id = task_id.replace("/", "~")
    folder_path = os.path.join('datasets', task_id)
    output_file_name = get_task_table_name(task_id)
    merge_files_in_folder(folder_path, output_file_name, output_format)

    if remove_duplicates:
        merged_file_path = get_task_merged_file_path(task_id, output_format)
        remove_duplicates_from_file(merged_file_path, subset=subset, file_format=output_format)