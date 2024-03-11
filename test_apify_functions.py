from apify_tasks import get_task_merged_file_path, get_task_table_name, get_apify_tasks, download_apify_dataset, merge_task_datasets, get_apify_runs, extract_apify_runs_datasets_ids, download_all_datasets_for_task
from snowflake_utils import create_table_from_csv, remove_duplicates

from dotenv import load_dotenv
import os

#Load API Token
load_dotenv()
api_token = os.getenv("API_TOKEN")

# Connection info for Snowflake
conn_info = {
    'user': os.getenv("SNOWFLAKE_USER"),
    'password': os.getenv("SNOWFLAKE_PASSWORD"),
    'role': os.getenv("SNOWFLAKE_ROLE"),
    'account': os.getenv("SNOWFLAKE_ACCOUNT"),
    'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
    'database': os.getenv("SNOWFLAKE_DATABASE"),
    'schema': os.getenv("SNOWFLAKE_SCHEMA")
}

#Some IDs to test functions
#task_id = "datasets/graphext~scrape-spanish-media"
#task_id = "graphext~scrape-spanish-media"
task_id = "graphext/scrape-train-tweets"

#Print All Saved Tasks by last run started at
#tasks = get_apify_tasks(api_token)
#print(tasks)


#Get a list of IDs of all runs of given Task
#runs = get_apify_runs(api_token, task_id)
#runs_ids = extract_apify_runs_datasets_ids(runs)

#Download a dataset
#dataset_id = "cYEYHZoRfZuwychN7"
#last_dataset_id = runs_ids[0]["defaultDatasetId"]
#download_apify_dataset(api_token, last_dataset_id, format="csv")

#Download data for a Task ID and merged them into a CSV removing duplicates
#Test download the last dataset by ID and push_to_table_from
download_all_datasets_for_task(api_token, task_id, format='json')
merge_task_datasets(task_id, output_format='csv', remove_duplicates=True)
#merge_task_datasets(task_id, output_format='csv', remove_duplicates=False)
#merge_task_datasets(task_id, output_format='csv', remove_duplicates=True, subset=['text'])

# Get paths to the Merged CSV file
csv_path = get_task_merged_file_path(task_id)
table_name = get_task_table_name(task_id)
# Name of the table you want to create

# Push Data to Snowflake
#Missing function for create or append to table instead of always creating the table, should be call pushing push_to_table_from(conn_info, csv_path)
create_table_from_csv(conn_info, csv_path)
remove_duplicates(conn_info, table_name)