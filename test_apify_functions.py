from apify_tasks import get_apify_tasks, download_apify_dataset, get_apify_runs, extract_apify_runs_datasets_ids, download_all_datasets_for_task
from dotenv import load_dotenv
import os

#Load API Token
load_dotenv()
api_token = os.getenv("API_TOKEN")

#Some IDs to test functions
dataset_id = "cYEYHZoRfZuwychN7"
task_id = "graphext~scrape-train-tweets"

#Print All Saved Tasks by last run started at
#tasks = get_apify_tasks(api_token)
#print(tasks)

#Get a list of IDs of all runs of given Task
#runs = get_apify_runs(api_token, task_id)
#runs_ids = extract_apify_runs_datasets_ids(runs)

#Download a dataset
#last_dataset_id = runs_ids[0]["defaultDatasetId"]
#download_apify_dataset(api_token, last_dataset_id, format="csv")

download_all_datasets_for_task(api_token, task_id, format='json')