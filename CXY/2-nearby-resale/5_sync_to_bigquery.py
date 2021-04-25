import os
import glob


from google.cloud import bigquery
from google.cloud import storage


INPUT_FILES = '1-wip-data/3_distance_classifier/*/*.csv'
OUTPUT_FILE = '1-wip-data/merged.csv'


def load_data_from_gcs(dataset, test10, source):
    bigquery_client = bigquery.Client(dataset)
    dataset = bigquery_client.dataset('FirebaseArchive')
    table = dataset.table(test10)
    job_name = str(uuid.uuid4())

    job = bigquery_client.load_table_from_storage(
        job_name, table, "gs://dataworks-356fa-backups/pullnupload.json")

    job.source_format = 'NEWLINE_DELIMITED_JSON'
    job.begin()
    wait_for_job(job)
    print("state of job is: " + job.state)
    print("errors: " + job.errors)