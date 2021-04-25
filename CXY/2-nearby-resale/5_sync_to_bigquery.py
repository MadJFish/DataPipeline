import sys
import os
import glob


from google.cloud import bigquery
from google.cloud import storage

delete_dataset = False
dataset_name = ""
targetFileName = ""

if len(sys.argv) == 4:
    delete_dataset = sys.argv[1]
    dataset_name = sys.argv[2]
    targetFileName = sys.argv[3]

# Construct a BigQuery client object.
client = bigquery.Client()
LOCAL_DIR = "1-wip-data"
TARGET_FILE = f"./{LOCAL_DIR}/{targetFileName}"


def create_bigquery_dataset(dataset_name_input):
    # TODO(developer): Set table_id to the ID of the table to create.
    dataset_id_name = '{}.%s' % (dataset_name_input)
    dataset_id = dataset_id_name.format(client.project)
    table_id = "round-booking-311405.school_vs_hdb_resale.merged_1_get_filtered_distance"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-southeast1"

    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

def delete_bigquery_dataset(dataset_name_input):
    # Make an API request.
    client.delete_dataset(
        dataset_name_input, delete_contents=True, not_found_ok=True
    )

def upload_csv_to_bigquery():
    # some variables
    filename = TARGET_FILE # this is the file path to your csv
    dataset_id = dataset_name
    table_id = targetFileName.replace(".", "_")

    # tell the client everything it needs to know to upload our csv
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True

    print("Uploading to BigQuery {} into {}:{}.".format(TARGET_FILE, dataset_id, table_id))

    # load the csv into bigquery
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    # looks like everything worked :)
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

if(delete_dataset):
    delete_bigquery_dataset(dataset_name)

create_bigquery_dataset(dataset_name)

upload_csv_to_bigquery()