from google.cloud import bigquery

# Construct a BigQuery client object
client = bigquery.Client()

table_id = "ebd.project.school_lat_long"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("lat_long", "STRING"),
        bigquery.SchemaField("school", "STRING"),
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
uri = "school_lat_long.csv"

load_job = client.load_Table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request

load_job.result()  # waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request
print("Loaded {} rows.".format(destination_table.num_rows))
