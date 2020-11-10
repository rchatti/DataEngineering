from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "shining-landing-763.TEMP_OPS.SPROUTS_UPC_TO_DELETE_UPC_LIST"

schema = [
    bigquery.SchemaField("UPC", "STRING"),
]

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema = schema
)

uri = "gs://spins-tmp-ext/home/rchatti/sprouts_list_of_upcs.csv"
load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))