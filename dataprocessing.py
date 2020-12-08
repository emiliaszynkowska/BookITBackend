
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
load_dotenv()

client = bigquery.Client()
filename = 'database.json'
dataset_id = 'dataset_bookit'
table_id = 'table-books'

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True

with open(filename, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="europe-west1",  
        job_config=job_config,
    )  

job.result()  

print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))