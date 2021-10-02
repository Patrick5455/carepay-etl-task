from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

from carepay_etl.models.bq_csv_config import BQCSVConfig
from carepay_etl.models.carepay_table import CarePayTable
from carepay_etl.utils.service_account_util import *
from carepay_etl.utils.constants import *

google_app_credential = get_credentialAsJson()
client: bigquery.Client = bigquery.Client()


def get_or_create_default_dataset(bq_dataset_id: str) -> bigquery.Dataset:
    print("getting or creating new dataset")
    project_id = google_app_credential['project_id']
    datasets = list(client.list_datasets())
    print(f"list of dataset {len(datasets)}")
    if len(datasets) > 0:
        for dataset in datasets:
            if bq_dataset_id.lower().strip() == str(dataset.dataset_id).lower().strip():
                print(f"dataset {bq_dataset_id} already exists")
                dataset_ref = client.dataset(bq_dataset_id)
                return client.get_dataset(dataset_ref)

    print(f"creating new dataset with name {bq_dataset_id}")
    bq_dataset_id = bigquery.Dataset('{}.{}'.format(project_id, bq_dataset_id))
    project_dataset = client.create_dataset(bq_dataset_id)
    return project_dataset


def create_bq_tables(table_names: list[str], bq_dataset_id: str = care_pay_dataset_id):
    project_dataset: bigquery.Dataset = get_or_create_default_dataset(bq_dataset_id)
    for name in table_names:
        print(f"checking if table {name} exists  or creating new table")
        try:
            table_ref = project_dataset.table(name)
            if client.get_table(table_ref) is None:
                table: bigquery.Table = bigquery.Table(table_ref)
                client.create_table(table)
                print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        except Exception as e:
            print(
                f"an error occurred while creating tables in project"
                f" {data_challenge_project_name} using dataset {project_dataset.friendly_name}",
                e)


def load_table_files_to_bq(carepay_tables: list[CarePayTable],
                           source_format: bigquery.SourceFormat) -> bool:
    tables_count = len(carepay_tables)
    print(f"length of carepay tables: {tables_count}")
    counter = 0
    for table in carepay_tables:
        bq_csv_config = BQCSVConfig(client,
                                    table.get_dataset_name(),
                                    table.get_table_name(),
                                    source_format)

        with open(table.get_table_csv_data_path(), "rb") as source_file:
            try:
                print(f"loading table into target: {table.get_dataset_name()}:{table.get_table_name()}")
                job = client.load_table_from_file(source_file, bq_csv_config.get_table_ref(),
                                                  job_config=bq_csv_config.get_job_config())
            except ValueError as ve:
                print(
                    f"an exception occurred while performing load table job to target "
                    f"{table.get_dataset_name()}:{table.get_table_name()}", ve)

        try:
            job.result()
            counter += 1
            print(
                "Loaded {} rows into {}:{}.".format
                (job.output_rows, table.get_dataset_name(), table.get_table_name()))
        except GoogleAPICallError as ge:
            print(
                f"an exception occurred while performing load table job to target"
                f" {table.get_dataset_name()}:{table.get_table_name()}", ge)

    if counter == tables_count:
        print(f"{counter} of {tables_count} tables loaded successfully")
        return True
    return False


def gcp_to_df(sql: str):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()
