from google.cloud import bigquery

from carepay_etl.models.carepay_csv_table import CarePayCsvTable
from carepay_etl.utils.service_account_util import *
from carepay_etl.utils.constants import *

google_app_credential = get_credential()
client: bigquery.Client = bigquery.Client()


def get_or_create_default_dataset(dataset_id: str) -> bigquery.Dataset:
    project_dataset = None
    project_id = google_app_credential['project_id']
    if dataset_id not in client.list_datasets(project_id):
        print(f"creating new dataset with name {dataset_id}")
        bq_dataset_id = bigquery.Dataset('{}.{}'.format(project_id, dataset_id))
        project_dataset = client.create_dataset(bq_dataset_id)
    else:
        print(f"dataset {dataset_id} already exists")
    return project_dataset


def create_bq_tables(table_names: list, bq_dataset_id: str = care_pay_dataset_id):
    project_dataset: bigquery.Dataset = get_or_create_default_dataset(bq_dataset_id)
    for name in table_names:
        try:
            # todo: function to create table schema
            # schema = [
            # bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
            #  bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),]
            table_ref = project_dataset.table(name)
            table: bigquery.Table = bigquery.Table(table_ref)
            client.create_table(table)
            print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        except Exception as e:
            print(
                f"an error occurred while creating tables in project"
                f" {data_challenge_project_name} using dataset {project_dataset.friendly_name}",
                e)


def load_csv_to_bq(care_pay_csv_tables: list[CarePayCsvTable]) -> bool:
    return False


def gcp_to_df(sql: str):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()


if __name__ == '__main__':
    tables = ["aaa", "bbb", "cccc"]
    create_bq_tables(tables, "helloworldxyz")
