from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

from carepay_etl.jobs.extract_job import *
from carepay_etl.jobs.transfrom_job import create_care_pay_tables_for_bq, Transformer
from carepay_etl.models.bq_csv_config import BQCSVConfig
from carepay_etl.models.carepay_table import CarePayTable
from carepay_etl.models.output_format import *
from carepay_etl.utils.service_account_util import *
from carepay_etl.utils.constants import *

google_app_credential = get_credentialAsJson()
client: bigquery.Client = bigquery.Client()


def get_or_create_default_dataset(bq_dataset_id: str) -> bigquery.Dataset:
    project_dataset = None
    project_id = google_app_credential['project_id']
    if bq_dataset_id not in client.list_datasets(project_id):
        print(f"creating new dataset with name {bq_dataset_id}")
        bq_dataset_id = bigquery.Dataset('{}.{}'.format(project_id, bq_dataset_id))
        project_dataset = client.create_dataset(bq_dataset_id)
    else:
        print(f"dataset {bq_dataset_id} already exists")
    return project_dataset


def create_bq_tables(table_names: list[str], bq_dataset_id: str = care_pay_dataset_id):
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


def load_table_files_to_bq(carepay_tables: list[CarePayTable],
                           source_format: bigquery.SourceFormat) -> bool:
    tables_count = len(carepay_tables)
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
        return True
    return False


def gcp_to_df(sql: str):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()


if __name__ == '__main__':
    # ["claims", "invoice_items", "invoices", "treatments"]

    carepay_db_connection = connectToMysql()
    tables_in_carepay = get_table_names(carepay_db_connection)
    for name in tables_in_carepay:
        table_df = get_table_df(name, carepay_db_connection)
        transformer = Transformer(dataframe=table_df, output_format=ParquetOutputFormat(), table_name=name)
        transformer.transform()
    carepay_db_connection.close()

    dataset_id = "helloworldabc12345678911"
    carepay_tables = create_care_pay_tables_for_bq(
        dataset_id, CsvOutputFormat(), csv_files_dir,
        create_bq_tables)

    # csv_tables = [
    #     CarePayTable(table_id=table_names[0],
    #                  table_data_path="../mysql_docker_build/data/claims.csv",
    #                  dataset_id=dataset_id),
    #
    #     CarePayTable(table_id=table_names[1],
    #                  table_data_path="../mysql_docker_build/data/invoice_items.csv",
    #                  dataset_id=dataset_id),
    #
    #     CarePayTable(table_id=table_names[2],
    #                  table_data_path="../mysql_docker_build/data/invoices.csv",
    #                  dataset_id=dataset_id),
    #
    #     CarePayTable(table_id=table_names[3],
    #                  table_data_path="../mysql_docker_build/data/treatments.csv",
    #                  dataset_id=dataset_id)
    # ]

    load_table_files_to_bq(carepay_tables, source_format=bigquery.SourceFormat.CSV)
