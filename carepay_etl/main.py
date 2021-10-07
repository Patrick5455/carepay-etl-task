from carepay_etl.jobs.extract_job import *
from carepay_etl.jobs.load_job import *
from carepay_etl.jobs.transfrom_job import *
from carepay_etl.models.output_format import *
from carepay_etl.utils.constants import *


def main():
    """
         The main method serves as the entry point to this script.
         It would be used to create a connection to the source databsase
         and begin the extraction job  followed by the modelling/transformation job
         and then load to the target db
     """

    print("\nstarted extraction job\n")
    carepay_db_connection = connectToMysql()
    tables_in_carepay = get_table_names(carepay_db_connection)

    print("\nstarted transformation job\n")
    for name in tables_in_carepay:
        table_df = get_table_df(name, carepay_db_connection)
        transformer = Transformer(dataframe=table_df, output_format=ParquetOutputFormat(), table_name=name)
        transformer.transform()
    carepay_db_connection.close()

    dataset_id = care_pay_dataset_id
    carepay_tables = create_care_pay_tables_for_bq(
        dataset_id, ParquetOutputFormat(), parquet_files_dir)

    print("\nstarted load job\n")
    load_table_files_to_bq(carepay_tables, source_format=bigquery.SourceFormat.PARQUET)

    print("\n ETL Job completed successfully")


if __name__ == "__main__":
    main()
