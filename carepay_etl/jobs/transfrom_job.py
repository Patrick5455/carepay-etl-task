import pandas as pd

from carepay_etl.models.carepay_table import CarePayTable
from carepay_etl.models.output_format import *
from carepay_etl.utils.constants import *
import glob


class Transformer:
    """

    """

    def __init__(self, dataframe: pd.DataFrame, output_format: OutputFormat, table_name: str):
        self._dataframe = dataframe
        self._output_format = output_format
        self._file_name = table_name.upper()

    def get_dataframe(self):
        return self._dataframe

    def set_dataframe(self, dataframe):
        self._dataframe = dataframe

    def get_output_format(self):
        return self._output_format

    def set_output_format(self, output_format):
        self._output_format = output_format

    def fill_null_or_na(self):
        for col in self._dataframe.columns:
            if self._dataframe[col].isnull or self._dataframe[col].isna:
                print(f"found null values in column {col}")
                self._dataframe[col].fillna(self._dataframe[col].mode(), inplace=True)

    def _get_or_create_output_file_dir(self, file_path_to_save: str):
        if not os.path.exists(file_path_to_save):
            os.makedirs(file_path_to_save)

    def convert_to_type(self):
        if isinstance(self._output_format, CsvOutputFormat):
            self._get_or_create_output_file_dir(csv_files_dir)
            self._dataframe.to_csv("{}/{}.{}".format(csv_files_dir, self._file_name, csv_extension))

        if isinstance(self._output_format, ParquetOutputFormat):
            self._get_or_create_output_file_dir(parquet_files_dir)
            self._dataframe.to_parquet("{}/{}.{}".format(parquet_files_dir, self._file_name, parquet_extension),
                                       compression="gzip")

        if isinstance(self._output_format, AvroOutputFormat):
            self._get_or_create_output_file_dir(avro_files_dir)
            raise "Avro Format is currently not supported for this version"

    def transform(self):
        self.fill_null_or_na()
        self.convert_to_type()


def create_care_pay_tables_for_bq(dataset_id: str,
                                  output_format: OutputFormat,
                                  file_dir: str, create_bq_table_func) -> list:
    table_files = dict()
    table_names = []
    care_pay_tables = []

    def create_table_files(file_extension):
        for file in glob.glob(f"{file_dir}/*.{file_extension}"):
            file_table_name = str(file).split("/")[-1].split(".")[0]
            table_files[file_table_name] = file
            table_names.append(file_table_name)

    if isinstance(output_format, ParquetOutputFormat):
        create_table_files(parquet_extension)

    if isinstance(output_format, AvroOutputFormat):
        create_table_files(avro_extension)

    if isinstance(output_format, CsvOutputFormat):
        create_table_files(csv_extension)

    create_bq_table_func(table_names, dataset_id)

    for table_name in table_names:
        care_pay_tables.append(
            CarePayTable(table_id=table_name,
                         table_data_path=table_files[table_name],
                         dataset_id=dataset_id),
        )

    return care_pay_tables
