import shutil
from tempfile import mkstemp

import pandas as pd

from carepay_etl.jobs.load_job import create_bq_tables
from carepay_etl.models.carepay_table import CarePayTable
from carepay_etl.models.output_format import *
from carepay_etl.utils.constants import *
import glob
from fastparquet import write
import pandavro as pdx

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
                print(f"fixing null values in column {col}")
                self._dataframe[col].fillna(self._dataframe[col].mode(), inplace=True)

    def _get_or_create_output_file_dir(self, file_path_to_save: str):
        if not os.path.exists(file_path_to_save):
            os.makedirs(file_path_to_save)

    def fix_ascii_for_parquet_files(self, file_path, orignal_string, new_string):
        print("fixing bigquery bug with ASCII  (0) character in parquet files")
        # Create temp file
        fh, abs_path = mkstemp()
        with os.fdopen(fh, 'w', encoding='utf-8') as new_file:
            with open(file_path, encoding='utf-8', errors='replace') as old_file:
                print("\nCurrent line: \t")
                i = 0
                for line in old_file:
                    print(i, end="\r", flush=True)
                    i = i + 1
                    line = line.replace(orignal_string, new_string)
                    # line = line.replace("", " ")
                new_file.write(line)
        # Copy the file permissions from the old file to the new file
        shutil.copymode(file_path, abs_path)
        # Remove original file
        os.remove(file_path)
        # Move new file
        shutil.move(abs_path, file_path)

    def convert_to_type(self):
        if isinstance(self._output_format, CsvOutputFormat):
            self._get_or_create_output_file_dir(csv_files_dir)
            self._dataframe.to_csv("{}/{}.{}".format(csv_files_dir, self._file_name, csv_extension))

        if isinstance(self._output_format, ParquetOutputFormat):
            self._get_or_create_output_file_dir(parquet_files_dir)
            file_path = "{}/{}.{}".format(parquet_files_dir, self._file_name, parquet_extension)
            self._dataframe.to_parquet(file_path)
            # self.fix_ascii_for_parquet_files(file_path, '\0', '')

        if isinstance(self._output_format, AvroOutputFormat):
            self._get_or_create_output_file_dir(avro_files_dir)
            file_path = "{}/{}.{}".format(avro_files_dir, self._file_name, avro_extension)
            pdx.to_avro(file_path, self._dataframe)
            saved = pdx.read_avro(file_path)
            print(saved)

    def transform(self):
        self.fill_null_or_na()
        self.convert_to_type()


def create_care_pay_tables_for_bq(dataset_id: str,
                                  output_format: OutputFormat,
                                  file_dir: str) -> list:
    table_files = dict()
    table_names = []
    care_pay_tables = []

    def create_table_files(file_extension):
        for file in glob.glob(f"{file_dir}/*.{file_extension}"):
            file_table_name = str(file).split("/")[-1].split(".")[0]
            table_files[file_table_name] = file
            table_names.append(file_table_name)

    if isinstance(output_format, ParquetOutputFormat):
        print("creating parquet table files")
        create_table_files(parquet_extension)

    if isinstance(output_format, AvroOutputFormat):
        print("creating avro table files")
        create_table_files(avro_extension)

    if isinstance(output_format, CsvOutputFormat):
        print("creating csv table files")
        create_table_files(csv_extension)

    create_bq_tables(table_names, dataset_id)

    for table_name in table_names:
        print("creating carepay tables")
        care_pay_tables.append(
            CarePayTable(table_id=table_name,
                         table_data_path=table_files[table_name],
                         dataset_id=dataset_id),
        )

    return care_pay_tables
