import pandas as pd

from carepay_etl.models.output_format import *
from carepay_etl.utils.constants import *


class Transform:

    def __int__(self, dataframe: pd.DataFrame, output_format: OutputFormat, file_name: str):
        self._dataframe = dataframe
        self._output_format = output_format
        self._file_name = file_name

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
            os.mkdir(file_path_to_save)

    def convert_to_type(self):
        if isinstance(self._output_format, CsvOutputFormat):
            self._get_or_create_output_file_dir(csv_files_dir)
            self._dataframe.to_csv("{}/{}.csv".format(csv_files_dir, self._file_name))

        if isinstance(self._output_format, ParquetOutputFormat):
            self._get_or_create_output_file_dir(parquet_files_dir)
            self._dataframe.to_parquet("{}/{}.parquet.gzip".format(parquet_files_dir, self._file_name),
                                       compression="gzip")

        if isinstance(self._output_format, AvroOutputFormat):
            self._get_or_create_output_file_dir(avro_files_dir)
            raise "Avro Format is currently not supported for this version"
