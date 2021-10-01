import pandas as pd

from carepay_etl.models.output_format import *
from carepay_etl.utils.constants import csv_files_dir


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

    def convert_to_type(self):
        if isinstance(self._output_format, CsvOutputFormat):
            self._dataframe.to_csv("{}/{}.csv".format(csv_files_dir, self._file_name))

        if isinstance(self._output_format, ParquetOutputFormat):
            self._dataframe.to_parquet("{}/{}.parquet.gzip".format(csv_files_dir, self._file_name), compression="gzip")

        if isinstance(self._output_format, AvroOutputFormat):
            raise "Avro Format is currently not supported for this version"



