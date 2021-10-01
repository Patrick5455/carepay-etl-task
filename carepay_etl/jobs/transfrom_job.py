import pandas as pd

from carepay_etl.models.output_format import *;


class Transform:

    def __int__(self, dataframe: pd.DataFrame, output_format: OutputFormat, path_to_save_file: str, file_name: str):
        self._dataframe = dataframe
        self._output_format = output_format
        self._path_to_save_file = path_to_save_file
        self._file_name = file_name

    def get_dataframe(self):
        return self._dataframe

    def set_dataframe(self, dataframe):
        self._dataframe = dataframe

    def get_output_format(self):
        return self._output_format

    def set_output_format(self, output_format):
        self._output_format = output_format

    def fill_null(self):
        pass

    def convert_to_type(self):
        if isinstance(self._output_format, CsvOutputFormat):
            return self._dataframe.to_csv()

        if isinstance(self._output_format, ParquetOutputFormat):
            return self._dataframe.to_parquet()

        if isinstance(self._output_format, AvroOutputFormat):
            raise "Avro Format is currently not supported for this version"
