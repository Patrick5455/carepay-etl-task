class OutputFormat:

    def __int__(self):
        pass

    def get_format_type(self) -> str:
        return ""


class CsvOutputFormat(OutputFormat):

    def __init__(self):
        super()
        self._format_type = "csv"

    def get_format_type(self) -> str:
        return self._format_type


class AvroOutputFormat(OutputFormat):

    def __init__(self):
        super()
        self._format_type = "avro"

    def get_format_type(self) -> str:
        return self._format_type


class ParquetOutputFormat(OutputFormat):

    def __init__(self):
        super()
        self._format_type = "parquet"

    def get_format_type(self) -> str:
        return self._format_type
