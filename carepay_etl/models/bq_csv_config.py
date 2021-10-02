from google.cloud import bigquery


class BQCSVConfig:
    """"
    :param
       client
       dataset_id
       table_id
       sourceFormat
       job_config_autodetect
   """

    def __init__(self, client: bigquery.Client, dataset_id: str, table_id: str,
                 source_format: bigquery.SourceFormat = bigquery.SourceFormat.CSV,
                 job_config_autodetect=True):
        self._dataset_ref = client.dataset(dataset_id)
        self._table_ref = self._dataset_ref.table(table_id)
        self._job_config = bigquery.LoadJobConfig(autodetect=True, source_format=source_format,
                                                  allow_quoted_newlines=True)
        self._job_config_source_format = source_format
        self._job_config_autodetect = job_config_autodetect

    def get_dataset_ref(self):
        return self._dataset_ref

    def set_dataset_ref(self, dataset_ref):
        self._dataset_ref = dataset_ref

    def get_table_ref(self):
        return self._table_ref

    def set_table_ref(self, table_ref):
        self._table_ref = table_ref

    def get_job_config(self):
        return self._job_config

    def set_job_config(self, job_config):
        self._job_config = job_config

    def get_job_config_source_format(self):
        return self._job_config_source_format

    def set_job_config_source_format(self, job_config_source_format):
        self._job_config.source_format = job_config_source_format
        self._job_config_source_format = job_config_source_format

    def get_job_config_autodetect(self):
        return self._job_config_autodetect

    def set_job_config_autodetect(self, job_config_autodetect):
        self._job_config.autodetect = job_config_autodetect
        self._job_config_autodetect = job_config_autodetect
