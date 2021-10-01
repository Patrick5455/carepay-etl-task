from google.cloud import bigquery


class BQCSVConfigs:
    """"
    :param
       client
       dataset_id
       table_id
       job_config_autodetect
   """

    def __init__(self, client: bigquery.client, dataset_id: str, table_id: str, sourceFormat: bigquery.SourceFormat=bigquery.SourceFormat.CSV,
                 job_config_autodetect=True):
        self._dataset_ref = client.dataset(dataset_id)
        self._table_ref = self._dataset_ref.table(table_id)
        self._job_config = bigquery.LoadJobConfig()
        self._job_config_source_format = sourceFormat
        self._job_config_autodetect = job_config_autodetect

    def get_dataset_ref(self):
        return self._dataset_ref

    def set_dataset_ref (self, dataset_ref):
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



