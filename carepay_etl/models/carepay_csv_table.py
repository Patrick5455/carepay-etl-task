from carepay_etl.utils.constants import care_pay_dataset_id


class CarePayCsvTable:

    """"
     :param
        table_id
        table_csv_data_path
        dataset_name
    """

    def __init__(self, table_id, table_csv_data_path, dataset_name=care_pay_dataset_id):
        self._table_name = table_id
        self._table_csv_data_path = table_csv_data_path
        self._dataset_name = dataset_name

    def __repr__(self):
        print(f'table_name: {self._table_name}\ntable_csv_data_path: {self._table_csv_data_path}'
              f'\ndataset_name: {self._dataset_name}')

    def get_table_name(self):
        return self._table_name

    def set_table_name(self, table_name):
        self._table_name = table_name

    def get_table_csv_data_path(self):
        return self._table_csv_data_path

    def set_table_csv_data_path(self, table_csv_data_path):
        self._table_csv_data_path = table_csv_data_path

    def get_dataset_name(self):
        return self._dataset_name

    def set_dataset_name(self, dataset_name):
        self._dataset_name = dataset_name

