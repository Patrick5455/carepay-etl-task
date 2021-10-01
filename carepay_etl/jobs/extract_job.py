import pandas as pd
from carepay_etl.utils.constants import *

import mysql.connector as connection


def connectToMysql() -> connection:
    global carepay_db_connection
    try:
        carepay_db_connection = connection.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, passwd=DB_PASS,
                                                   use_pure=True)
        print(f"successfully connected with {DB_NAME} DB")
        return carepay_db_connection
    except Exception as db_error:
        carepay_db_connection.close()
        print(f"something went wrong while trying to  connect with {DB_NAME} DB", str(db_error))


def get_table_names(carepay_db_connection: connection) -> list[str]:
    get_schema_table_names = f"SHOW TABLES IN {DB_NAME}"
    table_names_in_care_pay = pd.read_sql(get_schema_table_names, carepay_db_connection)["Tables_in_carepay"].tolist()
    return table_names_in_care_pay


def get_table_df(table_name:str, carepay_db_connection: connection) -> pd.DataFrame:

    fetch_all_query = f"SELECT * FROM {table_name}"
    table__df = pd.read_sql(fetch_all_query, carepay_db_connection)
    return table__df

#
# if __name__ == '__main__':
#     carepay_db_connection = connectToMysql()
#     tables_in_carepay = get_table_names(carepay_db_connection)
#     for name in tables_in_carepay:
#         print(get_table_df(name, carepay_db_connection))
#     carepay_db_connection.close()
#
#
