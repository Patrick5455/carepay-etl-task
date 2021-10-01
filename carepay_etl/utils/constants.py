import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

data_challenge_project_name = 'data-challenge'
care_pay_dataset_id = 'carepay'

carepay_db_tables = ["CLAIM", "INVOICE", "INVOICE_ITEM", "TREATMENT"]
