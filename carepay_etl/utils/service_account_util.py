import json
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "hardy-aleph-327710-5f1e524716c0.json"
google_app_json_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def get_credential(asString: bool = False):
    print(google_app_json_file)
    if asString:
        return google_app_json_file
    else:
        with open(google_app_json_file, 'r') as json_file:
            google_app_credential = json.load(json_file)
    return google_app_credential
