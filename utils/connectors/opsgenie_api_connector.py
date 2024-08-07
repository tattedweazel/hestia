import json
import requests
from utils.connectors.api_connector import ApiConnector


class OpsGenieAPIConnector(ApiConnector):

    def __init__(self, file_location):
        super().__init__(file_location)
        self.response = None
        self.url = self.creds["OPSGENIE_URL"]
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"GenieKey {self.creds['OPSGENIE_TOKEN']}"
        }


    def build_alert(self, log_line, job_name):
        return {
            "message": f'{job_name}: {log_line["message"]}',
            "details": log_line
        }


    def alert(self, log_line, job_name):
        data = self.build_alert(log_line, job_name)
        self.response = requests.post(url=self.url, headers=self.headers, data=json.dumps(data))
