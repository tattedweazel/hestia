import json
import requests
from utils.connectors.api_connector import ApiConnector


class TwitterAdsApiConnector(ApiConnector):

	def __init__(self, file_location):
		super().__init__(file_location)
		self.account_id = self.creds["TWITTER_ADS_ACCOUNT_ID"]
		self.endpoints = {
			'active_entities': f'{self.account_id}/active_entities',
			'sync_report': f'{self.account_id}'
		}


	def get_request_headers(self):
		headers = {
			"Content-Type": "application/json",
			'Authorization': f'Bearer {self.creds["TWITTER_ADS_TOKEN"]}',
		}

		return headers


	def get_request_url(self, endpoint):
		return f'https://{self.creds["TWITTER_ADS_INSTANCE_URI"]}/{self.endpoints[endpoint]}'


	def build_request_data(self, data_dict):
		return json.dumps(data_dict)


	def make_request(self, endpoint, request_data):
		headers = self.get_request_headers()
		data = self.build_request_data(request_data)
		response = requests.post(self.get_request_url(endpoint), headers=headers, data=data)
		return {
			'code': response.status_code,
			'message': json.loads(response.text)['message'],
			'data': json.loads(response.text)
		}

