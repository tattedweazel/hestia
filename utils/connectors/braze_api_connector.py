import json
import requests
from utils.connectors.api_connector import ApiConnector


class BrazeApiConnector(ApiConnector):

	def __init__(self, file_location):
		super().__init__(file_location)
		self.endpoints = {
			'export_segment': 'users/export/segment',
			'import_users_identify': 'users/identify',
			'update_users': 'users/track?=',
			'delete_users': 'users/delete'
		}
		self.app_id = self.creds["BRAZE_APP_ID"]


	def get_request_headers(self, endpoint):
		headers = {
			'Content-Type': 'application/json',
			'Authorization': f'Bearer {self.creds["BRAZE_API_KEY"]}',
		}
		if endpoint == 'export_segment':
			headers['output_format'] = 'zip'

		return headers


	def get_request_url(self, endpoint):
		return f'https://{self.creds["BRAZE_INSTANCE_URL"]}/{self.endpoints[endpoint]}'


	def build_request_data(self, data_dict):
		return json.dumps(data_dict)


	def make_request(self, endpoint, request_data):
		headers = self.get_request_headers(endpoint)
		data = self.build_request_data(request_data)
		response = requests.post(self.get_request_url(endpoint), headers=headers, data=data)
		return {
			'code': response.status_code,
			'message': json.loads(response.text)['message'],
			'data': json.loads(response.text)
		}
