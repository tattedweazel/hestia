import json
import requests
from utils.connectors.api_connector import ApiConnector


class TikTokAdsApiConnector(ApiConnector):

	def __init__(self, file_location):
		super().__init__(file_location)
		self.app_id = self.creds["TIKTOK_ADS_APP_ID"]
		self.endpoints = {
			'auth': 'oauth2/access_token/',
			'sync_report': 'reports/integrated/get/'
		}
		self.data_level_dimensions = {
			"AUCTION_ADVERTISER": "advertiser_id",
			"AUCTION_CAMPAIGN": "campaign_id",
			"AUCTION_ADGROUP": "adgroup_id",
			"AUCTION_AD": "ad_id"
		}
		self.data_level_granularity = {
			"day": "stat_time_day",
			"hour": "stat_time_hour"
		}
		self.attribute_metrics = {
			"AUCTION_ADVERTISER": [],
			"AUCTION_CAMPAIGN": ["campaign_name", "objective_type"],
			"AUCTION_ADGROUP": ["campaign_name", "objective_type", "campaign_id", "adgroup_name", "placement", "tt_app_id", "tt_app_name", "mobile_app_id", "promotion_type", "dpa_target_audience_type"],
			"AUCTION_AD": ["campaign_name", "objective_type", "campaign_id", "adgroup_name", "placement", "adgroup_id", "ad_name", "ad_text", "tt_app_id", "tt_app_name", "mobile_app_id", "promotion_type", "dpa_target_audience_type"]
		}
		self.basic_metrics = ["spend", "cpc", "cpm", "impressions", "clicks", "ctr", "reach", "cost_per_1000_reached", "conversion", "cost_per_conversion", "conversion_rate", "result", "cost_per_result", "result_rate", "frequency"]


	def get_request_headers(self, endpoint=None):
		headers = {
			"Content-Type": "application/json"
		}
		if endpoint != 'auth':
			headers['Access-Token'] = {self.creds["TIKTOK_ADS_TOKEN"]}

		return headers


	def get_request_url(self, endpoint):
		return f'https://{self.creds["TIKTOK_ADS_INSTANCE_URI"]}/{self.endpoints[endpoint]}'


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

	def get_data_level_dimensions(self, data_level, dimension_granularity):
		return [
			self.data_level_dimensions[data_level],
			self.data_level_dimensions[dimension_granularity]
		]

	def get_metrics(self, data_level):
		return self.attribute_metrics[data_level].extend(self.basic_metrics)


	def generate_auth(self, endpoint):
		headers = self.get_request_headers(endpoint)
		request_data = {
			"app_id": self.app_id,
			"auth_code": "",  # Temporary. Supplied by Tiktok.
			"secret": self.creds["TIKTOK_ADS_SECRET"],
		}
		data = self.build_request_data(request_data)
		response = requests.post(self.get_request_url(endpoint), headers=headers, data=data)
		return {
			'code': response.status_code,
			'message': json.loads(response.text)['message'],
			'data': json.loads(response.text)
		}
