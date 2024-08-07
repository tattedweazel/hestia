import os
from base.etl_jobv3 import EtlJobV3
from base.exceptions import ResponseCodeException
from time import sleep
from utils.connectors.tiktok_ads_api_connector import TikTokAdsApiConnector


class TikTokAdsBasicReportingJob(EtlJobV3):

	def __init__(self, data_level, target_date = None, db_connector = None, file_location=''):
		super().__init__(jobname = __name__, db_connector = db_connector)
		"""Endpoint currently returns data of up to 10,000 advertisements. If advertisements > 10,000, use campaign_ids, adgroup_ids, ad_ids as filter to batch."""
		self.file_location = file_location
		self.tiktok_ads_api_connector = TikTokAdsApiConnector(file_location=self.file_location)
		self.endpoint = "sync_report"
		self.data_level = data_level
		self.dimension_granularity = "day"  # Can only be 'day' or 'hour'

		self.request_data = {
			"advertiser_id": '<Advertiser ID>',  # TODO: Get Advertiser ID(s)
			"service_type": "AUCTION",
			"report_type": "BASIC",
			"data_level": self.data_level,
			"dimensions": self.tiktok_ads_api_connector.get_data_level_dimensions(self.data_level, self.dimension_granularity),  # TODO: Ask Phil if we want to group by Hour or Day.
			"metrics": self.tiktok_ads_api_connector.get_metrics(self.data_level),
			"start_date": "",  # TODO: Optional. Good for daily batching. YYYY-MM-DD.
			"end_date": "",  # TODO: Optional. Good for daily batching. YYYY-MM-DD.
			"lifetime": False,  # Should only be used for backfilling
			"page": 1,
			"page_size": 10
		}
		self.records = None

		self.loggerv3.alert = False


	def make_request(self):
		self.loggerv3.info('Requesting data')
		response = self.tiktok_ads_api_connector.make_request(self.endpoint, self.request_data)
		self.loggerv3.info(f'Response code: {response["code"]}')
		if response['code'] > 202:
			self.loggerv3.alert(f'Code {response["code"]}: {response["message"]}')
			raise ResponseCodeException(response)
		self.records = response['data']


	def parse_data(self):
		pass


	def write_to_redshift(self):
		pass



	def execute(self):
		self.loggerv3.info(f"Running TikTok Ads Basic Reporting Job - {self.data_level} on {self.target_date}")
		self.make_request()

		self.loggerv3.success("All Processing Complete!")
