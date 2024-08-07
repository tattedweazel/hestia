import os
from base.etl_jobv3 import EtlJobV3
from base.exceptions import ResponseCodeException
from time import sleep
from utils.connectors.twitter_ads_api_connector import TwitterAdsApiConnector


class TwitterAdsAnalyticsJob(EtlJobV3):

	def __init__(self, data_level, target_date = None, db_connector = None, file_location=''):
		super().__init__(jobname = __name__, db_connector = db_connector)
		self.file_location = file_location
		self.twitter_ads_api_connector = TwitterAdsApiConnector(file_location=self.file_location)
		self.entities_endpoint = "active_entities"
		self.analytics_endpoint = "sync_report"
		self.granularity = "DAY"  # DAY, HOUR, TOTAL
		self.start_time = target_date

		self.request_data = {
			"entity": "LINE_ITEM",  #  LINE_ITEM or PROMOTED_TWEET. But also ACCOUNT, CAMPAIGN, FUNDING_INSTRUMENT, ORGANIC_TWEET, PROMOTED_ACCOUNT, MEDIA_CREATIVE
			"start_time": self.start_time,  # YYYY-MM-DD. Must be expressed in whole hour?
			"end_time": "2017-05-26",  # Must be expressed in whole hour?
			"granularity": self.granularity,
			"placement": "ALL_ON_TWITTER",
			"metric_groups": "ENGAGEMENT"
		}
		self.entity_records = None
		self.records = None

		self.loggerv3.alert = False


	def request_entities(self):
		self.loggerv3.info('Requesting entities')
		response = self.twitter_ads_api_connector.make_request(self.entities_endpoint, {})
		self.loggerv3.info(f'Response code: {response["code"]}')
		if response['code'] > 202:
			self.loggerv3.alert(f'Code {response["code"]}: {response["message"]}')
			raise ResponseCodeException(response)
		self.entity_records = response['data']
		# TODO: Parse entities and store as self.request_data["entity_ids"] = [<ENTITY IDS>]
    	# TODO: Add batching logic for entity ids > 20


	def make_request(self):
		self.loggerv3.info('Requesting data')
		response = self.twitter_ads_api_connector.make_request(self.analytics_endpoint , self.request_data)
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
		self.loggerv3.info(f"Running Twitter Ads Analytics Job on {self.target_date}")
		self.request_entities()
		self.make_request()

		self.loggerv3.success("All Processing Complete!")
