import os
from base.etl_jobv3 import EtlJobV3
from base.exceptions import ResponseCodeException
from time import sleep
from utils.components.dater import Dater
from utils.components.extract_from_zip import ExtractFromZip
from utils.connectors.s3_api_connector import S3ApiConnector
from utils.connectors.braze_api_connector import BrazeApiConnector
from utils.components.sql_helper import SqlHelper


class BrazeAnonymousUserBackfillJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, file_location=''):
		super().__init__(jobname = __name__, db_connector = db_connector, local_mode=True)
		self.sql_helper = SqlHelper()
		self.dater = Dater()
		self.file_location = file_location
		self.braze_api_connector = BrazeApiConnector(file_location=self.file_location)
		self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket='rt-braze-current', profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
		self.downloads_directory = self.file_location + 'downloads/dsp'
		self.extract_directory = self.file_location + 'extract'
		self.request_data = {
			'segment_id': 'f5da6610-8913-4008-9fee-5f6a73e25e69',
			'fields_to_export': ['external_id', 'email', 'user_aliases']
		}
		self.export_endpoint = 'export_segment'
		self.s3_object_prefix = None
		self.prefix = None
		self.s3_objects = None
		self.records = []
		self.filtered_records = []
		self.profile_matches = []
		self.identified_users = []
		self.batched_matched_profiles = []
		self.import_endpoint = 'import_users_identify'
		self.SLEEP_TIME = 300


	def request_s3_object_prefix(self):
		self.loggerv3.info('Requesting s3 object prefix')
		response = self.braze_api_connector.make_request(self.export_endpoint, self.request_data)
		self.loggerv3.info(f'Response code: {response["code"]}')
		if response['code'] > 202:
			raise ResponseCodeException(response)

		self.s3_object_prefix = response['data']['object_prefix']
		self.loggerv3.info(f'Object prefix: {self.s3_object_prefix}')


	def retrieve_object_list_from_s3(self):
		self.loggerv3.info(f"Pausing for {self.SLEEP_TIME} seconds to allow for S3 delay...")
		sleep(self.SLEEP_TIME)
		self.loggerv3.info("Retrieving object list from s3")
		today = self.dater.format_date(self.dater.get_today())
		self.prefix = f'segment-export/{self.request_data["segment_id"]}/{today}/{self.s3_object_prefix}/'
		self.s3_objects = self.s3_api_connector.get_object_list_from_bucket(self.prefix)


	def clear_downloads_directory(self):
		self.loggerv3.info('Clearing downloads directory')
		if len(os.listdir(f'{self.downloads_directory}/')) > 1:
			os.system(f'rm {self.downloads_directory}/*.*')


	def extract_files_from_s3(self):
		self.loggerv3.info('Extracting zip files from s3')
		for key in self.s3_objects:
			key_no_prefix = key.replace(self.prefix, '')
			filename = '/'.join([self.downloads_directory, key_no_prefix])
			self.s3_api_connector.download_files_from_object(key, filename)


	def populate_records_from_extracted_files(self):
		self.loggerv3.info('Extracting files from zips')
		self.records = []
		number_of_files = len([name for name in os.listdir(self.downloads_directory)])
		for idx, zipfile in enumerate(os.listdir(self.downloads_directory)):
			self.loggerv3.inline_info(f'Extracting {idx+1} of {number_of_files}')
			filename = '/'.join([self.downloads_directory, zipfile])
			extractor = ExtractFromZip(filename, self.extract_directory)
			self.records.extend(extractor.execute())


	def filter_records_with_necessary_attributes(self):
		self.loggerv3.info('Filtering records')
		self.filtered_records = []
		for record in self.records:
			if 'email' in record and 'user_aliases' in record and 'external_id' not in record:
				self.filtered_records.append(record)


	def find_record_matches_by_email(self):
		self.loggerv3.info('Finding RT profile matches')
		filtered_emails = []
		for record in self.filtered_records:
			filtered_emails.append(record['email'].replace("'", "\\'"))

		where_in = self.sql_helper.array_to_sql_list(filtered_emails)
		if not where_in:
			self.loggerv3.info('No emails in filtered records')
			return

		results = self.db_connector.query_v2_db(f"""SELECT u.uuid, u.email
												FROM production.users u
												WHERE u.email IN ({where_in});
											""")

		for result in results:
			self.profile_matches.append({
					'external_id': result[0],
					'email': result[1]
				})


	def build_identified_users_output(self):
		self.loggerv3.info('Building output object')
		for record in self.filtered_records:
			for profile in self.profile_matches:
				if record['email'].lower() == profile['email'].lower():
					self.identified_users.append({
						'external_id': profile['external_id'],
						'user_alias': record['user_aliases'][0]
					})


	def create_profile_batches(self):
		self.loggerv3.info('Creating profile batches')
		number_per_batch = 50
		sub_counter = 0
		batch_counter = 0
		for item in self.identified_users:
			if sub_counter == 0:
				self.batched_matched_profiles.append([])

			self.batched_matched_profiles[batch_counter].append(item)
			sub_counter += 1

			if sub_counter == number_per_batch:
				sub_counter = 0
				batch_counter += 1


	def import_to_braze(self):
		self.loggerv3.info('Import into Braze')
		for batch in self.batched_matched_profiles:
			import_data = {
				'aliases_to_identify': batch
			}
			response = self.braze_api_connector.make_request(self.import_endpoint, import_data)
			self.loggerv3.info(response)
			if response['code'] > 202:
				raise ResponseCodeException(response)


	def execute(self):
		self.loggerv3.info(f"Running Shopify Anonymous User Backfill Job")
		self.request_s3_object_prefix()
		self.retrieve_object_list_from_s3()
		self.clear_downloads_directory()
		self.extract_files_from_s3()
		self.populate_records_from_extracted_files()
		self.filter_records_with_necessary_attributes()
		self.find_record_matches_by_email()
		self.build_identified_users_output()
		self.create_profile_batches()
		self.import_to_braze()
		self.clear_downloads_directory()
		self.loggerv3.success("All Processing Complete!")
