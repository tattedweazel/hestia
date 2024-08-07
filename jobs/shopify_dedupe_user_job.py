import json
import os
import pandas as pd
from base.etl_jobv3 import EtlJobV3
from base.exceptions import ResponseCodeException, MissingBrazeRecordsException, CustomEventMergeException
from datetime import datetime
from time import sleep
from utils.components.dater import Dater
from utils.connectors.s3_api_connector import S3ApiConnector
from utils.connectors.braze_api_connector import BrazeApiConnector
from utils.components.extract_from_zip import ExtractFromZip


class ShopifyDedupeUserJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location=''):
		super().__init__(jobname = __name__, db_connector = db_connector)
		self.dater = Dater()
		self.braze_api_connector = BrazeApiConnector(file_location=file_location)
		self.s3_api_connector = S3ApiConnector(file_location=file_location, bucket='rt-braze-current', profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
		self.downloads_directory = 'downloads/'
		self.extract_directory = 'extract'
		self.request_data = {
			'segment_id': '540b25a7-7033-4c66-9eaa-4b1fc1c4ce03',
			'fields_to_export': ['external_id', 'email', 'apps', 'total_revenue', 'custom_attributes', 'custom_events', 'purchases']
		}
		self.df_json_file = 'batch_df.json'
		self.export_endpoint = 'export_segment'
		self.s3_object_prefix = None
		self.s3_objects = None
		self.BRAZE_S3_ACCESS_SLEEP_TIME = 200
		self.prefix = None
		self.records = None  # User records from each file from exported zip
		self.duplicate_records_df = None  # Pandas DF. Primary keys are email and external ID.
		self.dupe_records_df = None  # Pandas DF, joining other DFs. Primary keys are email and external ID.
		self.dupe_records = None  # List of dicts, each dict is a user with non unique emails
		self.clustered_records = None  # Dict. Keys are emails, values are lists of user profiles (users and attributes)
		self.rooster_teeth_user_ids = None
		self.records_to_update = None
		self.records_to_remove = None
		self.today = self.dater.format_date(self.dater.get_today())
		self.attribute_updates = None
		self.event_updates = None
		self.purchase_updates = None
		self.app_id = self.braze_api_connector.app_id
		self.attribute_batches = None
		self.event_batches = None
		self.purchase_batches = None
		self.batch_records_to_remove = None
		self.import_endpoint = 'update_users'
		self.delete_endpoint = 'delete_users'
		self.UPDATE_USER_BATCH_SIZE = 75
		self.REMOVE_USER_BATCH_SIZE = 50
		self.RATE_LIMIT_THRESHOLD = 45000
		self.RATE_LIMIT_SLEEP_TIME = 70


	def request_s3_object_prefix(self):
		self.loggerv3.info('Requesting s3 object prefix')
		response = self.braze_api_connector.make_request(self.export_endpoint, self.request_data)
		self.loggerv3.info(f'Response code: {response["code"]}')
		if response['code'] > 202:
			raise ResponseCodeException(response)

		self.s3_object_prefix = response['data']['object_prefix']
		self.loggerv3.info(f'Object prefix: {self.s3_object_prefix}')


	def retrieve_object_list_from_s3(self):
		self.loggerv3.info(f"Pausing for {self.BRAZE_S3_ACCESS_SLEEP_TIME} seconds to allow for S3 delay...")
		sleep(self.BRAZE_S3_ACCESS_SLEEP_TIME)
		self.loggerv3.info("Retrieving object list from s3")
		self.prefix = f'segment-export/{self.request_data["segment_id"]}/{self.today}/{self.s3_object_prefix}/'
		self.s3_objects = self.s3_api_connector.get_object_list_from_bucket(self.prefix)


	def clear_downloads_directory(self):
		self.loggerv3.info('Clearing downloads directory')
		if len(os.listdir(f'{self.downloads_directory}/')) > 1:
			os.system(f'rm {self.downloads_directory}/*.*')


	def extract_files_from_s3(self):
		self.loggerv3.info('Extracting zip files from s3')
		for key in self.s3_objects:
			key_no_prefix = key.replace(self.prefix, '')
			filename = ''.join([self.downloads_directory, key_no_prefix])
			self.s3_api_connector.download_files_from_object(key, filename)


	def populate_records_from_extracted_files(self):
		self.loggerv3.info('Extracting files from zips')
		self.records = []
		number_of_files = len([name for name in os.listdir(self.downloads_directory)])
		for idx, zipfile in enumerate(os.listdir(self.downloads_directory)):
			self.loggerv3.inline_info(f'Extracting {idx+1} of {number_of_files}')
			data_file_name = ''.join([self.downloads_directory, zipfile])
			extractor = ExtractFromZip(data_file_name, self.extract_directory)
			self.records.extend(extractor.execute())


	def filter_records_to_duplicate_users(self):
		self.loggerv3.info('Finding duplicate users')
		records_df = pd.DataFrame(self.records, index=None)
		grouped_records_df = records_df.groupby(['email']).size().reset_index(name='count')
		duplicate_records_df = grouped_records_df[grouped_records_df['count'] > 1]
		duplicates = duplicate_records_df['count'].sum() - len(duplicate_records_df.index)
		self.loggerv3.info(f'There are {duplicates} duplicates')
		self.duplicate_records_df = records_df[records_df['email'].isin(duplicate_records_df['email'])]


	def write_duplicate_records_df_to_json(self):
		self.loggerv3.info('Writing duplicate records to json file for posterity')
		self.duplicate_records_df.to_json(self.df_json_file)


	def read_duplicate_records_df_from_json(self):
		self.loggerv3.info('Reading duplicate records from json file')
		self.dupe_records_df = pd.read_json(self.df_json_file)
		self.dupe_records = self.dupe_records_df.to_dict('records')


	def extract_last_used_attribute(self):
		self.loggerv3.info('Extracting last used attribute')
		for record in self.dupe_records:
			record['last_used'] = None
			if record['apps'] is not None:
				for app in record['apps']:
					if app['last_used'] is not None:
						last_used = datetime.strptime(app['last_used'], '%Y-%m-%dT%H:%M:%S.%fZ')
						if record['last_used'] is None:
							record['last_used'] = last_used
						elif last_used > record['last_used']:
							record['last_used'] = last_used


	def determine_max_last_used(self):
		self.loggerv3.info('Determining max last used')
		last_used_map = {}
		for record in self.dupe_records:
			email = record['email']
			records_last_used = record['last_used']
			if records_last_used is None:
				continue
			elif email in last_used_map:
				if records_last_used > last_used_map[email]:
					last_used_map[email] = records_last_used
			else:
				last_used_map[email] = records_last_used

		for record in self.dupe_records:
			record['max_last_used'] = None
			email = record['email']
			if email in last_used_map and last_used_map[email] == record['last_used']:
				record['max_last_used'] = True


	def get_rooster_teeth_user_ids(self):
		self.loggerv3.info('Getting rooster teeth user IDs')
		results = self.db_connector.query_v2_db(f"""SELECT uuid, email
													FROM production.users;
												""")
		self.rooster_teeth_user_ids = {}
		for result in results:
			self.rooster_teeth_user_ids[result[0]] = result[1].lower()


	def append_rooster_teeth_ids(self):
		self.loggerv3.info('Appending rooster teeth user IDs to records')
		for record in self.dupe_records:
			record['rooster_teeth_id'] = None
			external_id = record['external_id']
			if external_id in self.rooster_teeth_user_ids:
				record['rooster_teeth_id'] = external_id


	def extract_custom_attribute_count(self):
		self.loggerv3.info('Extracting custom attribute count')
		for record in self.dupe_records:
			custom_attribute_count = len(record['custom_attributes']) if record['custom_attributes'] is not None else None
			record['custom_attribute_count'] = custom_attribute_count


	def determine_max_custom_attributes(self):
		self.loggerv3.info('Determining max custom attributes')
		custom_attribute_count_map = {}
		for record in self.dupe_records:
			email = record['email']
			custom_attribute_count = record['custom_attribute_count']
			if custom_attribute_count is None:
				continue
			elif email in custom_attribute_count_map:
				if custom_attribute_count > custom_attribute_count_map[email]:
					custom_attribute_count_map[email] = custom_attribute_count
			else:
				custom_attribute_count_map[email] = custom_attribute_count

		for record in self.dupe_records:
			record['max_custom_attributes'] = None
			email = record['email']
			if email in custom_attribute_count_map and custom_attribute_count_map[email] == record['custom_attribute_count']:
				record['max_custom_attributes'] = True


	def extract_custom_events_into_dict(self):
		self.loggerv3.info('Extracting custom events into dict')
		for record in self.dupe_records:
			record['custom_events_dict'] = {}
			if record['custom_events'] is not None:
				for custom_event in record['custom_events']:
					record['custom_events_dict'][custom_event['name']] = custom_event


	def select_records_to_update(self):
		self.loggerv3.info('Selecting records to update')
		self.records_to_update = {}

		for record in self.dupe_records:
			if record['rooster_teeth_id'] is not None and record['max_last_used'] is not None:
				self.records_to_update[record['email']] = record

		for record in self.dupe_records:
			if record['email'] not in self.records_to_update and record['rooster_teeth_id'] is not None and record['max_custom_attributes'] is not None:
				self.records_to_update[record['email']] = record

		for record in self.dupe_records:
			if record['email'] not in self.records_to_update and record['rooster_teeth_id'] is not None:
				self.records_to_update[record['email']] = record

		for record in self.dupe_records:
			if record['email'] not in self.records_to_update and record['rooster_teeth_id'] is None and record['max_last_used'] is not None:
				self.records_to_update[record['email']] = record

		for record in self.dupe_records:
			if record['email'] not in self.records_to_update and record['rooster_teeth_id'] is None and record['max_last_used'] is None and record['max_custom_attributes'] is not None:
				self.records_to_update[record['email']] = record

		for record in self.dupe_records:
			if record['email'] not in self.records_to_update:
				self.records_to_update[record['email']] = record


	def check_for_missing_records_to_update(self):
		self.loggerv3.info('Checking for missing records')
		for record in self.dupe_records:
			if record['email'] not in self.records_to_update:
				raise MissingBrazeRecordsException()
		return True


	def select_records_to_remove(self):
		self.loggerv3.info('Selecting records to remove')
		self.records_to_remove = []
		for record in self.dupe_records:
			record_to_update = self.records_to_update[record['email']]
			if record['external_id'] != record_to_update['external_id']:
				self.records_to_remove.append(record)


	def append_new_custom_attributes(self):
		self.loggerv3.info('Appending new custom attributes')
		for record in self.records_to_remove:
			record_to_update = self.records_to_update[record['email']]
			if 'new_custom_attributes' not in record_to_update:
				record_to_update['new_custom_attributes'] = {}
			existing_custom_attributes = record_to_update['custom_attributes'] if record_to_update['custom_attributes'] is not None else {}
			if record['custom_attributes'] is not None:
				for key, value in record['custom_attributes'].items():
					if key not in existing_custom_attributes:
						record_to_update['new_custom_attributes'][key] = value


	def format_braze_date(self, date):
		return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ').isoformat()


	def merge_custom_events(self, event1, event2, merge_type):
		if not isinstance(event1, dict):
			raise TypeError
		if not isinstance(event2, dict):
			raise TypeError
		if event1['name'] != event2['name']:
			raise CustomEventMergeException(event1['name'], event2['name'])

		event1['first'] = self.format_braze_date(event1['first'])
		event2['first'] = self.format_braze_date(event2['first'])
		event1['last'] = self.format_braze_date(event1['last'])
		event2['last'] = self.format_braze_date(event2['last'])
		if merge_type == 'existing':
			return {
				'name': event1['name'],
				'time': min(event1['first'], event2['first']),
				'properties': {
					'last': max(event1['last'], event2['last']),
					'count': event1['count'] + event2['count']
				}
			}
		elif merge_type == 'new':
			return {
				'name': event1['name'],
				'time': min(event1['first'], event2['first']),
				'properties': {
					'last': max(event1['last'], event2['last']),
					'count': event2['count']
				}
			}


	def format_custom_event(self, event):
		return {
			'name': event['name'],
			'time': event['first'],
			'properties': {
				'last': event['last'],
				'count': event['count']
			}
		}


	def append_update_new_custom_event(self):
		"""
		If the custom event of a to-be-deleted record is already in the to-be-updated custom events, then we merge them. AkA Existing Merge
		However, if the custom event of a to-be-deleted record is also a custom event in the to-be-updated record, then we merge those. AKA New merge
		Else just add the custom event of the to-be-deleted record to the to-be-updated custom events.
		"""
		self.loggerv3.info('Appending new custom events')
		for record in self.records_to_remove:
			record_to_update = self.records_to_update[record['email']]
			if 'new_custom_events_dict' not in record_to_update:
				record_to_update['new_custom_events_dict'] = {}
			if len(record['custom_events_dict']) > 0:
				for event_name, event in record['custom_events_dict'].items():
					if event_name in record_to_update['new_custom_events_dict']:
						primary_event = record_to_update['new_custom_events_dict'][event_name]
						updated_event = self.merge_custom_events(primary_event, event, merge_type='existing')
						record_to_update['new_custom_events_dict'][event_name] = updated_event
					elif event_name in record_to_update['custom_events_dict']:
						primary_event = record_to_update['custom_events_dict'][event_name]
						updated_event = self.merge_custom_events(primary_event, event, merge_type='new')
						record_to_update['new_custom_events_dict'][event_name] = updated_event
					else:
						record_to_update['new_custom_events_dict'][event_name] = self.format_custom_event(event)


	def format_purchase(self, purchase):
		return {
			'product_id': purchase['name'],
			'app_id': self.app_id,
			'price': 0.0,
			'currency': 'USD',
			'time': self.format_braze_date(purchase['first']),
			'properties': {
				'last': self.format_braze_date(purchase['last']),
				'count': purchase['count']
			}
		}


	def append_new_purchases(self):
		self.loggerv3.info('Appending new purchases')
		for record in self.records_to_remove:
			record_to_update = self.records_to_update[record['email']]
			if 'new_purchases' not in record_to_update:
				record_to_update['new_purchases'] = []
			if record['purchases'] is not None:
				for purchase in record['purchases']:
					new_purchase = self.format_purchase(purchase)
					record_to_update['new_purchases'].append(new_purchase)


	def update_total_revenue(self):
		self.loggerv3.info('Updating total revenue')
		for record in self.records_to_remove:
			record_to_update = self.records_to_update[record['email']]
			if 'new_total_revenue' not in record_to_update:
				record_to_update['new_total_revenue'] = record_to_update['total_revenue']
			if record['total_revenue'] > 0.0:
				record_to_update['new_total_revenue'] += record['total_revenue']


	def send_updated_records_to_s3(self):
		self.loggerv3.info('Sending updated records to s3')
		records_to_update_json = json.dumps(self.records_to_update, default=str)
		now = datetime.now()
		key = f'segment-processed/{self.request_data["segment_id"]}/{self.today}/updated_records/{now}'
		response = self.s3_api_connector.put_object(file_object=records_to_update_json, key=key)
		if response['ResponseMetadata']['HTTPStatusCode'] != 200:
			raise ResponseCodeException(response)


	def send_removed_records_to_s3(self):
		self.loggerv3.info('Sending removed records to s3')
		records_to_remove_json = json.dumps(self.records_to_remove, default=str)
		now = datetime.now()
		key = f'segment-processed/{self.request_data["segment_id"]}/{self.today}/removed_records/{now}'
		response = self.s3_api_connector.put_object(file_object=records_to_remove_json, key=key)
		if response['ResponseMetadata']['HTTPStatusCode'] != 200:
			raise ResponseCodeException(response)


	def build_attributes_output_data_structure(self):
		self.loggerv3.info('Building attributes output data structure')
		self.attribute_updates = []
		for email, record in self.records_to_update.items():
			attribute_profile = {'external_id': record['external_id']}
			if len(record['new_custom_attributes']) > 0:
				for key, value in record['new_custom_attributes'].items():
					attribute_profile[key] = value
			if record['new_total_revenue'] > 0.0:
				attribute_profile['total_revenue'] = record['new_total_revenue']
			if len(attribute_profile) > 1:
				self.attribute_updates.append(attribute_profile)


	def build_events_output_data_structure(self):
		self.loggerv3.info('Building events output data structure')
		self.event_updates = []
		event_update_holder = []
		for email, record in self.records_to_update.items():
			if len(record['new_custom_events_dict']) > 0:
				for event_name, event_dict in record['new_custom_events_dict'].items():
					event_profile = {'external_id': record['external_id']}
					for key, value in event_dict.items():
						event_profile[key] = value
					event_update_holder.append(event_profile)

		for update in event_update_holder:
			for i in range(update['properties']['count']):
				self.event_updates.append(update)


	def build_purchases_output_data_structure(self):
		self.loggerv3.info('Building purchases output data structure')
		self.purchase_updates = []
		for email, record in self.records_to_update.items():
			if len(record['new_purchases']) > 0:
				for new_purchase in record['new_purchases']:
					purchase_profile = {'external_id': record['external_id']}
					for key, value in new_purchase.items():
						purchase_profile[key] = value
					self.purchase_updates.append(purchase_profile)


	def create_update_batches(self, update_list):
		batches = []
		sub_counter = 0
		batch_counter = 0
		for item in update_list:
			number_of_updates = len(item)
			if sub_counter == 0:
				batches.append([])

			if (sub_counter + number_of_updates) <= self.UPDATE_USER_BATCH_SIZE:
				batches[batch_counter].append(item)
				sub_counter += number_of_updates
			else:
				sub_counter = number_of_updates
				batch_counter += 1
				batches.append([])
				batches[batch_counter].append(item)

		return batches


	def create_attribute_batches(self):
		self.loggerv3.info('Creating attribute batches')
		self.attribute_batches = self.create_update_batches(self.attribute_updates)


	def create_event_batches(self):
		self.loggerv3.info('Creating event batches')
		self.event_batches = self.create_update_batches(self.event_updates)


	def create_purchase_batches(self):
		self.loggerv3.info('Creating purchase batches')
		self.purchase_batches = self.create_update_batches(self.purchase_updates)


	def create_remove_batches(self):
		self.loggerv3.info('Creating records to remove batches')
		self.batch_records_to_remove = []
		sub_counter = 0
		batch_counter = 0
		for item in self.records_to_remove:
			if sub_counter == 0:
				self.batch_records_to_remove.append([])

			self.batch_records_to_remove[batch_counter].append(item['external_id'])
			sub_counter += 1

			if sub_counter == self.REMOVE_USER_BATCH_SIZE:
				sub_counter = 0
				batch_counter += 1


	def import_updates_to_braze(self, idx, import_data):
		if idx % self.RATE_LIMIT_THRESHOLD == 0:
			sleep(self.RATE_LIMIT_SLEEP_TIME)
		response = self.braze_api_connector.make_request(self.import_endpoint, import_data)
		if response['code'] <= 202:
			self.loggerv3.info(response)
		if response['code'] > 202:
			raise ResponseCodeException(response)


	def import_attribute_updates_to_braze(self):
		self.loggerv3.info('Importing attribute updates into Braze')
		for idx, batch in enumerate(self.attribute_batches):
			import_data = {
				'attributes': batch
			}
			self.import_updates_to_braze(idx, import_data)


	def import_event_updates_to_braze(self):
		self.loggerv3.info('Importing event updates into Braze')
		for idx, batch in enumerate(self.event_batches):
			import_data = {
				'events': batch
			}
			self.import_updates_to_braze(idx, import_data)


	def import_purchase_updates_to_braze(self):
		self.loggerv3.info('Importing purchase updates into Braze')
		for idx, batch in enumerate(self.purchase_batches):
			import_data = {
				'purchases': batch
			}
			self.import_updates_to_braze(idx, import_data)


	def delete_dupe_users_in_braze(self):
		self.loggerv3.info('Deleting dupe users in Braze')
		for idx, batch in enumerate(self.batch_records_to_remove):
			import_data = {
				'external_ids': batch
			}
			if idx % self.RATE_LIMIT_THRESHOLD == 0:
				sleep(self.RATE_LIMIT_SLEEP_TIME)
			response = self.braze_api_connector.make_request(self.delete_endpoint, import_data)
			if response['code'] <= 202:
				self.loggerv3.info(response)
			if response['code'] > 202:
				raise ResponseCodeException(response)


	def clean_up(self):
		if len(os.listdir(f'{self.downloads_directory}/')) > 1:
			os.system(f'rm {self.downloads_directory}/*.*')
		os.system(f'rm batch_df.json')


	def execute(self):
		self.loggerv3.info(f"Running Shopify Dedupe User Job")
		# Extract
		self.request_s3_object_prefix()
		self.retrieve_object_list_from_s3()
		self.clear_downloads_directory()
		self.extract_files_from_s3()
		self.populate_records_from_extracted_files()
		self.filter_records_to_duplicate_users()
		self.write_duplicate_records_df_to_json()
		# Transform
		self.read_duplicate_records_df_from_json()
		self.extract_last_used_attribute()
		self.determine_max_last_used()
		self.get_rooster_teeth_user_ids()
		self.append_rooster_teeth_ids()
		self.extract_custom_attribute_count()
		self.determine_max_custom_attributes()
		self.extract_custom_events_into_dict()
		self.select_records_to_update()
		self.check_for_missing_records_to_update()
		self.select_records_to_remove()
		self.append_new_custom_attributes()
		self.append_update_new_custom_event()
		self.append_new_purchases()
		self.update_total_revenue()
		self.send_updated_records_to_s3()
		self.send_removed_records_to_s3()
		self.build_attributes_output_data_structure()
		self.build_events_output_data_structure()
		self.build_purchases_output_data_structure()
		self.create_attribute_batches()
		self.create_event_batches()
		self.create_purchase_batches()
		self.create_remove_batches()
		# Load
		self.import_attribute_updates_to_braze()
		self.import_event_updates_to_braze()
		self.import_purchase_updates_to_braze()
		self.delete_dupe_users_in_braze()

		self.clean_up()
		self.loggerv3.success("All Processing Complete!")
