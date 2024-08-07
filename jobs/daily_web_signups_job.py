import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.components.batcher import Batcher


class DailyWebSignupsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'daily_web_signups')
		self.start_date = target_date
		self.start_date_dt = datetime.strptime(target_date, '%Y-%m-%d')
		self.end_date_dt = self.start_date_dt + timedelta(days=1)
		self.end_date = self.end_date_dt.strftime('%Y-%m-%d')
		self.max_date_dt = self.start_date_dt - timedelta(days=1)
		self.max_date = self.max_date_dt.strftime('%Y-%m-%d')
		self.batcher = Batcher()
		self.signups = []
		self.existing_user_keys = []
		self.filtered_signups = []
		self.remaining_signups = []
		self.final_dataframe = None


	def load_signups(self):
		self.loggerv3.info("Loading Signups")
		query = f"""
		SELECT
			user_id,
       		option_selected,
       		'signup_flow' as location
		FROM warehouse.signup_flow_event
		WHERE
			state = 'exited'
			AND step = 1
			AND option_selected is NOT NULL
			AND event_timestamp >= '{self.start_date}'
			AND event_timestamp < '{self.end_date}'
			AND option_selected in ('email', 'google', 'apple', 'facebook')
		GROUP BY 1, 2, 3;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.signups.append({
				"user_id": result[0],
				"option_selected": result[1],
				"location": result[2]
			})


	def load_existing_user_keys(self):
		self.loggerv3.info('Loading existing user keys')
		user_ids = [signup['user_id'] for signup in self.signups]

		self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=user_ids)
		for batch in self.batches:
			target_keys = ','.join([f"'{x}'" for x in batch])
			query = f"""
			SELECT uuid
			FROM users
			WHERE created_at < '{self.max_date}' and uuid in ({target_keys});
			"""
			results = self.db_connector.query_v2_db(query)
			for result in results:
				self.existing_user_keys.append(result[0])


	def update_signups_based_on_existing_users(self):
		self.loggerv3.info('Updating signups')
		for signup in self.signups:
			if signup['user_id'] not in self.existing_user_keys:
				self.filtered_signups.append(signup)


	def get_remaining_web_signups(self):
		self.loggerv3.info('Getting remaining web signups')
		query = f"""
		SELECT
			user_id,
			option_selected,
			'chat' as location
		FROM warehouse.chat_signup_complete
		WHERE
			platform = 'web'
			AND event_timestamp >= '{self.start_date}' and event_timestamp < '{self.end_date}'
		UNION
		SELECT
			user_id,
			option_selected,
			'community' as location
		FROM warehouse.community_signup_complete
		WHERE platform = 'web'
			AND event_timestamp >= '{self.start_date}' and event_timestamp < '{self.end_date}'
		UNION
		SELECT
			user_id,
			option_selected,
			'gate' as location
		FROM warehouse.gate_signup_complete
		WHERE platform = 'web'
			AND event_timestamp >= '{self.start_date}' and event_timestamp < '{self.end_date}'
		UNION
		SELECT
			user_id,
			option_selected,
			'hero' as location
		FROM warehouse.hero_signup_complete
		WHERE platform = 'web'
			AND event_timestamp >= '{self.start_date}' and event_timestamp < '{self.end_date}';
		
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.remaining_signups.append({
				"user_id": result[0],
				"option_selected": result[1],
				"location": result[2]
			})


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		remaining_signups_df = pd.DataFrame(self.remaining_signups)
		filtered_signups_df = pd.DataFrame(self.filtered_signups)

		self.final_dataframe = pd.concat([remaining_signups_df, filtered_signups_df])
		self.final_dataframe = self.final_dataframe.groupby(['option_selected', 'location'])['user_id'].count().reset_index()
		self.final_dataframe['signup_date'] = self.start_date
		self.final_dataframe = self.final_dataframe.rename(columns={'user_id': 'signups'})


	def write_to_red_shift(self):
		self.loggerv3.info("Writing to Red Shift...")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Normalized Daily Web Signups {self.target_date}")
		self.load_signups()
		self.load_existing_user_keys()
		self.update_signups_based_on_existing_users()
		self.get_remaining_web_signups()
		self.build_final_dataframe()
		self.write_to_red_shift()
		self.loggerv3.success("All Processing Complete!")
