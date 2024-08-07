import config
import pandas as pd
from utils.connectors.database_connector import DatabaseConnector
from utils.components.batcher import Batcher
from utils.components.loggerv3 import Loggerv3


class FreeViewersByChannel:

	def __init__(self, target_date, channel):
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.batcher = Batcher()
		self.target_date = target_date
		self.target_date = target_date
		self.channel = channel
		self.user_tier = 'free'
		self.output_directory = config.file_location + 'tools/output'
		self.file_name = '_'.join([self.user_tier, self.channel.lower().strip().replace(' ', '_'), 'viewers.csv'])
		self.viewers = []
		self.user_emails = []
		self.batches = None
		self.current_subs = []
		self.already_subbed = None
		self.final_dataframe = None


	def get_viewers(self):
		self.loggerv3.info('Getting viewers by channel')
		query = f"""
		SELECT
			du.user_id,
			sum(active_seconds) as active_seconds
		FROM warehouse.vod_viewership vv
		INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
		INNER JOIN warehouse.dim_user du on du.user_key = vv.user_key
		WHERE
			dse.channel_title = '{self.channel}'
			AND vv.start_timestamp >= '{self.target_date}'
			AND vv.user_tier = '{self.user_tier}'
		GROUP BY 1
		HAVING sum(vv.active_seconds) > 60;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.viewers.append(result[0])


	def get_viewer_info(self):
		self.loggerv3.info("Getting viewer info")
		target_keys = [f"'{x}'" for x in self.viewers]
		self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=target_keys)
		for batch in self.batches:
			users = ','.join(batch)
			results = self.db_connector.query_v2_db(f"""
			    SELECT
			        uuid,
			        email,
			        username
			    FROM users
			    WHERE uuid in ({users});
			""")
			for result in results:
				self.user_emails.append({
					'uuid': result[0],
					'email': result[1],
					'username': result[2]
				})


	def load_active_subs(self):
		self.loggerv3.info('Loading active subs')
		query = f"""
		SELECT user_uuid
		FROM user_subscriptions
		WHERE state in ('canceled', 'paying', 'payment_pending', 'trialing', 'expired')
		AND user_uuid is not NULL
		AND ends_at > current_date 
		AND started_at <= current_date
		GROUP BY 1
		"""
		results = self.db_connector.query_business_service_db_connection(query)
		for result in results:
			self.current_subs.append(result[0])


	def load_trial_subs(self):
		self.loggerv3.info('Loading active trials')
		query = f""" 
			SELECT concat(user_uuid, original_order_id) as id
			FROM android_subscriptions
			WHERE current_date BETWEEN trial_started_at and trial_ends_at
				AND user_uuid is NOT NULL
			GROUP BY 1

			UNION

			SELECT concat(user_uuid, original_transaction_id) as id
			FROM itunes_subscriptions
			WHERE current_date BETWEEN trial_started_at and trial_ends_at
				AND user_uuid is NOT NULL
			GROUP BY 1

			UNION

			SELECT concat(account_code, uuid) as id
			FROM subscriptions
			WHERE current_date BETWEEN trial_started_at and trial_ends_at
				AND uuid is NOT NULL
			GROUP BY 1

			UNION

			SELECT concat(user_uuid, original_transaction_id) as id
			FROM roku_subscriptions
			WHERE current_date BETWEEN trial_started_at and trial_ends_at
				AND user_uuid is NOT NULL
			GROUP BY 1;
		"""
		results = self.db_connector.query_business_service_db_connection(query)
		for result in results:
			self.current_subs.append(result[0])


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		self.final_users = []
		self.already_subbed = []
		for user in self.user_emails:
			if user['uuid'] not in self.current_subs:
				self.final_users.append(user)
			else:
				self.already_subbed.append(user)


		self.existing_df = pd.read_csv('downloads/dsp/free_achievement_hunter_viewers.csv')
		self.existing = self.existing_df.to_dict('records')
		self.existing_uuids = [e['uuid'] for e in self.existing]

		self.final_users2 = []

		for user in self.final_users:
			if user['uuid'] not in self.existing_uuids:
				self.final_users2.append(user)

		self.final_dataframe = pd.DataFrame(self.final_users2)


	def output(self):
		self.loggerv3.info('Outputting final viewers list')
		full_file_path = '/'.join([self.output_directory, self.file_name])
		self.final_dataframe.to_csv(full_file_path, index=False)


	def execute(self):
		self.get_viewers()
		self.get_viewer_info()
		self.load_active_subs()
		self.load_trial_subs()
		self.build_final_dataframe()
		self.output()
		self.loggerv3.success("All Processing Complete!")



