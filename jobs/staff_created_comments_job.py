import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.components.batcher import Batcher
from utils.components.byte_helper import byte_helper


class StaffCreatedCommentsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'staff_created_comments')

		self.target_dto = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.next_dto = self.target_dto + timedelta(1)
		self.next_date = datetime.strftime(self.next_dto, '%Y-%m-%d')
		self.target_dto = self.target_dto + timedelta(hours=7)
		self.batcher = Batcher()
		self.batches = None
		self.created_comments = []
		self.episode_info = []
		self.usernames = []
		self.created_comments_df = None
		self.episode_info_df = None
		self.usernames_df = None
		self.final_dataframe = None


	def load_created_comments(self):
		# topic_type mapping:
		# 0 - Episode
		# 1 - Community
		# 2 - Bonus Feature
		# 3 - Movie (this is currently never used)

		self.loggerv3.info("Loading Comment Data")
		query = f""" 
		SELECT
			uuid,
			owner_uuid as created_by,
			created_at,
			topic_uuid,
			case
				when topic_type = 0 then 'Episode'
				when topic_type = 1 then 'Community Comment'
				when topic_type = 2 then 'Bonus Feature'
				when topic_type = 3 then 'Movie'
				else 'Unknown'
			end as topic_type
		FROM comments
		WHERE
			created_at >= '{self.target_date}' AND
			created_at < '{self.next_date}' AND
			created_by_staff = 1;
		"""
		results = self.db_connector.query_comments_db_connection(query)
		for result in results:
			record = {
				"comment_uuid" : byte_helper(result[0]),
				"creator_uuid" : byte_helper(result[1]),
				"date_created" : result[2],
				"topic_uuid" : byte_helper(result[3]),
				"comment_type" : result[4]
			}
			self.created_comments.append(record)


	def get_episode_info(self):
		self.loggerv3.info('Getting Episode Info')
		target_episode_keys = []
		for cc in self.created_comments:
			if cc['comment_type'] in ('Episode', 'Bonus Feature') and cc['topic_uuid'] is not None:
				target_episode_keys.append(f"'{cc['topic_uuid']}'")

		self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=target_episode_keys)
		for batch in self.batches:
			episode_keys = ','.join(batch)
			query = f"""
			SELECT
				episode_key,
				episode_title,
				series_title,
				channel_title
			FROM warehouse.dim_segment_episode
			WHERE episode_key in ({episode_keys});
			"""
			results = self.db_connector.read_redshift(query)
			for result in results:
				self.episode_info.append({
					'topic_uuid': result[0],
					'episode_title': result[1],
					'series_title': result[2],
					'channel_title': result[3]
				})


	def get_usernames(self):
		self.loggerv3.info('Getting Usernames')
		target_user_uuids = []
		for cc in self.created_comments:
			if cc['creator_uuid'] is not None:
				target_user_uuids.append(f"'{cc['creator_uuid']}'")

		self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=target_user_uuids)
		for batch in self.batches:
			user_uuids = ','.join(batch)
			query = f"""
			SELECT
				uuid,
				username
			FROM users
			WHERE uuid in ({user_uuids});
			"""
			results = self.db_connector.query_v2_db(query)
			for result in results:
				self.usernames.append({
					'creator_uuid': result[0],
					'creator_username': result[1]
				})


	def create_final_dataframe(self):
		self.loggerv3.info('Creating Final Dataframe')
		# Create DFs
		self.created_comments_df = pd.DataFrame(self.created_comments)
		self.episode_info_df = pd.DataFrame(self.episode_info)
		self.usernames_df = pd.DataFrame(self.usernames)
		# Drop Dupes
		self.episode_info_df.drop_duplicates(inplace=True)
		self.usernames_df.drop_duplicates(inplace=True)
		# Merge DFs
		self.final_dataframe = self.created_comments_df.merge(self.episode_info_df, how='left', on='topic_uuid')
		self.final_dataframe = self.final_dataframe.merge(self.usernames_df, how='left', on='creator_uuid')
		# Drop Columns
		self.final_dataframe.drop('topic_uuid', inplace=True, axis=1)


	def write_to_redshift(self):
		if self.final_dataframe is None:
			return
		self.loggerv3.info("Writing results to Redshift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Staff Created Comments Job for {self.target_date}")
		self.load_created_comments()
		if len(self.created_comments) > 0:
			self.get_episode_info()
			self.get_usernames()
			self.create_final_dataframe()
			self.write_to_redshift()
		else:
			self.loggerv3.info('No staff comments')
		self.loggerv3.success("All Processing Complete!")
