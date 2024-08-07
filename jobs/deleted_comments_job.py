import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.components.byte_helper import byte_helper


class DeletedCommentsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'deleted_comments')

		self.target_dto = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.next_dto = self.target_dto + timedelta(1)
		self.next_date = datetime.strftime(self.next_dto, '%Y-%m-%d')
		self.target_dto = self.target_dto + timedelta(hours=7)
		self.deleted_comments = []
		self.full_records = []
		self.final_df = None


	def load_deleted_comments(self):
		# topic_type mapping:
		# 0 - Episode
		# 1 - Community
		# 2 - Bonus Feature
		# 3 - Movie (this is currently never used)

		self.loggerv3.info("Loading Comment Data...")
		query = f""" 
					SELECT
					    uuid,
					    deleted_at as date_deleted,
					    deleted_by,
					    owner_uuid as created_by,
					    created_at,
					    message as comment_text,
					    topic_uuid,
					    parent_uuid,
					    case
					    	when topic_type = 0 then 'Episode'
					    	when topic_type = 1 then 'Community Post'
					    	when topic_type = 2 then 'Bonus Feature'
					    	when topic_type = 3 then 'Movie'
					    	else 'Unknown'
					    end as topic_type,
					    admin_removed
					FROM comments
					WHERE
					    deleted_at >= '{self.target_date}' AND
					    deleted_at < '{self.next_date}';
		"""
		results = self.db_connector.query_comments_db_connection(query)
		for result in results:
			record = {
				"uuid" : byte_helper(result[0]),
				"date_deleted" : result[1],
				"deleted_by" : byte_helper(result[2]),
				"created_by" : byte_helper(result[3]),
				"created_at" : result[4],
				"comment_text" : byte_helper(result[5]),
				"topic_uuid" : byte_helper(result[6]),
				"parent_uuid" : byte_helper(result[7]),
				"topic_type" : result[8],
				"admin_removed" : result[9]
			}
			self.deleted_comments.append(record)


	def populate_missing_fields(self):
		for comment in self.deleted_comments:
			episode_title, series_title, channel_title = self.get_episode_info(comment['topic_uuid'], comment['topic_type'])
			record = {
					"comment_uuid": 		comment['uuid'],
					"creator_uuid": 		comment['created_by'],
					"creator_username": 	self.get_username(comment['created_by']),
					"date_created":			comment['created_at'],
					"deleter_uuid":			comment['deleted_by'],
					"deleter_username":		self.get_username(comment['deleted_by']),
					"date_deleted": 		comment['date_deleted'],
					"comment_text": 		comment['comment_text'][0:100],
					"comment_type":			comment['topic_type'],
					"episode_title": 		episode_title,
					"series_title": 		series_title,
					"channel_title": 		channel_title,
					"removed_by_admin":		comment['admin_removed'],
					"flags":				self.get_flags(comment['uuid'])
				}
			self.full_records.append(record)


	def get_episode_info(self, topic_uuid, topic_type):
		if topic_type.lower() == 'community post' or topic_type.lower() == 'unknown':
			return None, None, None

		results = self.db_connector.read_redshift(f""" 
			SELECT
				episode_title,
				series_title,
				channel_title
			FROM warehouse.dim_segment_episode
			WHERE episode_key = '{topic_uuid}'
		""")

		for result in results:
			return result[0], result[1], result[2]

		return None, None, None



	def get_username(self, user_uuid):
		if user_uuid is None:
			return None

		results = self.db_connector.query_v2_db(f"SELECT username FROM users WHERE uuid = '{user_uuid}'")
		for result in results:
			return result[0]

		return None


	def get_flags(self, comment_uuid):
		flags = []
		results = self.db_connector.query_comments_db_connection(f"SELECT flag FROM flags WHERE comment_uuid = '{comment_uuid}'")
		for result in results:
			if result[0] not in flags:
				flags.append(result[0])
		if len(flags) > 0:
			return ', '.join(flags)
		return None


	def create_final_dataframe(self):
		self.final_df = pd.DataFrame(self.full_records)


	def write_to_red_shift(self):
		self.loggerv3.info("Writing to Red Shift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Deleted Comments Job for {self.target_date}")
		self.load_deleted_comments()
		self.populate_missing_fields()
		self.create_final_dataframe()
		self.write_to_red_shift()
		self.loggerv3.success("All Processing Complete!")
