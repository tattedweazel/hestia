import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.components.batcher import Batcher
from utils.components.byte_helper import byte_helper


class StaffCreatedPostsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'staff_created_posts')

		self.target_dto = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.next_dto = self.target_dto + timedelta(1)
		self.next_date = datetime.strftime(self.next_dto, '%Y-%m-%d')
		self.target_dto = self.target_dto + timedelta(hours=7)
		self.batcher = Batcher()
		self.batches = None
		self.created_posts = []
		self.usernames = []
		self.created_posts_df = None
		self.usernames_df = None
		self.final_dataframe = None


	def load_created_posts(self):
		self.loggerv3.info("Loading Post Data")
		query = f""" 
		SELECT
			id as post_uuid,
       		author_id as creator_uuid,
       		created_at as date_created
		FROM posts
		WHERE owner_type = 'Community'
			AND created_at >= '{self.target_date}'
			AND created_at < '{self.next_date}';
		"""
		results = self.db_connector.query_community_db_connection(query)
		for result in results:
			record = {
				"post_uuid" : byte_helper(result[0]),
				"creator_uuid" : byte_helper(result[1]),
				"date_created" : result[2]
			}
			self.created_posts.append(record)


	def get_usernames(self):
		self.loggerv3.info('Getting Usernames')
		target_user_uuids = []
		for cc in self.created_posts:
			if cc['creator_uuid'] is not None:
				target_user_uuids.append(f"'{cc['creator_uuid']}'")

		if len(target_user_uuids) > 0:
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
		else:
			return


	def create_final_dataframe(self):
		self.loggerv3.info('Creating Final Dataframe')
		if len(self.created_posts) > 0 and len(self.usernames) > 0:
			# Create DFs
			self.created_posts_df = pd.DataFrame(self.created_posts)
			self.usernames_df = pd.DataFrame(self.usernames)
			# Drop Dupes
			self.usernames_df.drop_duplicates(inplace=True)
			# Merge DFs
			self.final_dataframe = self.created_posts_df.merge(self.usernames_df, how='left', on='creator_uuid')
		else:
			return


	def write_to_redshift(self):
		if self.final_dataframe is None:
			return
		self.loggerv3.info("Writing results to Redshift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Staff Created Posts Job for {self.target_date}")
		self.load_created_posts()
		self.get_usernames()
		self.create_final_dataframe()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
