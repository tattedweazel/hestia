import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.components.byte_helper import byte_helper


class CommentsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'comments')

		self.target_dto = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.next_dto = self.target_dto + timedelta(1)
		self.next_date = datetime.strftime(self.next_dto, '%Y-%m-%d')
		self.target_dto = self.target_dto + timedelta(hours=7)
		
		self.comments = {}
		self.episode_ids = []
		self.episodes = {}
		self.final_df = None


	def load_episode_comments(self):
		self.loggerv3.info("Loading Comment Data...")
		query = f""" SELECT
									    topic_uuid as item_uuid,
									    count(distinct owner_uuid) as commentors,
									    count(*) as comments
									FROM
									    comments
									WHERE
									    topic_type != 1 AND
									    created_at >= '{self.target_date}' AND
									    created_at < '{self.next_date}'
									GROUP BY 1
									ORDER BY 2 desc;
		"""
		results = self.db_connector.query_comments_db_connection(query)
		for result in results:
			self.episode_ids.append(byte_helper(result[0]))
			record = {
				"date": self.target_dto,
				"episode_uuid": byte_helper(result[0]),
				"episode_title": None,
				"series_title": None,
				"channel_title": None,
				"commentors": result[1],
				"comments": result[2]
			}
			self.comments[byte_helper(result[0])] = record


	def load_episode_data(self):
		self.loggerv3.info("Loading Episode Data...")
		episode_ids_string = ','.join("'{0}'".format(w) for w in self.episode_ids)
		query = f""" SELECT
						episode_key,
						episode_title,
						series_title,
						channel_title
					FROM
						warehouse.dim_segment_episode
					WHERE
						episode_key in (
							{episode_ids_string}
						)
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.episodes[result[0]] = {
				"episode_title": result[1],
				"series_title": result[2],
				"channel_title": result[3]
			}


	def merge_data(self):
		for episode_id in self.comments:
			if episode_id in self.episodes:
				self.comments[episode_id]['episode_title'] = self.episodes[episode_id]['episode_title']
				self.comments[episode_id]['series_title'] = self.episodes[episode_id]['series_title']
				self.comments[episode_id]['channel_title'] = self.episodes[episode_id]['channel_title']


	def create_final_dataframe(self):
		records = []
		for episode_id in self.comments:
			records.append(self.comments[episode_id])
		self.final_df = pd.DataFrame(records)


	def write_to_redshift(self):
		self.loggerv3.info("Writing to Redshift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Comment Job")
		self.load_episode_comments()
		self.load_episode_data()
		self.merge_data()
		self.create_final_dataframe()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
