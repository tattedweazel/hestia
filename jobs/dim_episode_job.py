import pandas as pd
from base.etl_jobv3 import EtlJobV3


class DimEpisodeJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'dim_segment_episode')
		self.episodes = []
		self.final_dataframe = None


	def load_episodes(self):
		self.loggerv3.info('Loading episodes')
		results = self.db_connector.query_svod_be_db_connection(f""" 
		SELECT
			ep.id as episode_id,							
			ep.uuid as episode_key,
			ep.title as episode_title,
			ep.number as episode_number,
			ep.slug as episode_slug,
			ep.length as length_in_seconds,
			ep.sponsor_golive_at as air_date,
			ep.public_golive_at as public_date,
			sn.uuid as season_id,
			sn.title as season_title,
			sn.number as season_number,
			sh.uuid as series_id,
			sh.title as series_title,
			ch.uuid as channel_id,
			ch.name as channel_title
		FROM episodes ep
		LEFT JOIN seasons sn on ep.season_id = sn.id
		LEFT JOIN shows sh on sn.show_id = sh.id
		LEFT JOIN channels ch on sh.channel_id = ch.id;
		""")
		for result in results:
			self.episodes.append({
				"episode_id": result[0],
				"episode_key": result[1],
				"episode_title": result[2],
				"episode_number": result[3],
				"episode_slug": result[4],
				"length_in_seconds": result[5],
				"air_date": result[6],
				"public_date": result[7],
				"season_id": result[8],
				"season_title": result[9],
				"season_number": int(result[10]),
				"series_id": result[11],
				"series_title": result[12],
				"channel_id": result[13],
				"channel_title": result[14]
			})


	def load_bonus_features(self):
		self.loggerv3.info('Loading bonus features')
		results = self.db_connector.query_svod_be_db_connection(f"""
		SELECT
			ep.id as episode_id,
			ep.uuid as episode_key,
			ep.title as episode_title,
			ep.number as episode_number,
			ep.slug as episode_slug,
			ep.length as length_in_seconds,
			ep.sponsor_golive_at as air_date,
			ep.public_golive_at as public_date,
			Null as season_id,
			Null as season_title,
			Null as season_number,
			sh.uuid as series_id,
			sh.title as series_title,
			ch.uuid as channel_id,
			ch.name as channel_title
		FROM bonus_features ep
		LEFT JOIN shows sh on ep.parent_content_id = sh.id
		LEFT JOIN channels ch on sh.channel_id = ch.id;
		""")
		for result in results:
			self.episodes.append({
				"episode_id": result[0],
				"episode_key": result[1],
				"episode_title": result[2],
				"episode_number": result[3],
				"episode_slug": result[4],
				"length_in_seconds": result[5],
				"air_date": result[6],
				"public_date": result[7],
				"season_id": result[8],
				"season_title": result[9],
				"season_number": result[10],
				"series_id": result[11],
				"series_title": result[12],
				"channel_id": result[13],
				"channel_title": result[14]
			})


	def clear_dim_table(self):
		self.loggerv3.info('Truncating table')
		query = f"TRUNCATE TABLE warehouse.{self.table_name}"
		self.db_connector.write_redshift(query)


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		self.final_dataframe = pd.DataFrame(self.episodes)


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(
			self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append'
		)
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Dim Episode")
		self.load_episodes()
		self.load_bonus_features()
		self.build_final_dataframe()
		self.clear_dim_table()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
