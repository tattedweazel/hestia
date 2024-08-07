from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd
from utils.connectors.megaphone_api_connector import MegaphoneApiConnector



class DimMegaphoneJob(EtlJobV3):

	def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
		super().__init__(jobname=__name__, db_connector=db_connector)
		self.schema = 'warehouse'
		self.podcasts_table = 'dim_megaphone_podcast'
		self.episodes_table = 'dim_megaphone_episode'
		self.queries = []
		self.megaphone_api_connector = MegaphoneApiConnector(self.file_location, self.loggerv3)
		self.channels = [
			"rooster_teeth",
			"the_roost",
			"rooster_teeth_premium",
			"blind_nil_audio"
		]

		self.podcasts = []
		self.episodes = []
		self.podcast_df = None
		self.episodes_df = None


	def fetch_data(self):
		self.loggerv3.info("Fetching data")
		for idx, channel in enumerate(self.channels):
			self.loggerv3.info(f'Fetching podcasts for channel {idx+1} of {len(self.channels)}')
			podcasts = self.megaphone_api_connector.get_all_podcasts(channel)
			for pdx, podcast in enumerate(podcasts):
				self.add_to_podcasts(podcast, channel)
				self.loggerv3.info(f'Fetching episodes for podcast {pdx+1} of {len(podcasts)}')
				episodes = self.megaphone_api_connector.get_all_episodes(channel, podcast['id'])
				for episode in episodes:
					self.add_to_episodes(episode)


	def add_to_podcasts(self, podcast, channel):
		clean_podcast = {
			"id": podcast["id"],
			"created_at": podcast["createdAt"],
			"updated_at": podcast["updatedAt"],
			"title": podcast["title"],
			"link": podcast["link"],
			"author": podcast["author"],
			"explicit": podcast["explicit"],
			"owner_name": podcast["ownerName"],
			"subtitle": podcast["subtitle"],
			"uid": podcast["uid"],
			"slug": podcast["slug"],
			"network_id": podcast["networkId"],
			"podcast_type": podcast["podcastType"],
			"channel": channel,
			"clean_title": podcast["title"].replace('(FIRST Member Early Access)', '').strip()
		}
		self.podcasts.append(clean_podcast)


	def add_to_episodes(self, episode):
		clean_episode = {
			"id": episode["id"],
			"created_at": episode["createdAt"],
			"updated_at": episode["updatedAt"],
			"title": episode["title"],
			"pub_date": episode["pubdate"],
			"author": episode["author"],
			"episode_type": episode["episodeType"],
			"season_number": episode["seasonNumber"],
			"episode_number": episode["episodeNumber"],
			"duration": episode["duration"],
			"uid": episode["uid"],
			"podcast_id": episode["podcastId"],
			"pubdate_timezone": episode["pubdateTimezone"],
			"external_id": episode["externalId"],
			"clean_title": episode["cleanTitle"],
			"podcast_title": episode["podcastTitle"],
			"network_id": episode["networkId"],
			"podcast_author": episode["podcastAuthor"]
		}
		self.episodes.append(clean_episode)



	def build_dataframes(self):
		self.loggerv3.info('Building dataframes')
		self.podcast_df = pd.DataFrame(self.podcasts)
		self.episodes_df = pd.DataFrame(self.episodes)


	def truncate_tables(self):
		self.loggerv3.info(f'Truncating tables')
		self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.podcasts_table};")
		self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.episodes_table};")


	def write_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		if len(self.podcast_df) == 0:
			self.loggerv3.error('Podcast Dataframe is empty. Check Megaphone API.')
		if len(self.episodes_df) == 0:
			self.loggerv3.error('Episodes Dataframe is empty. Check Megaphone API.')
		self.db_connector.write_to_sql(self.podcast_df, self.podcasts_table, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.write_to_sql(self.episodes_df, self.episodes_table, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.podcasts_table, schema=self.schema)
		self.db_connector.update_redshift_table_permissions(self.episodes_table, schema=self.schema)


	def execute(self):
		self.loggerv3.start(f"Running Dim Megaphone Job")
		self.fetch_data()
		self.build_dataframes()
		self.truncate_tables()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
