import config
import os
import pandas as pd
from tools.modules.cast_analysis.base.episode import Episode
from tools.modules.cast_analysis.base.series import Series
from utils.connectors.database_connector import DatabaseConnector
from utils.components.batcher import Batcher
from utils.components.loggerv3 import Loggerv3


class FunhausRTCastAnalysis:

	def __init__(self, min_air_date, max_air_date):
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.batcher = Batcher()
		self.min_air_date = min_air_date
		self.max_air_date = max_air_date
		self.series = []
		self.episode_data = {}
		self.batches = None
		self.output_directory = config.file_location + 'tools/output'


	def load_series_episode_cast(self):
		self.loggerv3.info("Loading Cast")
		series_episode_cast = {}
		query = f"""
		SELECT
			fcm.cast_member,
			fcm.episode_title,
			fcm.episode_key,
			fcm.series_title
		FROM warehouse.funhaus_cast_map fcm
		INNER JOIN warehouse.dim_segment_episode dse ON dse.episode_key = fcm.episode_key
		WHERE
			dse.air_date >= '{self.min_air_date}' AND
			dse.air_date < '{self.max_air_date}';
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			cast_member = result[0]
			episode_str = f"{result[2]}/-/{result[1]}"
			series_name = result[3]
			if series_name not in series_episode_cast:
				series_episode_cast[series_name] = {episode_str: [cast_member]}
			elif episode_str not in series_episode_cast[series_name]:
				series_episode_cast[series_name][episode_str] = [cast_member]
			else:
				series_episode_cast[series_name][episode_str].append(cast_member)
		return series_episode_cast


	def populate_data(self):
		collection = self.load_series_episode_cast()
		for series_record in collection:
			series = Series(series_record)
			for episode_str in collection[series_record]:
				episode = Episode(episode_str)
				for cast_member in collection[series_record][episode_str]:
					episode.add_to_cast(cast_member)
				series.add_episode(episode)
			self.series.append(series)


	def populate_episode_data(self):
		self.loggerv3.info("Loading Episode Data")
		episode_uuids = set()
		for series in self.series:
			for episode in series.episodes:
				episode_uuids.add(f"'{episode.uuid}'")
		self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=episode_uuids)
		for batch in self.batches:
			uuids = ','.join(batch)
			query = f"""
				SELECT
					dse.episode_key,
					dse.air_date,
					dse.length_in_seconds,
					sum(vv.active_seconds) / 3600.0 as hours_viewed,
					count(*) as views,
					count(distinct vv.user_key) as unique_viewers
				FROM warehouse.dim_segment_episode dse
				LEFT JOIN warehouse.vod_viewership vv
				ON dse.episode_key = vv.episode_key
				WHERE dse.episode_key IN ({uuids}) AND
					  vv.start_timestamp >= '{self.min_air_date}' AND
					  vv.start_timestamp < '{self.max_air_date}' AND
					  dse.air_date >= '{self.min_air_date}' AND
					  dse.air_date < '{self.max_air_date}'
				GROUP BY 1, 2, 3;
			"""
			results = self.db_connector.read_redshift(query)
			for result in results:
				self.episode_data[result[0]] = {
					"air_date": result[1],
					"length": result[2],
					"hours_viewed": result[3],
					"views": result[4],
					"uvs": result[5]
				}


	def batch_items(self, items=[], batch_size=500):
		batches = []
		current_batch = []
		for item in items:
			if len(current_batch) == batch_size:
				batches.append(current_batch)
				current_batch = []
			current_batch.append(item)
		batches.append(current_batch)
		return batches


	def hydrate_episodes(self):
		self.loggerv3.info("Hydrating Episodes")
		for series in self.series:
			for episode in series.episodes:
				if episode.uuid in self.episode_data:
					episode.hydrate(self.episode_data[episode.uuid])


	def hydrate_series(self):
		self.loggerv3.info("Hydrating Series")
		for series in self.series:
			series.hydrate()


	def find_series(self, target_name):
		for series in self.series:
			if series.title.lower() == target_name.lower():
				return series


	def display_series(self):
		self.series.sort(key=lambda x: x.avg_uvs_per_appearance, reverse=True)
		for series in self.series:
			print(f"{series.title}: {series.avg_uvs_per_appearance}")


	def output_series_overview(self):
		overview = []
		for series in self.series:
			overview.append({
				'title': series.title,
				'avg_episode_views': round(series.avg_episode_views, 0),
				'episodes': len(series.episodes),
				'avg_cast_count': round(series.avg_cast_count, 0)
			})
		df = pd.DataFrame(overview)
		convert = {'avg_episode_views': int, 'episodes': int, 'avg_cast_count': int}
		df = df.astype(convert)
		df = df.sort_values(by=['episodes'], ascending=False)
		df.to_csv(f'{self.output_directory}/series_overview.csv', sep='\t', encoding='utf-8')


	def output_individual_cast_report(self, exclude_single_appearances=False):
		for series in self.series:
			df = pd.DataFrame(series.cast).transpose()
			convert = {'appearances': int, 'attributed_hours': int, 'attributed_views': int, 'uvs_per_appearance': int, 'hours_per_appearance': int, 'avg_minutes_per_viewer': int, 'uv_score': int, 'minutes_score': int}
			df = df.astype(convert)
			df = df.sort_values(by=['composite_score'], ascending=False)
			if exclude_single_appearances:
				df = df[df['appearances'] > 1]
			df.to_csv(f'{self.output_directory}/individual_{series.title.lower()}.csv', sep='\t', encoding='utf-8')


	def output_combo_cast_report(self):
		for series in self.series:
			df = pd.DataFrame(series.cast_combo_rollups).transpose()
			df.reset_index(inplace=True)
			df = df.rename(columns={'index': 'combo'})
			df['combo'] = df['combo'].str.replace('-', ', ')
			df = df.sort_values(by=['avg_views'], ascending=False)
			df.to_csv(f'{self.output_directory}/combo_{series.title.lower()}.csv', sep='\t', encoding='utf-8')


	def output_episodes_report(self):
		for series in self.series:
			episodes = []
			for episode in series.episodes:
				episodes.append(episode.output())
			df = pd.DataFrame(episodes)
			convert = {'hours_viewed': int}
			df = df.astype(convert)
			df = df.sort_values(by=['uvs'], ascending=False)
			df.to_csv(f'{self.output_directory}/episodes_{series.title.lower()}.csv', sep='\t', encoding='utf-8')


	def output(self):
		self.output_series_overview()
		self.output_individual_cast_report()
		self.output_combo_cast_report()
		self.output_episodes_report()


	def clean_up(self):
		self.loggerv3.info('Cleaning up files in local dir')
		if len(os.listdir(f'{self.output_directory}/')) > 0:
			os.system(f'rm {self.output_directory}/*.*')


	def execute(self):
		self.clean_up()
		self.populate_data()
		self.populate_episode_data()
		self.hydrate_episodes()
		self.hydrate_series()
		self.output()
		self.loggerv3.success("All Processing Complete!")



