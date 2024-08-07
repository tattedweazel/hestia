import config
import os
import pandas as pd
from utils.classes.yt_episode import YTEpisode
from utils.classes.yt_series import YTSeries
from utils.connectors.database_connector import DatabaseConnector
from utils.components.batcher import Batcher
from utils.components.loggerv3 import Loggerv3


class FunhausYTCastAnalysis:

	def __init__(self, min_air_date, max_air_date, series_show_flag):
		""" series_show_flag must be either 'rt_series' or 'yt_show' """
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.batcher = Batcher()
		self.min_air_date = min_air_date
		self.max_air_date = max_air_date
		self.series_show_flag = series_show_flag
		self.output_directory = config.file_location + 'tools/output'
		self.series = []
		self.filtered_series = []
		self.episode_data = {}
		self.batches = None


	def build_query_for_data_load(self):
		self.loggerv3.info('Building query for data load')
		query = None
		if self.series_show_flag == 'rt_series':
			query = f"""
			SELECT
				fcm.cast_member,
				fcm.episode_title,
				fcm.episode_key,
				fcm.series_title,
				vm.video_id
			FROM warehouse.funhaus_cast_map fcm
			INNER JOIN warehouse.dim_segment_episode dse ON dse.episode_key = fcm.episode_key
			INNER JOIN warehouse.yt_video_map vm ON vm.episode_key = dse.episode_key
			WHERE
				dse.air_date >= '{self.min_air_date}' AND
				dse.air_date < '{self.max_air_date}';
			"""
		elif self.series_show_flag == 'yt_show':
			query = f"""
			SELECT
				fcm.cast_member,
				fcm.episode_title,
				fcm.episode_key,
				(CASE WHEN fp.show_name is NULL THEN 'Misc' ELSE fp.show_name END) as show_name,
				vm.video_id
			FROM warehouse.funhaus_cast_map fcm
			INNER JOIN warehouse.dim_segment_episode dse ON dse.episode_key = fcm.episode_key
			INNER JOIN warehouse.yt_video_map vm ON vm.episode_key = dse.episode_key
			LEFT JOIN airtable.funhaus_productions fp ON fp.production_id = fcm.production_id
			WHERE
				dse.air_date >= '{self.min_air_date}' AND
				dse.air_date < '{self.max_air_date}';
			"""

		return query



	def load_series_episode_cast(self):
		self.loggerv3.info("Loading Cast")
		series_episode_cast = {}
		query = self.build_query_for_data_load()
		results = self.db_connector.read_redshift(query)
		for result in results:
			cast_member = result[0]
			episode_key = result[2]
			episode_title = result[1]
			video_id = result[4]
			episode_str = f"{episode_key}/-/{episode_title}/-/{video_id}"
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
			series = YTSeries(series_record)
			for episode_str in collection[series_record]:
				episode = YTEpisode(episode_str)
				for cast_member in collection[series_record][episode_str]:
					episode.add_to_cast(cast_member)
				series.add_episode(episode)
			self.series.append(series)


	def populate_episode_data(self):
		self.loggerv3.info("Loading Episode Data")
		video_ids = set()
		for series in self.series:
			for episode in series.episodes:
				video_ids.add(f"'{episode.video_id}'")
		self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=video_ids)
		for batch in self.batches:
			vids = ','.join(batch)
			query = f"""
			WITH airtable_productions as (
				SELECT yvm.episode_key, fp.game_name, fp.show_name
				FROM warehouse.yt_video_map yvm
				LEFT JOIN warehouse.funhaus_cast_map fcm ON fcm.episode_key = yvm.episode_key
				LEFT JOIN airtable.funhaus_productions fp ON fp.production_id = fcm.production_id
				GROUP BY 1, 2, 3
			)
			SELECT
				dyvv2.video_id,
				dyvv2.published_at,
				ap.game_name,
				(CASE WHEN ap.show_name is NULL THEN 'Misc' ELSE ap.show_name END) as show_name,
				sum(coca2.watch_time_minutes) / 60.0 as hours_viewed,
				sum(coca2.views) as views
			FROM warehouse.dim_yt_video_v2 dyvv2
			LEFT JOIN warehouse.content_owner_combined_a2 coca2 ON coca2.video_id = dyvv2.video_id
			LEFT JOIN warehouse.yt_video_map yvm ON yvm.video_id = dyvv2.video_id
			LEFT JOIN airtable_productions ap ON ap.episode_key = yvm.episode_key
			WHERE dyvv2.video_id IN ({vids}) AND
				  to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') >= '{self.min_air_date}' AND
				  to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') < '{self.max_air_date}' AND
				  dyvv2.published_at >= '{self.min_air_date}' AND
				  dyvv2.published_at < '{self.max_air_date}'
			GROUP BY 1, 2, 3, 4;
			"""
			results = self.db_connector.read_redshift(query)
			for result in results:
				self.episode_data[result[0]] = {
					"air_date": result[1],
					"game_name": result[2],
					"show_name": result[3],
					"hours_viewed": round(result[4], 0),
					"views": round(result[5], 0)
				}


	def hydrate_episodes(self):
		self.loggerv3.info("Hydrating Episodes")
		for series in self.series:
			for episode in series.episodes:
				if episode.video_id in self.episode_data:
					episode.hydrate(self.episode_data[episode.video_id])


	def hydrate_series(self):
		self.loggerv3.info("Hydrating Series")
		for series in self.series:
			if len(series.episodes) > 0:
				series.hydrate()

	def filter_series(self):
		self.loggerv3.info("Filtering Series")
		self.filtered_series = []
		for series in self.series:
			if len(series.episodes) > 0:
				self.filtered_series.append(series)


	def find_series(self, target_name):
		for series in self.series:
			if series.title.lower() == target_name.lower():
				return series


	def display_series(self):
		self.series.sort(key=lambda x: x.avg_views_per_appearance, reverse=True)
		for series in self.series:
			print(f"{series.title}, {round(series.avg_episode_views, 0)} views/episode, {len(series.episodes)} episodes")


	def output_series_overview(self):
		self.loggerv3.info('Outputting series overview')
		overview = []
		for series in self.filtered_series:
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
		df.to_csv(f'{self.output_directory}/{self.series_show_flag}_series_overview.csv', sep='\t', encoding='utf-8')


	def output_episodes_overview(self):
		self.loggerv3.info('Outputting episodes overview')
		overview = []
		for series in self.filtered_series:
			for episode in series.episodes:
				overview.append({
					'series': series.title,
					'episode': episode.title,
					'views': episode.views
				})
		df = pd.DataFrame(overview)
		convert = {'views': int}
		df = df.astype(convert)
		df = df.sort_values(by=['views'], ascending=False)
		df.to_csv(f'{self.output_directory}/{self.series_show_flag}_episodes_overview.csv', sep='\t', encoding='utf-8')


	def output_individual_cast_report(self, exclude_single_appearances=False):
		self.loggerv3.info('Outputting individual cast report')
		for series in self.filtered_series:
			df = pd.DataFrame(series.cast).transpose()
			convert = {'appearances': int, 'attributed_hours': int, 'attributed_views': int, 'views_per_appearance': int, 'hours_per_appearance': int, 'avg_minutes_per_view': int, 'view_score': int, 'minutes_score': int}
			df = df.astype(convert)
			df = df.sort_values(by=['composite_score'], ascending=False)
			if exclude_single_appearances:
				df = df[df['appearances'] > 1]
			df.to_csv(f'{self.output_directory}/individual_{series.title.lower()}.csv', sep='\t', encoding='utf-8')


	def output_combo_cast_report(self):
		self.loggerv3.info('Outputting cast combo report')
		for series in self.filtered_series:
			df = pd.DataFrame(series.cast_combo_rollups).transpose()
			df.reset_index(inplace=True)
			df = df.rename(columns={'index': 'combo'})
			df['combo'] = df['combo'].str.replace('-', ', ')
			df = df.sort_values(by=['avg_views'], ascending=False)
			df.to_csv(f'{self.output_directory}/combo_{series.title.lower()}.csv', sep='\t', encoding='utf-8')


	def output_episodes_report(self):
		self.loggerv3.info('Outputting episodes report')
		for series in self.filtered_series:
			episodes = []
			for episode in series.episodes:
				episodes.append(episode.output())
			df = pd.DataFrame(episodes)
			convert = {'hours_viewed': int}
			df = df.astype(convert)
			df = df.sort_values(by=['views'], ascending=False)
			df.to_csv(f'{self.output_directory}/episodes_{series.title.lower()}.csv', sep='\t', encoding='utf-8')


	def output(self):
		self.output_series_overview()
		self.output_episodes_overview()
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
		self.filter_series()
		self.output()
		self.loggerv3.success("All Processing Complete!")



