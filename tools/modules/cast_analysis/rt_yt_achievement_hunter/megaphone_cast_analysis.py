import config
import os
import pandas as pd
from utils.classes.yt_episode import YTEpisode
from utils.classes.yt_series import YTSeries
from utils.connectors.database_connector import DatabaseConnector
from utils.components.batcher import Batcher
from utils.components.loggerv3 import Loggerv3


class MegaphoneCastAnalysis:

	def __init__(self, min_air_date, max_air_date, series_name):
		"""Using megaphone"""
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.batcher = Batcher()
		self.output_directory = config.file_location + 'tools/output'
		self.series_name = series_name
		self.min_air_date = min_air_date
		self.max_air_date = max_air_date
		self.cast_members = []
		self.yt_videos = []

		self.series = []
		self.filtered_series = []
		self.episode_data = {}
		self.batches = None


	def get_cast_members(self):
		self.loggerv3.info('Getting cast members')
		query = f"""
		SELECT
			(CASE
				WHEN concat(cm.first_name, concat(' ', cm.last_name)) in ('Joe Lee', 'Loe Jeez', 'Joseph Lee') THEN 'Joe Lee'
				WHEN concat(cm.first_name, concat(' ', cm.last_name)) in ('Kylah Cooke', 'ky cooke') THEN 'Kylah Cook'
				WHEN concat(cm.first_name, concat(' ', cm.last_name)) in ('Gabie Jackman', 'Black Krystel') THEN 'BK'
				ELSE concat(cm.first_name, concat(' ', cm.last_name))
			END) as cast_member,
			e.title as episode_title,
			e.uuid as episode_key,
			s2.title as series_name
		FROM cast_members cm
		LEFT JOIN cast_member_maps cmm ON cmm.cast_member_uuid = cm.uuid
		LEFT JOIN episodes e ON cmm.cast_memberable_id = e.uuid
		LEFT JOIN seasons s ON e.season_id = s.id
		LEFT JOIN shows s2 ON s.show_id = s2.id
		WHERE
			cm.last_name NOT IN ('Kovic', 'Haywood')
			AND e.title is not null
			AND lower(s2.title) = '{self.series_name}';
			AND e.uuid not in ('1c22ae39-6a6d-4877-b36f-dee0175988de', '43933283-7e35-43ca-b6e6-c1d4ad6f70f5');
		"""
		results = self.db_connector.query_svod_be_db_connection(query)
		for result in results:
			self.cast_members.append({
				'cast_member': result[0],
				'episode_title': result[1],
				'episode_key': result[2],
				'series_name': result[3]
			})


	def load_series_episode_cast(self):
		self.loggerv3.info("Loading Cast")
		series_episode_cast = {}

		for record in self.cast_members:
			cast_member = record['cast_member']
			episode_key = record['episode_key']
			episode_title = record['episode_title']
			episode_str = f"{episode_key}/-/{episode_title}/-/{episode_key}"
			series_name = record['series_name']
			if series_name not in series_episode_cast:
				series_episode_cast[series_name] = {episode_str: [cast_member]}
			elif episode_str not in series_episode_cast[series_name]:
				series_episode_cast[series_name][episode_str] = [cast_member]
			else:
				series_episode_cast[series_name][episode_str].append(cast_member)
		return series_episode_cast


	def populate_data(self):
		self.collection = self.load_series_episode_cast()
		for series_record in self.collection:
			series = YTSeries(series_record)
			for episode_str in self.collection[series_record]:
				episode = YTEpisode(episode_str)
				for cast_member in self.collection[series_record][episode_str]:
					episode.add_to_cast(cast_member)
				series.add_episode(episode)
			self.series.append(series)


	def populate_episode_data(self):
		self.loggerv3.info("Loading Episode Data")
		episode_keys = set()
		for series in self.series:
			for episode in series.episodes:
				episode_keys.add(f"'{episode.uuid}'")
		self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=episode_keys)
		for batch in self.batches:
			vids = ','.join(batch)
			query = f"""
			WITH cte as (
				SELECT
				   m.episode_title,
				   right(m.episode_title, 4) as episode_num,
				   min(air_date) as air_date,
				   sum(m.views) as views
				FROM warehouse.agg_daily_podcast_metrics m
				WHERE
					lower(m.series_title) = '{self.series_name}'
					AND m.early_access = 'false'
					AND m.platform = 'megaphone'
				GROUP BY 1, 2
			), cte2 as (
				SELECT episode_key,
					   right(episode_title, 4) as episode_num
				FROM warehouse.dim_segment_episode
				WHERE lower(series_title) = '{self.series_name}'
			)
			SELECT 
			   cte2.episode_key,
			   cte.air_date,
			   cte.views
			FROM cte
			LEFT JOIN cte2 on cte2.episode_num = cte.episode_num
			WHERE cte2.episode_key IN ({vids})
			ORDER BY 1 desc;
			"""
			results = self.db_connector.read_redshift(query)
			for result in results:
				self.episode_data[result[0]] = {
					"air_date": result[1],
					"hours_viewed": 1,
					"views": round(result[2], 0)
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
		df.to_csv(f'{self.output_directory}/series_overview.csv', encoding='utf-8')


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
		df.to_csv(f'{self.output_directory}/episodes_overview.csv', encoding='utf-8')


	def output_individual_cast_report(self, exclude_single_appearances=False):
		self.loggerv3.info('Outputting individual cast report')
		for series in self.filtered_series:
			df = pd.DataFrame(series.cast).transpose()
			convert = {'appearances': int, 'attributed_hours': int, 'attributed_views': int, 'views_per_appearance': int, 'hours_per_appearance': int, 'avg_minutes_per_view': int, 'view_score': int, 'minutes_score': int}
			df = df.astype(convert)
			df = df.sort_values(by=['composite_score'], ascending=False)
			if exclude_single_appearances:
				df = df[df['appearances'] > 1]
			df.to_csv(f'{self.output_directory}/individual_{series.title.lower()}.csv', encoding='utf-8')


	def output_combo_cast_report(self):
		self.loggerv3.info('Outputting cast combo report')
		for series in self.filtered_series:
			df = pd.DataFrame(series.cast_combo_rollups).transpose()
			df.reset_index(inplace=True)
			df = df.rename(columns={'index': 'combo'})
			df['combo'] = df['combo'].str.replace('-', ', ')
			df = df.sort_values(by=['avg_views'], ascending=False)
			df.to_csv(f'{self.output_directory}/combo_{series.title.lower()}.csv', encoding='utf-8')


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
			df.to_csv(f'{self.output_directory}/episodes_{series.title.lower()}.csv', encoding='utf-8')


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
		self.get_cast_members()
		self.populate_data()
		self.populate_episode_data()
		self.hydrate_episodes()
		self.hydrate_series()
		self.filter_series()
		self.output()
		self.loggerv3.success("All Processing Complete!")



