import config
import os
import pandas as pd
from utils.classes.yt_episode import YTEpisode
from utils.classes.yt_series import YTSeries
from utils.connectors.database_connector import DatabaseConnector
from utils.components.batcher import Batcher
from utils.components.loggerv3 import Loggerv3


class AchievementHunterRTYTCastAnalysis:

	def __init__(self, min_air_date, max_air_date, series_name):
		"""Using content owner combined a2"""
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
			concat(cm.first_name, concat(' ', cm.last_name)) as cast_member,
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
			AND s2.title = '{self.series_name}';
		"""
		results = self.db_connector.query_svod_be_db_connection(query)
		for result in results:
			self.cast_members.append({
				'cast_member': result[0],
				'episode_title': result[1],
				'episode_key': result[2],
				'series_name': result[3]
			})


	def get_youtube_videos(self):
		self.loggerv3.info('Getting youtube videos')
		query = f"""
		SELECT
			episode_key,
			video_id 
		FROM warehouse.yt_video_map;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.yt_videos.append({
				'episode_key': result[0],
				'video_id': result[1]
			})


	def build_query_for_data_load(self):
		self.loggerv3.info('Building query for data load')
		cast_members_df = pd.DataFrame(self.cast_members)
		yt_videos_df = pd.DataFrame(self.yt_videos)
		self.joined_df = yt_videos_df.merge(cast_members_df, how='inner', on='episode_key')


	def load_series_episode_cast(self):
		self.loggerv3.info("Loading Cast")
		records = self.joined_df.to_dict('records')
		series_episode_cast = {}

		for record in records:
			cast_member = record['cast_member']
			episode_key = record['episode_key']
			episode_title = record['episode_title']
			video_id = record['video_id']
			episode_str = f"{episode_key}/-/{episode_title}/-/{video_id}"
			series_name = record['series_name']
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
				  to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') < current_date AND
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
		self.get_youtube_videos()
		self.build_query_for_data_load()
		self.populate_data()
		self.populate_episode_data()
		self.hydrate_episodes()
		self.hydrate_series()
		self.filter_series()
		self.output()
		self.loggerv3.success("All Processing Complete!")



