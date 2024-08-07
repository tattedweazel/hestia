import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.classes.yt_episode import YTEpisode
from utils.classes.yt_series import YTSeries
from utils.components.batcher import Batcher


class FunhausYTEditorAnalysisJob(EtlJobV3):

	def __init__(self, series_show_flag, target_date=None, db_connector=None, api_connector=None, file_location=''):
		super().__init__(jobname=__name__, db_connector=db_connector, table_name='')
		""" series_show_flag must be either 'rt_series' or 'yt_show' """
		self.batcher = Batcher()
		self.series_show_flag = series_show_flag
		self.schema = 'warehouse'
		self.episodes_table = 'yt_editor_analysis_episodes'
		self.individual_editor_table = 'yt_editor_analysis_individual_editors'
		self.editor_combos_table = 'yt_editor_analysis_editor_combos'
		self.series = []
		self.filtered_series = []
		self.episode_data = {}
		self.batches = []
		self.episodes_df = None
		self.individual_editor_df = None
		self.editor_combo_df = None


	def build_query_for_data_load(self):
		self.loggerv3.info('Building query for data load')
		query = None
		if self.series_show_flag == 'rt_series':
			query = f"""
			SELECT
				fcm.member,
				fcm.episode_title,
				fcm.episode_key,
				fcm.series_title,
				vm.video_id
			FROM warehouse.funhaus_cast_map fcm
			INNER JOIN warehouse.dim_segment_episode dse ON dse.episode_key = fcm.episode_key
			INNER JOIN warehouse.yt_video_map vm ON vm.episode_key = dse.episode_key
			WHERE fcm.role = 'editor';
			"""
		elif self.series_show_flag == 'yt_show':
			query = f"""
			SELECT
				fcm.member,
				fcm.episode_title,
				fcm.episode_key,
				(CASE WHEN fp.show_name is NULL THEN 'Misc' ELSE fp.show_name END) as show_name,
				vm.video_id
			FROM warehouse.funhaus_cast_map fcm
			INNER JOIN warehouse.dim_segment_episode dse ON dse.episode_key = fcm.episode_key
			INNER JOIN warehouse.yt_video_map vm ON vm.episode_key = dse.episode_key
			LEFT JOIN airtable.funhaus_productions fp ON fp.production_id = fcm.production_id
			WHERE fcm.role = 'editor';
			"""

		return query


	def load_series_episode_editors(self):
		self.loggerv3.info("Loading Editors")
		series_episode_editors = {}
		query = self.build_query_for_data_load()
		results = self.db_connector.read_redshift(query)
		for result in results:
			editor = result[0]
			episode_key = result[2]
			episode_title = result[1]
			video_id = result[4]
			episode_str = f"{episode_key}/-/{episode_title}/-/{video_id}"
			series_name = result[3]
			if series_name not in series_episode_editors:
				series_episode_editors[series_name] = {episode_str: [editor]}
			elif episode_str not in series_episode_editors[series_name]:
				series_episode_editors[series_name][episode_str] = [editor]
			else:
				series_episode_editors[series_name][episode_str].append(editor)
		return series_episode_editors


	def populate_data(self):
		collection = self.load_series_episode_editors()
		for series_record in collection:
			series = YTSeries(series_record)
			for episode_str in collection[series_record]:
				episode = YTEpisode(episode_str)
				for editor in collection[series_record][episode_str]:
					episode.add_to_cast(editor)
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
			WHERE
				dyvv2.video_id IN ({vids}) AND
				(
					(
						to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') >= '2021-03-01' AND
						to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') <= '2022-01-10' AND
						dyvv2.published_at >= '2021-03-01' AND
						dyvv2.published_at <= '2022-01-03' 
					) OR
					(
				  		to_date(cast(coca2.start_date as varchar), 'YYYYMMDD') >= '2022-04-01' AND
				  		dyvv2.published_at >= '2022-04-01' AND
				  		dyvv2.published_at <= current_date -7
					)
				) 
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


	def build_episodes_dataframe(self):
		self.loggerv3.info('Building episodes dataframe')
		episodes = []
		for series in self.filtered_series:
			for episode in series.episodes:
				episodes.append({
					'series': series.title,
					'episode': episode.title,
					'episode_id': episode.uuid,
					'yt_video_id': episode.video_id,
					'air_date': episode.air_date,
					'editors': ', '.join(episode.cast),
					'editor_count': len(episode.cast),
					'views': episode.views,
					'hours_viewed': episode.hours_viewed,
					'game_name': episode.game_name
				})
		self.episodes_df = pd.DataFrame(episodes)
		convert = {'views': int, 'hours_viewed': int}
		self.episodes_df = self.episodes_df.astype(convert)


	def write_episodes_to_redshift(self):
		self.loggerv3.info('Writing episodes to redshift')

		self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.episodes_table};")
		self.db_connector.write_to_sql(self.episodes_df, self.episodes_table, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.episodes_table, schema=self.schema)


	def build_individual_editor_dataframe(self):
		self.loggerv3.info('Building individual editor dataframe')
		individual_editor = []
		for series in self.filtered_series:
			for editor, data in series.cast.items():
				individual_editor.append({
					'series': series.title,
					'editor': editor,
					'appearances': data['appearances'],
					'min_appearance_date': min(data['episode_air_dates']),
					'max_appearance_date': max(data['episode_air_dates']),
					'attributed_hours': data['attributed_hours'],
					'attributed_views': data['attributed_views'],
					'views_per_appearance': data['views_per_appearance'],
					'hours_per_appearance': data['hours_per_appearance'],
					'avg_minutes_per_view': data['avg_minutes_per_view'],
					'view_score': data['view_score'],
					'minutes_score': data['minutes_score'],
					'composite_score': data['composite_score']
				})

		self.individual_editor_df = pd.DataFrame(individual_editor)
		convert = {'appearances': int, 'attributed_hours': int, 'attributed_views': int, 'views_per_appearance': int, 'hours_per_appearance': int, 'avg_minutes_per_view': int, 'view_score': int, 'minutes_score': int}
		self.individual_editor_df = self.individual_editor_df.astype(convert)


	def write_individual_editor_to_redshift(self):
		self.loggerv3.info('Writing individual editor to redshift')

		self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.individual_editor_table};")
		self.db_connector.write_to_sql(self.individual_editor_df, self.individual_editor_table, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.individual_editor_table, schema=self.schema)


	def build_editor_combo_dataframe(self):
		self.loggerv3.info('Building editor combo dataframe')
		editor_combos = []
		for series in self.filtered_series:
			for editor_str, data in series.cast_combo_rollups.items():
				editor_combos.append({
					'series': series.title,
					'editor_combo': editor_str.replace('-', ', '),
					'occurrences': data['occurrences'],
					'hours': data['hours'],
					'views': data['views'],
					'avg_hours': data['avg_hours'],
					'avg_views': data['avg_views']
				})
		self.editor_combo_df = pd.DataFrame(editor_combos)


	def write_editor_combo_to_redshift(self):
		self.loggerv3.info('Writing editor combos to redshift')

		self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.editor_combos_table};")
		self.db_connector.write_to_sql(self.editor_combo_df, self.editor_combos_table, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.editor_combos_table, schema=self.schema)


	def execute(self):
		self.loggerv3.info(f'Running Funhaus YouTube Editor Analysis Job - {self.series_show_flag}')
		self.populate_data()
		self.populate_episode_data()
		self.hydrate_episodes()
		self.hydrate_series()
		self.filter_series()
		# Episode
		self.build_episodes_dataframe()
		self.write_episodes_to_redshift()
		# Individual Editors
		self.build_individual_editor_dataframe()
		self.write_individual_editor_to_redshift()
		# Editor Combos
		self.build_editor_combo_dataframe()
		self.write_editor_combo_to_redshift()
		self.loggerv3.success("All Processing Complete!")
