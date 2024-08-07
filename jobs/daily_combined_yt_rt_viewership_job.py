import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.backfill_by_dt import backfill_by_date


class DailyCombinedYtRtViewershipJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'daily_combined_yt_rt_viewership')
		self.target_date = target_date
		self.min_date = '2022-04-01'
		self.dim_records = []
		self.records = []
		self.records_df = None
		self.yt_views = []
		self.rt_views = []
		self.final_dataframe = None


	def get_dim_records(self):
		self.loggerv3.info('Getting desktop viewers')
		query = f"""
			SELECT
			  dim.channel_title,
			  dim.video_id,
			  dim.video_title,
			  dim.published_at as pub_date,
			  mp.episode_key
			FROM warehouse.dim_yt_video_v2 dim
			LEFT JOIN warehouse.yt_video_map mp on mp.video_id = dim.video_id
			WHERE
			  dim.channel_title in ('Achievement Hunter', 'Funhaus', 'Rooster Teeth', 'Rooster Teeth Animation', 'LetsPlay', 'DEATH BATTLE!')
			  AND pub_date >= '2022-04-01';
		"""
		results = self.db_connector.read_redshift(query)

		for result in results:
			self.dim_records.append({
				'channel_title': result[0],
				'video_id': result[1],
				'video_title': result[2],
				'pub_date': result[3],
				'episode_key': result[4]
			})


	def build_time_series(self):
		self.loggerv3.info('Building time series')
		backfill_dates = backfill_by_date(latest_date=self.target_date, earliest_date=self.min_date)
		dates_list = []
		for date in backfill_dates:
			dates_list.append({
				'view_date': date
			})

		dates_df = pd.DataFrame(dates_list)
		dim_records_df = pd.DataFrame(self.dim_records)
		self.records_df = dim_records_df.merge(dates_df, how='cross')


	def get_yt_views(self):
		query = f"""
			SELECT to_char(to_date(cast(coca2.start_date as varchar), 'YYYYMMDD'), 'YYYY-MM-DD') as view_date,
				   dim.video_id,
				   sum(coca2.views) as yt_views
			FROM warehouse.content_owner_combined_a2 coca2
			LEFT JOIN warehouse.dim_yt_video_v2 dim on dim.video_id = coca2.video_id
			WHERE dim.channel_title in ('Achievement Hunter', 'Funhaus', 'Rooster Teeth', 'Rooster Teeth Animation', 'LetsPlay', 'DEATH BATTLE!')
			  AND dim.published_at >= '2022-04-01'
			GROUP BY 1, 2;
		"""
		results = self.db_connector.read_redshift(query)

		for result in results:
			self.yt_views.append({
				'view_date': result[0],
				'video_id': result[1],
				'yt_views': result[2]
			})


	def get_rt_views(self):
		query = f"""
			SELECT cast(start_timestamp as varchar(10)) as view_date,
				   vv.episode_key,
				   count(*) as rt_views
			FROM warehouse.vod_viewership vv
			INNER JOIN warehouse.yt_video_map mp on mp.episode_key = vv.episode_key
			WHERE start_timestamp >= '2022-04-01'
			GROUP BY 1, 2;
		"""
		results = self.db_connector.read_redshift(query)

		for result in results:
			self.rt_views.append({
				'view_date': result[0],
				'episode_key': result[1],
				'rt_views': result[2]
			})


	def build_final_dataframe(self):
		self.loggerv3.info("Building final dataframe")
		yt_views_df = pd.DataFrame(self.yt_views)
		rt_views_df = pd.DataFrame(self.rt_views)

		self.final_dataframe = self.records_df.merge(yt_views_df, how='left', on=['video_id', 'view_date'])
		self.final_dataframe = self.final_dataframe.merge(rt_views_df, how='left', on=['episode_key', 'view_date'])
		self.final_dataframe = self.final_dataframe[~(self.final_dataframe.rt_views.isna()) | ~(self.final_dataframe.yt_views.isna())]


	def truncate_table(self):
		self.loggerv3.info('Truncating table')
		self.db_connector.write_redshift(f"TRUNCATE TABLE warehouse.{self.table_name};")


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append', chunksize=5000)
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Daily Combined YT RT Viewership")
		self.get_dim_records()
		self.build_time_series()
		self.get_yt_views()
		self.get_rt_views()
		self.build_final_dataframe()
		self.truncate_table()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
