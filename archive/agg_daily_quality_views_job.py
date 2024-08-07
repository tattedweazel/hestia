import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class AggDailyQualityViewsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname=__name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_daily_quality_views')
		self.target_dt = datetime.strptime(f"{self.target_date} 07:00", '%Y-%m-%d %H:%M')
		self.next_date = datetime.strftime(self.target_dt + timedelta(1), '%Y-%m-%d')
		self.records = None
		self.final_dataframe = None


	def get_views_by_duration(self):
		self.query = f"""WITH cte_quartiles AS (
							SELECT
								vv.start_timestamp,
								dse.air_date,
								dse.channel_title,
								dse.series_title,
								dse.episode_title,
								dse.episode_key,
								vv.user_tier,
								dse.length_in_seconds,
								vv.active_seconds,
								(CASE WHEN cast(vv.active_seconds AS FLOAT)/cast(dse.length_in_seconds AS FLOAT) <= .25 THEN 1 ELSE 0 END) AS lte_25,
								(CASE WHEN cast(vv.active_seconds AS FLOAT)/cast(dse.length_in_seconds AS FLOAT) > .25 AND cast(vv.active_seconds AS FLOAT)/cast(dse.length_in_seconds AS FLOAT) <= .5 THEN 1 ELSE 0 END) AS gt_25_lte_50,
								(CASE WHEN cast(vv.active_seconds AS FLOAT)/cast(dse.length_in_seconds AS FLOAT) > .5 AND cast(vv.active_seconds AS FLOAT)/cast(dse.length_in_seconds AS FLOAT) <= .75 THEN 1 ELSE 0 END) AS gt_50_lte_75,
								(CASE WHEN cast(vv.active_seconds AS FLOAT)/cast(dse.length_in_seconds AS FLOAT) > .75 THEN 1 ELSE 0 END) AS gt_75
							FROM warehouse.vod_viewership vv
								JOIN warehouse.dim_segment_episode dse ON vv.episode_key = dse.episode_key
							WHERE vv.start_timestamp >= '{self.target_date}' AND
								  vv.start_timestamp < '{self.next_date}' AND
								  dse.length_in_seconds > 0
						), cte_denormalized_quartiles AS (
							SELECT
								cast(start_timestamp AS DATE) AS date,
								air_date,
								channel_title,
								series_title,
								episode_title,
								episode_key,
								user_tier,
								length_in_seconds as duration,
								sum(active_seconds) as time_watched_in_seconds,
								count(*) AS total_views,
								sum(lte_25) AS lte_25,
								sum(gt_25_lte_50) AS gt_25_lte_50,
								sum(gt_50_lte_75) AS gt_50_lte_75,
								sum(gt_75) AS gt_75
							FROM cte_quartiles q
							GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
						)
						SELECT
							cast(concat(date, ' 07:00:00') AS TIMESTAMP) AS date_time,
							air_date,
							channel_title,
							series_title,
							episode_title,
							episode_key,
							user_tier,
							duration,
							time_watched_in_seconds,
							total_views,
							lte_25,
							gt_25_lte_50,
							gt_50_lte_75,
							gt_75
						FROM cte_denormalized_quartiles;"""
		results = self.db_connector.read_redshift(self.query)

		self.records = []
		for result in results:
			data = {
				'date_time': result[0],
				'air_date': result[1],
				'channel_title': result[2],
				'series_title': result[3],
				'episode_title': result[4],
				'episode_key': result[5],
				'user_tier': result[6],
				'duration': result[7],
				'time_watched_in_seconds': result[8],
				'total_views': result[9],
				'lte_25': result[10],
				'gt_25_lte_50': result[11],
				'gt_50_lte_75': result[12],
				'gt_75': result[13],
				'quality_views': result[11] + result[12] + result[13]
			}
			self.records.append(data)


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		self.final_dataframe = pd.DataFrame(self.records, index=None)


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Aggregating quality views for {self.target_date}")
		self.get_views_by_duration()
		self.build_final_dataframe()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
