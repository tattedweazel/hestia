import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class FtpViewershipLookBackJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector)

		self.target_dto = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.next_dto = self.target_dto + timedelta(1)
		self.next_date = datetime.strftime(self.next_dto, '%Y-%m-%d')
		self.final_episode_df = None
		self.final_series_df = None
		self.final_channel_df = None
		self.final_platform_df = None
		self.tables = {
			'episode': 'ftp_viewership_lookback_episode',
			'series': 'ftp_viewership_lookback_series',
			'channel': 'ftp_viewership_lookback_channel',
			'platform': 'ftp_viewership_lookback_platform'
		}


	def load_episode_viewership(self):
		self.loggerv3.info("Loading Episode Viewership...")
		query = f""" 
					WITH converts as (
					    SELECT user_key
					    FROM warehouse.subscription
					    WHERE event_timestamp >= '{self.target_date}'
					      AND event_timestamp < '{self.next_date}'
					      AND subscription_event_type = 'FTP Conversion'
					    GROUP BY 1
					), viewership as (
					    SELECT vv.user_key,
					           dse.channel_title,
					           dse.series_id,
					           dse.season_id,
					           dse.episode_key,
					           sum(vv.active_seconds) as active_seconds
					    FROM warehouse.vod_viewership vv
					             INNER JOIN warehouse.dim_segment_episode dse ON vv.episode_key = dse.episode_key
					    WHERE vv.user_key in (SELECT user_key FROM converts)
					      AND vv.start_timestamp >= date_add('days', -13, '{self.target_date}')
					      AND vv.start_timestamp < '{self.target_date}'
					    GROUP BY 1, 2, 3, 4, 5
					    ORDER BY 6 desc
					), episode_rollup as (
					    SELECT series_id,
					           season_id,
					           episode_key,
					           (SELECT count(*) from converts)      as cohort_signups,
					           round(sum(active_seconds) / 60.0, 0) as minutes_viewed,
					           count(distinct user_key)             as viewers
					    FROM viewership
					    GROUP BY 1, 2, 3
					    ORDER BY 6 desc
					)
					SELECT
					    series_id,
					    season_id,
					    episode_key,
					    viewers,
					    cohort_signups
					FROM episode_rollup
					WHERE
					        viewers >= (cohort_signups * 0.05) 
					ORDER BY 5 desc, 1,2,3;
		"""
		results = self.db_connector.read_redshift(query)
		records = []
		for result in results:
			records.append({
				"conversion_date": self.target_date,
				"series_id": result[0],
				"season_id": result[1],
				"episode_key": result[2],
				"viewers": result[3],
				"cohort_signups": result[4]
			})
		self.final_episode_df = pd.DataFrame(records)


	def load_series_viewership(self):
		self.loggerv3.info("Loading Series Viewership...")
		query = f""" 
					WITH converts as (
					    SELECT user_key
					    FROM warehouse.subscription
					    WHERE event_timestamp >= '{self.target_date}'
					      AND event_timestamp < '{self.next_date}'
					      AND subscription_event_type = 'FTP Conversion'
					    GROUP BY 1
					), viewership as (
					    SELECT vv.user_key,
					           dse.series_id,
					           sum(vv.active_seconds) as active_seconds
					    FROM warehouse.vod_viewership vv
					             INNER JOIN warehouse.dim_segment_episode dse ON vv.episode_key = dse.episode_key
					    WHERE vv.user_key in (SELECT user_key FROM converts)
					      AND vv.start_timestamp >= date_add('days', -13, '{self.target_date}')
					      AND vv.start_timestamp < '{self.target_date}'
					    GROUP BY 1, 2
					    ORDER BY 3 desc
					), series_rollup as (
					    SELECT series_id,
					           (SELECT count(*) from converts)      as cohort_signups,
					           round(sum(active_seconds) / 60.0, 0) as minutes_viewed,
					           count(distinct user_key)             as viewers
					    FROM viewership
					    GROUP BY 1
					    ORDER BY 3 desc
					)
					SELECT
					    series_id,
					    viewers,
					    cohort_signups
					FROM series_rollup
					WHERE
					        viewers >= (cohort_signups * 0.05) 
					ORDER BY 3 desc, 1;
		"""
		results = self.db_connector.read_redshift(query)
		records = []
		for result in results:
			records.append({
				"conversion_date": self.target_date,
				"series_id": result[0],
				"viewers": result[1],
				"cohort_signups": result[2]
			})
		self.final_series_df = pd.DataFrame(records)


	def load_channel_viewership(self):
		self.loggerv3.info("Loading Channel Viewership...")
		query = f""" 
					WITH converts as (
					    SELECT user_key
					    FROM warehouse.subscription
					    WHERE event_timestamp >= '{self.target_date}'
					      AND event_timestamp < '{self.next_date}'
					      AND subscription_event_type = 'FTP Conversion'
					    GROUP BY 1
					), viewership as (
					    SELECT vv.user_key,
					           dse.channel_title,
					           sum(vv.active_seconds) as active_seconds
					    FROM warehouse.vod_viewership vv
					             INNER JOIN warehouse.dim_segment_episode dse ON vv.episode_key = dse.episode_key
					    WHERE vv.user_key in (SELECT user_key FROM converts)
					      AND vv.start_timestamp >= date_add('days', -13, '{self.target_date}')
					      AND vv.start_timestamp < '{self.target_date}'
					    GROUP BY 1, 2
					    ORDER BY 3 desc
					), series_rollup as (
					    SELECT channel_title,
					           (SELECT count(*) from converts)      as cohort_signups,
					           round(sum(active_seconds) / 60.0, 0) as minutes_viewed,
					           count(distinct user_key)             as viewers
					    FROM viewership
					    GROUP BY 1
					    ORDER BY 3 desc
					)
					SELECT
					    channel_title,
					    viewers,
					    cohort_signups
					FROM series_rollup
					WHERE
					        viewers >= (cohort_signups * 0.05) 
					ORDER BY 3 desc, 1;
		"""
		results = self.db_connector.read_redshift(query)
		records = []
		for result in results:
			records.append({
				"conversion_date": self.target_date,
				"channel_title": result[0],
				"viewers": result[1],
				"cohort_signups": result[2]
			})
		self.final_channel_df = pd.DataFrame(records)


	def load_platform_viewership(self):
		self.loggerv3.info("Loading Platform Viewership...")
		query = f""" 
					WITH converts as (
					    SELECT user_key
					    FROM warehouse.subscription
					    WHERE event_timestamp >= '{self.target_date}'
					      AND event_timestamp < '{self.next_date}'
					      AND subscription_event_type = 'FTP Conversion'
					    GROUP BY 1
					), viewership as (
					    SELECT user_key,
					           CASE
					               WHEN platform = 'iOS' THEN 'Mobile'
					               WHEN platform = 'Android' THEN 'Mobile'
					               WHEN platform LIKE 'lr_%%' THEN 'Living Room'
					               ELSE platform
					           END as platform,
					           sum(active_seconds) as active_seconds
					    FROM warehouse.vod_viewership
					    WHERE user_key in (SELECT user_key FROM converts)
					      AND start_timestamp >= date_add('days', -13, '{self.target_date}')
					      AND start_timestamp < '{self.target_date}'
					      AND max_position > 0
					    GROUP BY 1, 2
					    ORDER BY 3 desc
					), platform_rollup as (
					    SELECT platform,
					           (SELECT count(*) from converts)      as cohort_signups,
					           round(sum(active_seconds) / 60.0, 0) as minutes_viewed,
					           count(distinct user_key)             as viewers
					    FROM viewership
					    GROUP BY 1
					    ORDER BY 3 desc
					)
					SELECT
					    platform,
					    viewers,
					    cohort_signups
					FROM platform_rollup
					WHERE
					        viewers >= (cohort_signups * 0.05) 
					ORDER BY 1;
		"""
		results = self.db_connector.read_redshift(query)
		records = []
		for result in results:
			records.append({
				"conversion_date": self.target_date,
				"platform": result[0],
				"viewers": result[1],
				"cohort_signups": result[2]
			})
		self.final_platform_df = pd.DataFrame(records)


	def write_to_red_shift(self):
		self.loggerv3.info("Writing to Red Shift...")
		self.db_connector.write_to_sql(self.final_episode_df, self.tables['episode'], self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.write_to_sql(self.final_series_df, self.tables['series'], self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.write_to_sql(self.final_channel_df, self.tables['channel'], self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.write_to_sql(self.final_platform_df, self.tables['platform'], self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		for table in self.tables:
			self.db_connector.update_redshift_table_permissions(self.tables[table])


	def execute(self):
		self.loggerv3.info(f"Running FTP Viewership Lookback Job for {self.target_date}")
		self.load_episode_viewership()
		self.load_series_viewership()
		self.load_channel_viewership()
		self.load_platform_viewership()
		self.write_to_red_shift()
		self.loggerv3.success("All Processing Complete!")
