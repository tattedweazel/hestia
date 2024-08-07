import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class ChannelVelocityJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'segment_channel_velocity')
		
		self.week_ending = target_date
		self.week_ending_dt = datetime.strptime(self.week_ending, '%Y-%m-%d')
		self.week_starting = datetime.strftime(self.week_ending_dt - timedelta(6), '%Y-%m-%d')
		self.query_date_cap = datetime.strftime(self.week_ending_dt + timedelta(1), '%Y-%m-%d')

		self.final_dataframe = None


	def load_data(self):
		self.loggerv3.info(f"Loading View data...")
		results = self.db_connector.read_redshift(f""" 
												WITH trial_views AS (
												    SELECT
												        de.channel_title,
												        COUNT(distinct anonymous_id) as unique_viewers,
												        AVG(vv.active_seconds) as avg_time_watched,
												        COUNT(*) as total_views
												FROM
												    warehouse.vod_viewership vv
												INNER JOIN
												    warehouse.dim_segment_episode de
												ON
												    de.episode_key = vv.episode_key
												WHERE
												    vv.user_tier = 'trial' AND
												    vv.start_timestamp >= '{self.week_starting}' AND
												    vv.start_timestamp < '{self.query_date_cap}'
												GROUP BY 1
												),
												premium_views AS (
												    SELECT
												        de.channel_title,
												        COUNT(distinct anonymous_id) as unique_viewers,
												        AVG(vv.active_seconds) as avg_time_watched,
												        COUNT(*) as total_views
												FROM
												    warehouse.vod_viewership vv
												INNER JOIN
												    warehouse.dim_segment_episode de
												ON
												    de.episode_key = vv.episode_key
												WHERE
												    vv.user_tier = 'premium' AND
												    vv.start_timestamp >= '{self.week_starting}' AND
												    vv.start_timestamp < '{self.query_date_cap}'
												GROUP BY 1
												),
												free_views AS (
												    SELECT
												        de.channel_title,
												        COUNT(distinct anonymous_id) as unique_viewers,
												        AVG(vv.active_seconds) as avg_time_watched,
												        COUNT(*) as total_views
												FROM
												    warehouse.vod_viewership vv
												INNER JOIN
												    warehouse.dim_segment_episode de
												ON
												    de.episode_key = vv.episode_key
												WHERE
												    vv.user_tier = 'free' AND
												    vv.user_key is not null AND
												    vv.start_timestamp >= '{self.week_starting}' AND
												    vv.start_timestamp < '{self.query_date_cap}'
												GROUP BY 1
												),
												anon_views AS (
												    SELECT de.channel_title,
												           COUNT(distinct anonymous_id) as unique_viewers,
												           AVG(vv.active_seconds)       as avg_time_watched,
												           COUNT(*)                     as total_views
												    FROM 
												    	warehouse.vod_viewership vv
										            LEFT JOIN
										             	warehouse.dim_segment_episode de
												    ON
												    	de.episode_key = vv.episode_key
												    WHERE vv.user_tier = 'free'
												      AND vv.user_key is null
												      AND vv.start_timestamp >= '{self.week_starting}'
												      AND vv.start_timestamp < '{self.query_date_cap}'
												    GROUP BY 1
												),
												published_episodes AS (
												    SELECT
												        channel_title
												    FROM
												        warehouse.dim_segment_episode
												    WHERE
												        air_date BETWEEN '{self.week_starting}' and '{self.query_date_cap}'
												    GROUP BY 1 ORDER BY 1
												)
												SELECT
												    '{self.week_starting}' as week_start,
												    '{self.week_ending}' as week_end,
												    de.channel_title,
												    CASE WHEN tv.unique_viewers IS NULL THEN 0 ELSE tv.unique_viewers END AS trial_viewers,
												    CASE WHEN tv.total_views IS NULL THEN 0 ELSE tv.total_views END AS trial_views,
												    CASE WHEN tv.avg_time_watched IS NULL THEN 0 ELSE tv.avg_time_watched END AS trial_avg_seconds_watched,
												    CASE WHEN pv.unique_viewers IS NULL THEN 0 ELSE pv.unique_viewers END AS premium_viewers,
												    CASE WHEN pv.total_views IS NULL THEN 0 ELSE pv.total_views END AS premium_views,
												    CASE WHEN pv.avg_time_watched IS NULL THEN 0 ELSE pv.avg_time_watched END AS premium_avg_seconds_watched,
												    CASE WHEN fv.unique_viewers IS NULL THEN 0 ELSE fv.unique_viewers END AS free_viewers,
												    CASE WHEN fv.total_views IS NULL THEN 0 ELSE fv.total_views END AS free_views,
												    CASE WHEN fv.avg_time_watched IS NULL THEN 0 ELSE fv.avg_time_watched END AS free_avg_seconds_watched,
												    CASE WHEN av.unique_viewers IS NULL THEN 0 ELSE av.unique_viewers END AS anon_viewers,
												    CASE WHEN av.total_views IS NULL THEN 0 ELSE av.total_views END AS anon_views,
												    CASE WHEN av.avg_time_watched IS NULL THEN 0 ELSE av.avg_time_watched END AS anon_avg_seconds_watched,
												    count(*) as total_views,
												    count(distinct vv.anonymous_id) as total_unique_viewers,
												    AVG(vv.active_seconds) as avg_seconds_watched,
												    CASE WHEN pe.channel_title IS NULL THEN 0 ELSE 1 END as posted_within_period,
												    max(de.air_date) as most_recent_post
												FROM
												    warehouse.vod_viewership vv
												INNER JOIN
												    warehouse.dim_segment_episode de
												ON
												    de.episode_key = vv.episode_key
												LEFT JOIN
												    trial_views tv
												ON
												    tv.channel_title = de.channel_title
												LEFT JOIN
												    premium_views pv
												ON
												    pv.channel_title = de.channel_title
												LEFT JOIN
												    free_views fv
												ON
												    fv.channel_title = de.channel_title
												LEFT JOIN
												    anon_views av
												ON
												    av.channel_title = de.channel_title
												LEFT JOIN
												    published_episodes pe
												ON
												    pe.channel_title = de.channel_title
												WHERE
												    vv.start_timestamp >= '{self.week_starting}' AND
												    vv.start_timestamp < '{self.query_date_cap}' AND
												    vv.user_tier != 'grant'
												GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,19
												ORDER BY 3;""")
		records = []
		for result in results:
			record = {
				"week_start": result[0],
				"week_end": result[1],
				"channel_title": result[2],
				"trial_viewers": result[3],
				"trial_views": result[4],
				"trial_avg_seconds_watched": result[5],
				"premium_viewers": result[6],
				"premium_views": result[7],
				"premium_avg_seconds_watched": result[8],
				"free_viewers": result[9],
				"free_views": result[10],
				"free_avg_seconds_watched": result[11],
				"anon_viewers": result[12],
				"anon_views": result[13],
				"anon_avg_seconds_watched": result[14],
				"total_views": result[15],
				"total_unique_viewers": result[16],
				"avg_seconds_watched": result[17],
				"posted_within_period": result[18],
				"most_recent_post": result[19]
			}

			records.append(record)
		self.final_dataframe = pd.DataFrame(records)


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Channel Velocity for {self.week_starting} - {self.week_ending}")
		self.load_data()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
