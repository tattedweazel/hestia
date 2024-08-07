import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater


class CancellationRequestLookbacksJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'cancellation_request_lookbacks')

		self.Dater = Dater()
		self.target_dt = self.target_date.replace('-','')
		self.next_dt = self.Dater.find_next_day(self.target_dt)
		self.thirty_days_ago_dt = self.Dater.find_x_days_ago(self.target_dt, 30)
		self.sixty_days_ago_dt = self.Dater.find_x_days_ago(self.target_dt, 60)
		self.ninety_days_ago_dt = self.Dater.find_x_days_ago(self.target_dt, 90)
		self.formatted_dates = self.get_formatted_dates()

		self.records = []


	def get_formatted_dates(self):
		return {
			"target_date": self.Dater.format_date(self.target_dt),
			"next_date": self.Dater.format_date(self.next_dt),
			"thirty_days_ago": self.Dater.format_date(self.thirty_days_ago_dt),
			"sixty_days_ago": self.Dater.format_date(self.sixty_days_ago_dt),
			"ninety_days_ago": self.Dater.format_date(self.ninety_days_ago_dt)
		}


	def load_data(self):
		self.last_query = f""" 
			with cancelled_users as (
			    SELECT
			        user_key,
			        subscription_id,
			        membership_plan,
			        event_timestamp as cancellation_request_date,
			        start_timestamp,
			        end_timestamp
			    FROM warehouse.subscription
			    WHERE
			        start_timestamp >= '{self.formatted_dates['target_date']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}' AND
			        subscription_event_type = 'Paid Cancel Requested' AND
			        user_key is not NULL
			), vod_hours_viewed_30 as (
			    SELECT
			        vv.user_key as user_key,
			        sum(vv.active_seconds) / 3600.0 as hours_watched
			    FROM warehouse.vod_viewership vv
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['thirty_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			), vod_hours_viewed_60 as (
			    SELECT
			        vv.user_key as user_key,
			        sum(vv.active_seconds) / 3600.0 as hours_watched
			    FROM warehouse.vod_viewership vv
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['sixty_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			), vod_hours_viewed_90 as (
			    SELECT
			        vv.user_key as user_key,
			        sum(vv.active_seconds) / 3600.0 as hours_watched
			    FROM warehouse.vod_viewership vv
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['ninety_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			), live_hours_viewed_30 as (
			    SELECT
			        du.user_key as user_key,
			        sum(lv.active_seconds) / 3600.0 as hours_watched
			    FROM warehouse.livestream_viewership lv
			    INNER JOIN warehouse.dim_user du ON du.user_id = lv.user_id
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['thirty_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			), live_hours_viewed_60 as (
			    SELECT
			        du.user_key as user_key,
			        sum(lv.active_seconds) / 3600.0 as hours_watched
			    FROM warehouse.livestream_viewership lv
			    INNER JOIN warehouse.dim_user du ON du.user_id = lv.user_id
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['sixty_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			), live_hours_viewed_90 as (
			    SELECT
			        du.user_key as user_key,
			        sum(lv.active_seconds) / 3600.0 as hours_watched
			    FROM warehouse.livestream_viewership lv
			    INNER JOIN warehouse.dim_user du ON du.user_id = lv.user_id
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['ninety_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			), series_watched_30 as (
			    SELECT
			        vv.user_key,
			        count(distinct dse.series_title) as series_watched
			    FROM warehouse.vod_viewership vv
			    LEFT JOIN warehouse.dim_segment_episode dse
			    ON dse.episode_key = vv.episode_key
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['thirty_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			), series_watched_60 as (
			    SELECT
			        vv.user_key,
			        count(distinct dse.series_title) as series_watched
			    FROM warehouse.vod_viewership vv
			    LEFT JOIN warehouse.dim_segment_episode dse
			    ON dse.episode_key = vv.episode_key
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['sixty_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			), series_watched_90 as (
			    SELECT
			        vv.user_key,
			        count(distinct dse.series_title) as series_watched
			    FROM warehouse.vod_viewership vv
			    LEFT JOIN warehouse.dim_segment_episode dse
			    ON dse.episode_key = vv.episode_key
			    WHERE
			        user_key in (SELECT user_key FROM cancelled_users) AND
			        start_timestamp >= '{self.formatted_dates['ninety_days_ago']}' AND
			        start_timestamp < '{self.formatted_dates['next_date']}'
			    GROUP BY 1
			)
			SELECT
				cu.user_key,
			    cu.cancellation_request_date,
			    cu.start_timestamp,
			    cu.end_timestamp,
			    cu.membership_plan,
			    vhv30.hours_watched as vod_hours_watched_30_days,
			    vhv60.hours_watched as vod_hours_watched_60_days,
			    vhv90.hours_watched as vod_hours_watched_90_days,
			    lhv30.hours_watched as live_hours_watched_30_days,
			    lhv60.hours_watched as live_hours_watched_60_days,
			    lhv90.hours_watched as live_hours_watched_90_days,
			    sw30.series_watched as series_watched_30_days,
			    sw60.series_watched as series_watched_60_days,
			    sw90.series_watched as series_watched_90_days,
			    du.user_id as user_uuid
			FROM cancelled_users cu
			INNER JOIN warehouse.dim_user du
			ON du.user_key = cu.user_key
			LEFT JOIN vod_hours_viewed_30 vhv30
			ON vhv30.user_key = cu.user_key
			LEFT JOIN vod_hours_viewed_60 vhv60
			ON vhv60.user_key = cu.user_key
			LEFT JOIN vod_hours_viewed_90 vhv90
			ON vhv90.user_key = cu.user_key
			LEFT JOIN live_hours_viewed_30 lhv30
			ON lhv30.user_key = cu.user_key
			LEFT JOIN live_hours_viewed_60 lhv60
			ON lhv60.user_key = cu.user_key
			LEFT JOIN live_hours_viewed_90 lhv90
			ON lhv90.user_key = cu.user_key
			LEFT JOIN series_watched_30 sw30
			ON sw30.user_key = cu.user_key
			LEFT JOIN series_watched_60 sw60
			ON sw60.user_key = cu.user_key
			LEFT JOIN series_watched_90 sw90
			ON sw90.user_key = cu.user_key
		"""
		results = self.db_connector.read_redshift(self.last_query)
		for result in results:
			record = {
				'user_key': result[0],
				'cancellation_request_date': result[1],
				'current_period_start_date': result[2],
				'current_period_end_date': result[3],
				'membership_plan': result[4],
				'vod_hours_watched_30_days': result[5] if result[5] is not None else 0.0,
				'vod_hours_watched_60_days': result[6] if result[6] is not None else 0.0,
				'vod_hours_watched_90_days': result[7] if result[7] is not None else 0.0,
				'live_hours_watched_30_days': result[8] if result[8] is not None else 0.0,
				'live_hours_watched_60_days': result[9] if result[9] is not None else 0.0,
				'live_hours_watched_90_days': result[10] if result[10] is not None else 0.0,
				'series_watched_30_days': result[11] if result[11] is not None else 0,
				'series_watched_60_days': result[12] if result[12] is not None else 0,
				'series_watched_90_days': result[13] if result[13] is not None else 0
			}
			record['total_sub_days'] = abs((record['current_period_start_date'] - record['current_period_end_date']).days)
			record['days_into_sub'] = abs((record['cancellation_request_date'] - record['current_period_start_date']).days)
			record['days_remaining'] = abs((record['current_period_end_date'] - record['cancellation_request_date']).days)
			self.records.append(record)


	def build_final_dataframe(self):
		self.final_dataframe = pd.DataFrame(self.records)


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Cancellation Request Lookbacks for {self.target_date}")
		self.load_data()
		self.build_final_dataframe()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
