import math
import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class TrialAttributionV2Job(EtlJobV3):

		def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
			super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'trial_attribution_v2')

			self.Dater = DateHandler()
			self.trial_start_date = self.target_date.replace('-','')
			self.trial_end_date = self.Dater.find_x_days_ahead(self.trial_start_date, 6)
			self.next_date = self.Dater.find_next_day(self.trial_start_date)
			self.query_cap_date = self.Dater.find_next_day(self.trial_end_date)
			self.formatted_dates = self.get_formatted_dates()

			self.trial_users = []

			self.total_viewed_seconds = 0
			self.series_attributions = {}
			self.channel_attributions = {}
			self.platform_attributions = {}

			self.series_attributions_df = None
			self.channel_attributions_df = None
			self.platform_attributions_df = None


		def get_formatted_dates(self):
			return {
				"trial_start_date": self.Dater.format_date(self.trial_start_date),
				"next_date": self.Dater.format_date(self.next_date),
				"trial_end_date": self.Dater.format_date(self.trial_end_date),
				"query_cap_date": self.Dater.format_date(self.query_cap_date)
			}


		def load_trial_starts(self):
			self.loggerv3.info(f"Loading trials for {self.formatted_dates['trial_start_date']} ending {self.formatted_dates['trial_end_date']}")
			results = self.db_connector.read_redshift(f"""
												SELECT
													sub.user_key
												FROM warehouse.subscription sub
												WHERE sub.subscription_event_type = 'Trial' 
												AND convert_timezone('US/Central', sub.start_timestamp) >= '{self.formatted_dates['trial_start_date']}'
												AND convert_timezone('US/Central', sub.start_timestamp) < '{self.formatted_dates['next_date']}'
												""")
			for result in results:
				trialer = {
					"user_id": result[0],
					"views": []
				}
				if trialer not in self.trial_users:
					self.trial_users.append(trialer)


		def populate_views(self):
			self.loggerv3.info(f"Populating views for {len(self.trial_users)} users")

			for record in self.trial_users:
				record['views'] = self.load_views(record['user_id'])


		def load_views(self, user_key):
			results = self.db_connector.read_redshift(f""" WITH trial_watches AS (
													    SELECT
													        vv.user_key,
													        sum(vv.active_seconds) as time_watched
													    FROM
													        warehouse.vod_viewership vv
													    WHERE
													        convert_timezone('US/Central', vv.start_timestamp) >= '{self.formatted_dates['trial_start_date']}' AND
													        convert_timezone('US/Central', vv.start_timestamp) < '{self.formatted_dates['query_cap_date']}' AND
													        vv.user_key = {user_key}
													    GROUP BY 1
													)
													SELECT
													    convert_timezone('US/Central', vv.start_timestamp) as watch_date,
													    vv.user_key,
													    vv.platform,
													    dse.channel_title as channel,
													    dse.series_title as series,
													    dse.season_title as season,
													    dse.episode_title as episode,
													    dse.episode_key as episode_key,
													    sum(vv.active_seconds) as seconds_watched,
													    tw.time_watched as total_seconds_watched_during_trial,
													    trunc(round((cast(sum(vv.active_seconds / cast(tw.time_watched as float)) as float) * 100),3),3) as pct_of_trial_view_time
													FROM
													    warehouse.dim_segment_episode dse
													LEFT JOIN
													    warehouse.vod_viewership vv
													ON
													    vv.episode_key = dse.episode_key
													LEFT JOIN
													    trial_watches tw
													ON
													    tw.user_key = vv.user_key
													WHERE
													    convert_timezone('US/Central', vv.start_timestamp) >= '{self.formatted_dates['trial_start_date']}' AND
													    convert_timezone('US/Central', vv.start_timestamp) < '{self.formatted_dates['query_cap_date']}' AND
													    vv.user_key = {user_key}
													GROUP BY 1,2,3,4,5,6,7,8,10
													ORDER BY 1;
											""")
			views = []
			for result in results:
				views.append({
					"watch_date": result[0],
					"user_key": result[1],
					"platform": result[2],
					"channel": result[3],
					"series": result[4],
					"season": result[5],
					"episode": result[6],
					"episode_key": result[7],
					"seconds_watched": result[8],
					"seconds_watched_during_trial": result[9],
					"pct_of_trial_view_time": result[10]
				})
			return views


		def determine_total_watch_time(self):
			for record in self.trial_users:
				for view in record['views']:
					self.total_viewed_seconds += view['seconds_watched']


		def calculate_series_attributions(self):
			self.loggerv3.info("Calculating Series Attributions...")
			for record in self.trial_users:
				for view in record['views']:
					if view['series'] not in self.series_attributions:
						self.series_attributions[view['series']] = {
							"seconds_viewed": view['seconds_watched']
						}
					else:
						self.series_attributions[view['series']]['seconds_viewed'] += view['seconds_watched']

			records = []
			for series in self.series_attributions:
				self.series_attributions[series]['pct_of_total'] = round((self.series_attributions[series]['seconds_viewed'] / self.total_viewed_seconds) * 100, 3)
				cohort_size = len(self.trial_users)
				pct_of_total_watchtime = self.series_attributions[series]['pct_of_total']
				raw_representative_watchers = cohort_size * (pct_of_total_watchtime/100)
				representative_watchers = math.ceil(raw_representative_watchers)
				if pct_of_total_watchtime > 1:
					record = {
						"trial_start_date": self.formatted_dates['trial_start_date'],
						"cohort_size": cohort_size,
						"item_type": 'Series',
						"item_name": series,
						"pct_of_total_watchtime": pct_of_total_watchtime,
						"representative_watchers": representative_watchers
					}
					records.append(record)
			self.series_attributions_df = pd.DataFrame(records)


		def calculate_channel_attributions(self):
			self.loggerv3.info("Calculating Channel Attributions...")
			for record in self.trial_users:
				for view in record['views']:
					if view['channel'] not in self.channel_attributions:
						self.channel_attributions[view['channel']] = {
							"seconds_viewed": view['seconds_watched']
						}
					else:
						self.channel_attributions[view['channel']]['seconds_viewed'] += view['seconds_watched']

			records = []
			for channel in self.channel_attributions:
				self.channel_attributions[channel]['pct_of_total'] = round((self.channel_attributions[channel]['seconds_viewed'] / self.total_viewed_seconds) * 100, 3)
				cohort_size = len(self.trial_users)
				pct_of_total_watchtime = self.channel_attributions[channel]['pct_of_total']
				raw_representative_watchers = cohort_size * (pct_of_total_watchtime/100)
				representative_watchers = math.ceil(raw_representative_watchers)
				if pct_of_total_watchtime > 1:
					record = {
						"trial_start_date": self.formatted_dates['trial_start_date'],
						"cohort_size": cohort_size,
						"item_type": 'Channel',
						"item_name": channel,
						"pct_of_total_watchtime": pct_of_total_watchtime,
						"representative_watchers": representative_watchers
					}
					records.append(record)
			self.channel_attributions_df = pd.DataFrame(records)


		def calculate_platform_attributions(self):
			self.loggerv3.info("Calculating Platform Attributions...")
			for record in self.trial_users:
				for view in record['views']:
					if view['platform'] not in self.platform_attributions:
						self.platform_attributions[view['platform']] = {
							"seconds_viewed": view['seconds_watched']
						}
					else:
						self.platform_attributions[view['platform']]['seconds_viewed'] += view['seconds_watched']

			records = []
			for platform in self.platform_attributions:
				self.platform_attributions[platform]['pct_of_total'] = round((self.platform_attributions[platform]['seconds_viewed'] / self.total_viewed_seconds) * 100, 3)
				cohort_size = len(self.trial_users)
				pct_of_total_watchtime = self.platform_attributions[platform]['pct_of_total']
				raw_representative_watchers = cohort_size * (pct_of_total_watchtime/100)
				representative_watchers = math.ceil(raw_representative_watchers)
				record = {
					"trial_start_date": self.formatted_dates['trial_start_date'],
					"cohort_size": cohort_size,
					"item_type": 'Platform',
					"item_name": platform,
					"pct_of_total_watchtime": pct_of_total_watchtime,
					"representative_watchers": representative_watchers
				}
				records.append(record)
			self.platform_attributions_df = pd.DataFrame(records)


		def create_final_dataframe(self):
			dfs = [self.series_attributions_df, self.platform_attributions_df, self.channel_attributions_df]
			self.final_dataframe = pd.concat(dfs)


		def write_all_results_to_redshift(self):
			self.loggerv3.info("Writing results to Red Shift")
			column_order = [
				'trial_start_date',
				'cohort_size',
				'item_type',
				'item_name',
				'pct_of_total_watchtime',
				'representative_watchers'
			]
			self.db_connector.write_to_sql(
				self.final_dataframe[column_order], f"{self.table_name}", self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append'
			)
			self.db_connector.update_redshift_table_permissions(self.table_name)


		def execute(self):
			self.loggerv3.info("Running Trial Attribution V2")
			self.load_trial_starts()
			self.populate_views()
			self.determine_total_watch_time()
			self.calculate_series_attributions()
			self.calculate_channel_attributions()
			self.calculate_platform_attributions()
			self.create_final_dataframe()
			self.write_all_results_to_redshift()
			self.loggerv3.success("All Processing Complete!")
