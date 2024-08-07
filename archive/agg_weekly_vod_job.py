import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggWeeklyVodJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_weekly_segment_vod')

		self.Dater = DateHandler()
		self.end_day = self.target_date.replace('-','')
		self.next_day = self.Dater.find_next_day(self.end_day)
		self.start_day = self.Dater.find_x_days_ago(self.next_day, 7)
		self.formatted_dates = self.get_formatted_dates()


	def get_formatted_dates(self):
		return {
			"start": self.Dater.format_date(self.start_day),
			"end": self.Dater.format_date(self.end_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def get_active_viewers(self, sub_type='free'):
		if sub_type in ['trial', 'premium']:
			key = 'user_key'
			null_check = 'is not NULL'
		elif sub_type == 'anon':
			sub_type = 'free'
			key = 'anonymous_id'
			null_check = 'is NULL'
		else:
			key = 'anonymous_id'
			null_check = 'is not NULL'
		end_day = self.formatted_dates['next']
		start_day = self.formatted_dates['start']
		results = self.db_connector.read_redshift(f""" SELECT
    											count(distinct {key})
											FROM
											    warehouse.vod_viewership
											WHERE
											    start_timestamp >= '{start_day}' AND
											    start_timestamp < '{end_day}' AND
											    user_tier = '{sub_type}' AND
											    user_key {null_check}
											""")

		for result in results:
			return result[0]


	def get_total_views(self, sub_type):
		if sub_type == 'anon':
			sub_type = 'free'
			null_check = 'is NULL'
		else:
			null_check = 'is not NULL'
		end_day = self.formatted_dates['next']
		start_day = self.formatted_dates['start']
		results = self.db_connector.read_redshift(f""" SELECT
    											count(*)
											FROM
											    warehouse.vod_viewership
											WHERE
											    start_timestamp >= '{start_day}' AND
											    start_timestamp < '{end_day}' AND
											    user_tier = '{sub_type}' AND
											    user_key {null_check}
											""")

		for result in results:
			return result[0]


	def get_hours_viewed(self, sub_type):
		if sub_type == 'anon':
			sub_type = 'free'
			null_check = 'is NULL'
		else:
			null_check = 'is not NULL'
		end_day = self.formatted_dates['next']
		start_day = self.formatted_dates['start']
		results = self.db_connector.read_redshift(f""" SELECT
    											sum(active_seconds) / 60 / 60 as hours
											FROM
											    warehouse.vod_viewership
											WHERE
											    start_timestamp >= '{start_day}' AND
											    start_timestamp < '{end_day}' AND
											    user_tier = '{sub_type}' AND
											    user_key {null_check}
											""")

		for result in results:
			return int(result[0])


	def process(self):
		self.loggerv3.info("Collecting Data")
		vod_free_dav = self.get_active_viewers('free')
		vod_anon_dav = self.get_active_viewers('anon')
		vod_premium_dav = self.get_active_viewers('premium')
		vod_trial_dav = self.get_active_viewers('trial')
		vod_total_dav = vod_free_dav + vod_anon_dav + vod_premium_dav + vod_trial_dav
		vod_free_views = self.get_total_views('free')
		vod_anon_views = self.get_total_views('anon')
		vod_premium_views = self.get_total_views('premium')
		vod_trial_views = self.get_total_views('trial')
		vod_total_views = vod_free_views + vod_anon_views + vod_premium_views + vod_trial_views
		vod_free_hours = self.get_hours_viewed('free')
		vod_anon_hours = self.get_hours_viewed('anon')
		vod_premium_hours = self.get_hours_viewed('premium')
		vod_trial_hours = self.get_hours_viewed('trial')
		vod_total_hours = vod_free_hours + vod_anon_hours + vod_premium_hours + vod_trial_hours
		
		self.vod_df = pd.DataFrame([{
			"week_ending": self.formatted_dates['end'],
			"week_starting": self.formatted_dates['start'],
			"free_viewers": vod_free_dav,
			"anon_viewers": vod_anon_dav,
			"premium_viewers": vod_premium_dav,
			"trial_viewers": vod_trial_dav,
			"total_viewers": vod_total_dav,
			"free_views": vod_free_views,
			"anon_views": vod_anon_views,
			"premium_views": vod_premium_views,
			"trial_views": vod_trial_views,
			"total_views": vod_total_views,
			"free_hours": vod_free_hours,
			"anon_hours": vod_anon_hours,
			"premium_hours": vod_premium_hours,
			"trial_hours": vod_trial_hours,
			"total_hours": vod_total_hours
		}])


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.vod_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Weekly VOD for {self.formatted_dates['end']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
