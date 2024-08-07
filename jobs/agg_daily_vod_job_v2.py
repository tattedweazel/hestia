import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggDailyVodJobV2(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname=__name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_daily_vod')

		self.Dater = DateHandler()
		self.target_day = self.target_date.replace('-','')
		self.next_day = self.Dater.find_next_day(self.target_day)
		self.formatted_dates = self.get_formatted_dates()


	def get_formatted_dates(self):
		return {
			"target": self.Dater.format_date(self.target_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def get_daily_active_viewers(self, sub_type='free'):
		if sub_type == 'anon':
			key = 'anonymous_id'
		else:
			key = 'user_key'
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f""" SELECT
    											count(distinct {key})
											FROM
											    warehouse.vod_viewership
											WHERE
											    start_timestamp >= '{target_day}' AND
											    start_timestamp < '{next_day}' AND
											    user_tier = '{sub_type}'
											""")

		for result in results:
			return result[0]


	def get_total_views(self, sub_type):
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f""" SELECT
    											count(*)
											FROM
											    warehouse.vod_viewership
											WHERE
											    start_timestamp >= '{target_day}' AND
											    start_timestamp < '{next_day}' AND
											    user_tier = '{sub_type}'
											""")

		for result in results:
			return result[0]


	def get_hours_viewed(self, sub_type):
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f""" SELECT
    											sum(active_seconds) / 60 / 60 as hours
											FROM
											    warehouse.vod_viewership
											WHERE
											    start_timestamp >= '{target_day}' AND
											    start_timestamp < '{next_day}' AND
											    user_tier = '{sub_type}'
											""")

		for result in results:
			if result[0]: ## Make sure there are results before trying to convert None to Int
				return int(result[0])
			return 0


	def process(self):
		self.loggerv3.info("Collecting Data")
		vod_free_dav = self.get_daily_active_viewers('free')
		vod_anon_dav = self.get_daily_active_viewers('anon')
		vod_premium_dav = self.get_daily_active_viewers('premium')
		vod_trial_dav = self.get_daily_active_viewers('trial')
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
			"date": self.formatted_dates['target'],
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
		self.loggerv3.start(f"Running VOD for {self.formatted_dates['target']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
