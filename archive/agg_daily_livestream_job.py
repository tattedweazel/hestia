import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggDailyLivestreamJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_daily_segment_livestream')

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
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f""" SELECT
    											count(distinct {key})
											FROM
											    warehouse.livestream_viewership
											WHERE
											    start_timestamp >= '{target_day}' AND
											    start_timestamp < '{next_day}' AND
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
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f""" SELECT
    											sum(active_seconds) / 60 / 60 as hours
											FROM
											    warehouse.livestream_viewership
											WHERE
											    start_timestamp >= '{target_day}' AND
											    start_timestamp < '{next_day}' AND
											    user_tier = '{sub_type}' AND
											    user_key {null_check}
											""")

		for result in results:
			if result[0] is not None:
				return int(result[0])
			else:
				return 0


	def process(self):
		self.loggerv3.info("Collecting Data")
		live_free_dav = self.get_daily_active_viewers('free')
		live_anon_dav = self.get_daily_active_viewers('anon')
		live_premium_dav = self.get_daily_active_viewers('premium')
		live_trial_dav = self.get_daily_active_viewers('trial')
		live_total_dav = live_free_dav + live_anon_dav + live_premium_dav + live_trial_dav
		live_free_hours = self.get_hours_viewed('free')
		live_anon_hours = self.get_hours_viewed('anon')
		live_premium_hours = self.get_hours_viewed('premium')
		live_trial_hours = self.get_hours_viewed('trial')
		live_total_hours = live_free_hours + live_anon_hours + live_premium_hours + live_trial_hours
		
		self.live_df = pd.DataFrame([{
			"date": self.formatted_dates['target'],
			"free_viewers": live_free_dav,
			"anon_viewers": live_anon_dav,
			"premium_viewers": live_premium_dav,
			"trial_viewers": live_trial_dav,
			"total_viewers": live_total_dav,
			"free_hours": live_free_hours,
			"anon_hours": live_anon_hours,
			"premium_hours": live_premium_hours,
			"trial_hours": live_trial_hours,
			"total_hours": live_total_hours
		}])


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.live_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Livestream for {self.formatted_dates['target']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
