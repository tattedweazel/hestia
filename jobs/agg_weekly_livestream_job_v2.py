import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggWeeklyLivestreamJobV2(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_weekly_livestream')

		self.Dater = DateHandler()
		self.target_day = self.target_date.replace('-','')
		self.starting = self.Dater.find_x_days_ago(self.target_day, 6)
		self.next_day = self.Dater.find_next_day(self.target_day)
		self.formatted_dates = self.get_formatted_dates()


	def get_formatted_dates(self):
		return {
			"starting": self.Dater.format_date(self.starting),
			"ending": self.Dater.format_date(self.target_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def get_totals(self):
		starting_day = self.formatted_dates['starting']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f""" SELECT
														    user_tier,
														    cast(sum(active_seconds) / 3600.0 as decimal(6,0)) as hours,
														    count(*) as viewers
														FROM warehouse.livestream_viewership
														WHERE
														    start_timestamp >= '{starting_day}' AND
														    start_timestamp < '{next_day}'
														GROUP BY 1;
											""")
		totals = {
			"total": {
				"hours": 0,
				"viewers": 0
			},
			"premium": {
				"hours": 0,
				"viewers": 0
			},
			"trial": {
				"hours": 0,
				"viewers": 0
			},
			"grant": {
				"hours": 0,
				"viewers": 0
			},
			"anon": {
				"hours": 0,
				"viewers": 0
			},
			"free": {
				"hours": 0,
				"viewers": 0
			},
		}
		for result in results:
			totals[result[0]] = {
				"hours": result[1],
				"viewers": result[2]
			}
			totals['total']['hours'] += result[1]
			totals['total']['viewers'] += result[2]
		return totals


	def process(self):
		self.loggerv3.info("Collecting Data")
		totals = self.get_totals()
		self.live_df = pd.DataFrame([{
			"week_starting": self.formatted_dates['starting'],
			"week_ending": self.formatted_dates['ending'],
			"free_viewers": totals['free']['viewers'],
			"anon_viewers": totals['anon']['viewers'],
			"premium_viewers": totals['premium']['viewers'],
			"trial_viewers": totals['trial']['viewers'],
			"grant_viewers": totals['grant']['viewers'],
			"total_viewers": totals['total']['viewers'],
			"free_hours": totals['free']['hours'],
			"anon_hours": totals['anon']['hours'],
			"premium_hours": totals['premium']['hours'],
			"trial_hours": totals['trial']['hours'],
			"grant_hours": totals['grant']['hours'],
			"total_hours": totals['total']['hours']
		}])


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.live_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Weekly Livestream for {self.formatted_dates['starting']} - {self.formatted_dates['ending']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
