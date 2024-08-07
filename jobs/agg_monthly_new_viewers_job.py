import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggMonthlyNewViewersJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname=__name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_monthly_new_viewers')

		self.Dater = DateHandler()
		self.start_day = self.Dater.get_start_of_month(self.target_date.replace('-',''))
		self.end_day = self.Dater.get_end_of_month(self.start_day)
		self.next_day = self.Dater.find_next_day(self.end_day)
		self.formatted_dates = self.get_formatted_dates()
		self.new_viewers = []
		self.final_df = None


	def get_formatted_dates(self):
		return {
			"start": self.Dater.format_date(self.start_day),
			"end": self.Dater.format_date(self.end_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def get_new_viewers(self):
		target_day = self.formatted_dates['start']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f"""
														SELECT
														    user_key
														FROM warehouse.vod_viewership
														WHERE
														    start_timestamp >= '{target_day}' AND
														    start_timestamp < '{next_day}' AND
														    user_tier != 'grant' AND
														    user_key is not null AND
														    user_key not in (
														        SELECT vv2.user_key
														        FROM warehouse.vod_viewership vv2
														        WHERE vv2.start_timestamp < '{target_day}'
														        AND vv2.user_key is not null
														        GROUP BY 1
														    )
														GROUP BY 1;
													""")

		for result in results:
			self.new_viewers.append(result[0])


	def build_dataframe(self):
		self.final_df = pd.DataFrame([{
				"viewership_month": self.formatted_dates['start'],
				"total": len(self.new_viewers)
			}])


	def process(self):
		self.get_new_viewers()
		self.build_dataframe()


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Monthly New Viewers for {self.formatted_dates['start']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
