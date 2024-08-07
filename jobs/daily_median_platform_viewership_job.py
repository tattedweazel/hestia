import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class DailyMedianPlatformViewershipJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'daily_median_platform_viewership')

		self.target_dt = datetime.strptime(f"{self.target_date} 07:00", '%Y-%m-%d %H:%M')
		self.next_date = datetime.strftime(self.target_dt + timedelta(1), '%Y-%m-%d')

		self.final_dataframe = None


	def load_data(self):
		self.last_query = f""" with user_views as (
								    SELECT user_key,
								           platform,
								           sum(active_seconds) as active_seconds
								    FROM warehouse.vod_viewership
								    WHERE start_timestamp >= '{self.target_date}'
								      AND start_timestamp < '{self.next_date}'
								    GROUP BY 1,2
								)
								SELECT
								    user_views.platform,
								    median(user_views.active_seconds) / 3600.0 as active_hours
								FROM user_views
								GROUP BY 1
								ORDER BY 2 desc;
							"""
		results = self.db_connector.read_redshift(self.last_query)
		records = []
		for result in results:
			records.append({
				"run_date": self.target_dt,
				"platform": result[0],
				"median_hours": float(result[1])
				})
		self.records = records


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		final_dataframe = pd.DataFrame(self.records)
		self.db_connector.write_to_sql(final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Daily Median Platform Viewership for {self.target_date}")
		self.load_data()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
