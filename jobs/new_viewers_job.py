import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class NewViewersJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'new_viewers')

		self.target_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.final_df = None


	def load_new_viewers(self):
		self.loggerv3.info("Loading New Viewers...")
		query = f""" 
					with last_7_days as (
					    select distinct user_key
					    from warehouse.vod_viewership
					    where user_key is not null
					      AND user_tier in ('premium', 'trial', 'free')
					      AND start_timestamp >= dateadd('days', -6, '{self.target_date}')
					      AND start_timestamp < dateadd('days', 1, '{self.target_date}')
					), previous_viewers as (
					    select distinct user_key
					    from warehouse.vod_viewership
					    where user_key is not null
					      AND user_tier in ('premium', 'trial', 'free')
					      AND start_timestamp < dateadd('days', -6, '{self.target_date}')
					) select
					    count(*) as new_users
					  from last_7_days
					  where user_key not in (select user_key from previous_viewers);
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			record = {
				"run_date": self.target_date,  # Week Ending Date
				"total": result[0]
			}
			self.final_df = pd.DataFrame([record])


	def write_results_to_redshift(self):
		self.loggerv3.info("Writing to Redshift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running New Viewers Job for {self.target_date}")
		self.load_new_viewers()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
