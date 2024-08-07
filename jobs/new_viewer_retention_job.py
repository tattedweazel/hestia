import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class NewViewerRetentionJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'new_viewer_retention')

		self.target_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.final_df = None


	def load_new_viewers(self):
		self.loggerv3.info("Loading Viewers...")
		query = f""" 
			with week_one_viewers as (
			    select distinct user_key
			    from warehouse.vod_viewership
			    where user_key is not null
			      and user_tier in ('premium', 'trial', 'free')
			      and start_timestamp >= dateadd('days', -14, '{self.target_date}')
			      and start_timestamp < dateadd('days', -7, '{self.target_date}')
			), week_two_viewers as (
			    select distinct user_key
			    from warehouse.vod_viewership
			    where user_key is not null
			      and user_tier in ('premium', 'trial', 'free')
			      and start_timestamp >= dateadd('days', -7, '{self.target_date}')
			      and start_timestamp < '{self.target_date}'
			), past_viewers as (
			    select distinct user_key
			    from warehouse.vod_viewership
			    where user_key is not null
			      and user_tier in ('premium', 'trial', 'free')
			      and start_timestamp < dateadd('days', -14, '{self.target_date}')
			), new_registered as (
			    select count(user_key) as new_registered
			    from week_one_viewers
			    where user_key not in (select user_key from past_viewers)
			), retained as (
			    select
			    count(user_key) as retained
			    from week_one_viewers
			    where user_key in (select user_key from week_two_viewers)
			    and user_key not in (select user_key from past_viewers)
			)
			    select
			    dateadd('days', -1, '{self.target_date} 08:00') as week_ending,
			    (select new_registered from new_registered) as new_registered,
			    (select retained from retained) as retained,
			    (select retained from retained) / ((select new_registered from new_registered) * 1.0) as retention_rate;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			record = {
				"run_date": result[0],
				"new_registered": result[1],
				"retained": result[2],
				"retention_rate": result[3]
			}
			self.final_df = pd.DataFrame([record])


	def write_to_red_shift(self):
		self.loggerv3.info("Writing to Red Shift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running New Viewer Retention Job for {self.target_date}")
		self.load_new_viewers()
		self.write_to_red_shift()
		self.loggerv3.success("All Processing Complete!")
