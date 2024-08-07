import pandas as pd
from base.etl_jobv3 import EtlJobV3


class BackfillDailyViewerAgeByDeviceJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, db_connector = db_connector, table_name = 'daily_viewer_age_by_device')
		"""avg_age_in_days is the number of days between viewership date and the date their account was created."""
		self.viewers = []
		self.user_accounts = []
		self.records_df = None
		self.final_dataframe = None


	def get_desktop_viewers(self):
		self.loggerv3.info('Getting desktop viewers')
		query = f"""
			SELECT
				cast(event_timestamp as varchar(10)), 
				user_id
			FROM warehouse.video_event
			WHERE 
				platform = 'web' AND
			 	user_id is not NULL AND
			 	label in ('play_started', 'play_completed', 'play_resumed') AND 
			 	on_mobile_device is False
			GROUP BY 1, 2;
		"""
		results = self.db_connector.read_redshift(query)

		for result in results:
			self.viewers.append({
				'viewership_date': result[0],
				'user_id': result[1],
				'device': 'desktop'
			})


	def get_mobile_web_viewers(self):
		self.loggerv3.info('Getting mobile viewers')
		query = f"""
			SELECT 
				cast(event_timestamp as varchar(10)),
				user_id
			FROM warehouse.video_event
			WHERE 
				platform = 'web' AND
				user_id is not NULL AND
				label in ('play_started', 'play_completed', 'play_resumed') AND
				on_mobile_device is True
			GROUP BY 1, 2;
		"""
		results = self.db_connector.read_redshift(query)

		for result in results:
			self.viewers.append({
				'viewership_date': result[0],
				'user_id': result[1],
				'device': 'mobile web'
			})


	def get_user_account_creations(self):
		self.loggerv3.info('Getting user account creations')
		query = f"""
			SELECT
				uuid, 
				created_at 
		 	FROM 
				users;
		"""
		results = self.db_connector.query_v2_db(query)

		for result in results:
			self.user_accounts.append({
				'user_id': result[0],
				'created_at': result[1]
				})


	def join_data(self):
		self.loggerv3.info("Joining data")
		viewers_df = pd.DataFrame(self.viewers)

		# Segment viewers data by device
		desktop_df = viewers_df[viewers_df['device'] == 'desktop']
		mobile_web_df = viewers_df[viewers_df['device'] == 'mobile web']
		# Outer join users from each device to find overlap (think venn diagram)
		outer_join_df = pd.merge(desktop_df, mobile_web_df, on=['user_id', 'viewership_date'], how='outer', indicator=True)
		# Create device column with readable values
		outer_join_df.loc[outer_join_df['_merge'] == 'left_only', 'device'] = 'desktop only'
		outer_join_df.loc[outer_join_df['_merge'] == 'right_only', 'device'] = 'mobile web only'
		outer_join_df.loc[outer_join_df['_merge'] == 'both', 'device'] = 'desktop and mobile web'
		outer_join_df = outer_join_df[['viewership_date', 'user_id', 'device']]
		# Join data with user accounts
		accounts_df = pd.DataFrame(self.user_accounts)
		self.records_df = outer_join_df.merge(accounts_df, on='user_id', how='left')
		# Add date columns
		self.records_df['viewership_date_dt'] = pd.to_datetime(self.records_df['viewership_date'])
		self.records_df['age'] = (self.records_df['viewership_date_dt'] - self.records_df['created_at']).dt.days
		# Remove null age rows
		self.records_df = self.records_df[~self.records_df.age.isna()]


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		agg_records = []
		dates = set(self.records_df["viewership_date"].tolist())
		for date in dates:
			for device in ('desktop only', 'mobile web only', 'desktop and mobile web'):
				df = self.records_df[(self.records_df.device == device) & (self.records_df.viewership_date == date)]
				agg_records.append({
					'viewership_date': date,
					'device': device,
					'viewers': len(df),
					'days_0_30': len(df[df.age <= 30]),
					'days_31_180': len(df[(df.age > 30) & (df.age <= 180)]),
					'days_181_plus': len(df[df.age > 180]),
					'median_age_in_days': round(df.age.median(), 1)
				})

		self.final_dataframe = pd.DataFrame(agg_records)


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append', chunksize=5000)
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Backfill for Daily Viewer Age By Device")
		self.get_desktop_viewers()
		self.get_mobile_web_viewers()
		self.get_user_account_creations()
		self.join_data()
		self.build_final_dataframe()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
