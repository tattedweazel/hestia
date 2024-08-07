import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class DailySubscriptionPausesJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'daily_subscription_pauses')
		self.target_dt = datetime.strptime(f"{self.target_date} 07:00", '%Y-%m-%d %H:%M')
		self.next_date = datetime.strftime(self.target_dt + timedelta(1), '%Y-%m-%d')
		self.records = None
		self.final_dataframe = None


	def get_subscription_pause_counts(self):
		self.loggerv3.info(f"Aggregating subscription pauses for {self.target_date}")
		self.last_query = f"""
								SELECT
									subscription_event_type,
									count(*)
								FROM warehouse.subscription sub
								WHERE sub.subscription_event_type in (
									'Scheduled Subscription Pause',
									'Scheduled Subscription Pause Canceled',
									'Subscription Paused',
									'Subscription Resumed'
									)
									AND
									sub.event_timestamp >= '{self.target_date}' AND
									sub.event_timestamp < '{self.next_date}'
								GROUP BY 1;
								"""
		results = self.db_connector.read_redshift(self.last_query)

		self.records = []
		for result in results:
			data = {
				'run_date': self.target_date,
				'subscription_event': result[0],
				'count': result[1]
			}
			self.records.append(data)


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		self.final_dataframe = pd.DataFrame(self.records, index=None)


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f'Running Daily Subscription Pauses Job for {self.target_date}')
		self.get_subscription_pause_counts()
		self.build_final_dataframe()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
