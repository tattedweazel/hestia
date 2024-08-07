import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class PendingCancelBalanceJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'pending_cancel_balance')
		self.target_dt = datetime.strptime(f"{self.target_date} 07:00", '%Y-%m-%d %H:%M')
		self.balance = None


	def get_balance(self):
		self.last_query = f""" 
								SELECT
									count(distinct subscription_id) 
								FROM warehouse.subscription
								WHERE subscription_event_type = 'Paid Cancel Requested' AND
								  end_timestamp >= '{self.target_date}' AND
								  event_timestamp <= '{self.target_date}';
								"""
		results = self.db_connector.read_redshift(self.last_query)

		for result in results:
			self.balance = result[0]
			return


	def build_final_dataframe(self):
		self.final_dataframe = pd.DataFrame([{'run_date': self.target_dt, 'balance': self.balance}])


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Pending Cancel Balance for {self.target_date}")
		self.get_balance()
		self.build_final_dataframe()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
