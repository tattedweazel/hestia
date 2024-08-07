import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class SubscriptionCompositionJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'subscription_composition')

		self.run_date = datetime.utcnow()
		self.records = []


	def load_subscription_counts(self):
		results = self.db_connector.query_business_service_db_connection(f""" 
				select
				    plan_code,
				    count(*) as active_subs
				from user_subscriptions
				where state in ('canceled', 'paying', 'payment_pending')
				and user_uuid is not null
				group by 1;
			""")
		for result in results:
			self.records.append({
				"run_date": self.run_date,
				"plan": result[0],
				"total": result[1]
			})


	def build_final_dataframe(self):
		self.final_dataframe = pd.DataFrame(self.records)


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Subscription Composition Job")
		self.load_subscription_counts()
		self.build_final_dataframe()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
