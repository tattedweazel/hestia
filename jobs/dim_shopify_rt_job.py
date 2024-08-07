import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class DimShopifyRtJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'dim_shopify_rt')

		self.run_date = datetime.utcnow()
		self.records = []


	def load_shopify_mapping(self):
		results = self.db_connector.query_business_service_db_connection(f""" 
				select
				    sc.shopify_id,
				    sc.multipass_identifier,
				    scu.user_uuid
				from store_customers sc
				left join shopify_customer_users scu on sc.id = scu.store_customer_id
				where scu.user_uuid is not null;
			""")
		for result in results:
			self.records.append({
				"run_date": self.run_date,
				"shopify_id": result[0],
				"multipass_id": result[1],
				"rt_id": result[2],
			})


	def build_final_dataframe(self):
		self.final_dataframe = pd.DataFrame(self.records)


	def clear_dim_table(self):
		self.db_connector.write_redshift(f"TRUNCATE TABLE warehouse.{self.table_name}")


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Dim Shopify Rt")
		self.load_shopify_mapping()
		self.build_final_dataframe()
		self.clear_dim_table()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
