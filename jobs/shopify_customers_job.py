import pandas as pd
from base.etl_jobv3 import EtlJobV3


class ShopifyCustomersJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'shopify_customers')
		self.customers_data = []
		self.final_dataframe = None


	def get_customers_data(self):
		self.loggerv3.info('Getting customers data')
		query = """
		SELECT
			id as customer_id,
			multipass_identifier as multipass_id,
			orders_count as total_orders,
			cast(cast(total_spent as numeric(38,6)) * 100 as bigint) as total_spent_cents,
			created_at
		FROM rooster_teeth_ecomm.customers
		ORDER BY created_at desc;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.customers_data.append({
				'customer_id': result[0],
				'multipass_id': result[1],
				'total_orders': result[2],
				'total_spent_cents': result[3],
				'created_at': result[4]
			})

		self.final_dataframe = pd.DataFrame(self.customers_data)


	def truncate_data(self):
		self.loggerv3.info('Truncate data')
		self.db_connector.write_redshift(f"""   
			TRUNCATE TABLE warehouse.{self.table_name};
		""")


	def write_results_to_redshift(self):
		self.loggerv3.info("Writing to Redshift...")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append', chunksize=5000)
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info("Moving Shopify Customer Data")
		self.get_customers_data()
		self.truncate_data()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
