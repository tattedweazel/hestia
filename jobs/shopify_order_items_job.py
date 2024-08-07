import pandas as pd
from base.etl_jobv3 import EtlJobV3


class ShopifyOrderItemsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'shopify_order_items')
		self.order_items_data = []
		self.final_dataframe = None


	def get_order_items_data(self):
		self.loggerv3.info('Getting order items data')
		query = """
				SELECT
				    _sdc_source_key_id as order_id,
				    product_id,
				    variant_id,
				    sku,
				    quantity
				FROM rooster_teeth_ecomm.orders__line_items
				WHERE sku IS NOT NULL;
				"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.order_items_data.append({
				'order_id': result[0],
				'product_id': result[1],
				'variant_id': result[2],
				'sku': result[3],
				'quantity': result[4]
			})

		self.final_dataframe = pd.DataFrame(self.order_items_data)


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
		self.loggerv3.info("Moving Shopify Order Items Data")
		self.get_order_items_data()
		self.truncate_data()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
