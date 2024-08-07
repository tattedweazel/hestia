import pandas as pd
from base.etl_jobv3 import EtlJobV3


class ShopifyOrdersJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'shopify_orders')
		self.orders_data = []
		self.final_dataframe = None


	def get_orders_data(self):
		self.loggerv3.info('Getting orders data')
		query = """
			SELECT
				eol._sdc_source_key_id                  as order_id,
				o.order_number                          as order_number,
				eol.product_id                          as product_id,
				eol.variant_id                          as variant_id,
				eol.quantity                            as quantity,
				o.customer__id                          as shopify_id,
				CAST(o.total_price_usd * 100 as int)    as order_total_cents,
				CAST(o.total_discounts * 100 as int)    as order_total_discounts_cents,
				eol.sku                                 as sku,
				o.financial_status                      as financial_status,
				o.fulfillment_status                    as fulfillment_status,
				o.landing_site                          as landing_site,
				o.referring_site                        as referring_site,
				o.created_at                            as created_at
			FROM rooster_teeth_ecomm.orders__line_items eol
			INNER JOIN rooster_teeth_ecomm.orders o
			ON eol._sdc_source_key_id = o.id
			WHERE 
				eol.sku IS NOT NULL
				AND eol.variant_id is NOT NULL;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.orders_data.append({
				'order_id': result[0],
				'order_number': result[1],
				'product_id': result[2],
				'variant_id': result[3],
				'quantity': result[4],
				'shopify_id': result[5],
				'order_total_cents': result[6],
				'order_total_discounts_cents': result[7],
				'sku': result[8],
				'financial_status': result[9],
				'fulfillment_status': result[10],
				'landing_site': result[11],
				'referring_site': result[12],
				'created_at': result[13]
			})

		self.final_dataframe = pd.DataFrame(self.orders_data)


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
		self.loggerv3.info("Moving Shopify Orders Data")
		self.get_orders_data()
		self.truncate_data()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
