import pandas as pd
from base.etl_jobv3 import EtlJobV3


class ShopifyProductsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'shopify_products')
		self.products_data = []
		self.final_dataframe = None


	def get_products_data(self):
		self.loggerv3.info('Getting products data')
		query = """
			SELECT
			  p.id as product_id,
			  pv.sku,
			  pv.id as variant_id,
			  p.title as product_title,
			  pv.title as variant_title,
			  p.handle as slug,
			  p.created_at,
			  p.updated_at,
			  p.published_at,
			  p.image__src as image_url,
			  p.product_type,
			  p.status,
			  p.tags,
			  p.vendor,
			  pv.price as price
			FROM rooster_teeth_ecomm.products p
			INNER JOIN rooster_teeth_ecomm.products__variants pv on p.id = pv._sdc_source_key_id;
					"""
		results = self.db_connector.write_redshift(query)
		# NOTE: Not actually writing, query has the word "update". Need better solution
		for result in results:
			self.products_data.append({
				'product_id': result[0],
				'sku': result[1],
				'variant_id': result[2],
				'product_title': result[3],
				'variant_title': result[4],
				'slug': result[5],
				'created_at': result[6],
				'updated_at': result[7],
				'published_at': result[8],
				'image_url': result[9],
				'product_type': result[10],
				'status': result[11],
				'tags': result[12],
				'vendor': result[13],
				'price': result[14]
			})

		self.final_dataframe = pd.DataFrame(self.products_data)


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
		self.loggerv3.info("Moving Shopify Products Data")
		self.get_products_data()
		self.truncate_data()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
