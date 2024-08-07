import config
from base.server_process_handler import ServerProcessHandler
from utils.components.dater import Dater
from utils.connectors.database_connector import DatabaseConnector
from jobs.shopify_customers_job import ShopifyCustomersJob
from jobs.shopify_order_items_job import ShopifyOrderItemsJob
from jobs.shopify_orders_job import ShopifyOrdersJob
from jobs.shopify_products_job import ShopifyProductsJob
from utils.components.loggerv3 import Loggerv3


class EdsHighFrequencyProcessHandler(ServerProcessHandler):

	def __init__(self, local_mode):
		if local_mode:
			self.file_location = ''
		else:
			self.file_location = '/home/ubuntu/processes/rt-data-hestia/'
		self.dater = Dater()
		config.file_location = self.file_location
		config.local_mode = local_mode
		config.process_handler = __name__
		self.connector = DatabaseConnector(config.file_location, config.dry_run)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location)
		self.loggerv3.disable_alerting()


	def run_jobs(self):
		self.loggerv3.start('Starting EDS High Frequency Process Handler')

		scj = ShopifyCustomersJob(db_connector = self.connector)
		scj.execute()

		soij = ShopifyOrderItemsJob(db_connector = self.connector)
		soij.execute()

		soj = ShopifyOrdersJob(db_connector = self.connector)
		soj.execute()

		spj = ShopifyProductsJob(db_connector = self.connector)
		spj.execute()

		self.loggerv3.success("All Processing Complete!")
