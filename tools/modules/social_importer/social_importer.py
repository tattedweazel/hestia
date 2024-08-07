import pandas as pd
from utils.connectors.database_connector import DatabaseConnector


class SocialImporter:

	def __init__(self):
		self.table_name = 'social_metrics'
		self.db_connector = DatabaseConnector('')
		self.df = None


	def load_csv(self):
		self.df = pd.read_csv('tools/modules/social_importer/source/report.csv', encoding='utf-8')


	def write_to_redshift(self):
		self.db_connector.write_to_sql(self.df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', chunksize=5000, method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)



	def execute(self):
		self.load_csv()
		self.write_to_redshift()