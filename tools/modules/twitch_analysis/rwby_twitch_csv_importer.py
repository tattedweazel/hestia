import pandas as pd
from utils.connectors.database_connector import DatabaseConnector


class RwbyTwitchCsvImporter:

	def __init__(self):
		"""
		To execute:
		from tools.modules.twitch_analysis.rwby_twitch_csv_importer import RwbyTwitchCsvImporter
		rtci = RwbyTwitchCsvImporter()
		rtci.execute()
		"""
		self.metrics_table_name = 'twitch_stream_metrics'
		self.rev_table_name = 'twitch_weekly_rev'
		self.db_connector = DatabaseConnector('')
		self.df = None


	def load_metrics_csv(self):
		self.df = pd.read_csv('tools/modules/twitch_analysis/source/rwby_streams.csv', encoding='utf-8')
		self.df['channel'] = 'rwby vt'


	def write_metrics_to_redshift(self):
		self.db_connector.write_to_sql(self.df, self.metrics_table_name, self.db_connector.sv2_engine(), schema='warehouse', chunksize=5000, method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.metrics_table_name)


	def load_rev_csv(self):
		self.df = pd.read_csv('tools/modules/twitch_analysis/source/rwby_rev.csv', encoding='utf-8')
		self.df['channel'] = 'rwby vt'


	def write_rev_to_redshift(self):
		self.db_connector.write_to_sql(self.df, self.rev_table_name, self.db_connector.sv2_engine(), schema='warehouse', chunksize=5000, method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.rev_table_name)


	def execute(self):
		self.load_metrics_csv()
		self.write_metrics_to_redshift()
		self.load_rev_csv()
		self.write_rev_to_redshift()
