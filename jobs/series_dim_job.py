import pandas as pd
from base.etl_jobv3 import EtlJobV3


class SeriesDimJob(EtlJobV3):


	def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
		super().__init__(jobname=__name__, db_connector=db_connector, table_name='dim_series')
		self.schema = 'warehouse'
		self.shows = []
		self.show_df = None


	def load_shows(self):
		self.loggerv3.info('Loading shows')
		query = """
			SELECT
				shows.uuid,
				shows.name,
				channels.name
			FROM shows
			LEFT JOIN channels ON shows.site = channels.id
			WHERE channels.id not in (8);
		"""
		results = self.db_connector.query_v2_db(query)
		for record in results:
			self.shows.append({
				"series_key": record[0],
				"name": record[1],
				"channel": record[2]
			})
		self.show_df = pd.DataFrame(self.shows)


	def truncate_table(self):
		self.loggerv3.info(f'Truncating table')
		self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.table_name};")


	def write_results_to_redshift(self):
		self.loggerv3.info("Writing results to Redshift")
		self.db_connector.write_to_sql(self.show_df, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


	def execute(self):
		self.loggerv3.info("Running Series Dim Job")
		self.load_shows()
		self.truncate_table()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
