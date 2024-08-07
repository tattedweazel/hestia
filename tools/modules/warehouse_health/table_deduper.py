import config
import pandas as pd
from utils.connectors.database_connector import DatabaseConnector
from utils.components.batcher import Batcher
from utils.components.loggerv3 import Loggerv3
from utils.components.sql_helper import SqlHelper


class TableDeduper:

	def __init__(self, table, primary_keys, existing_schema='warehouse', temp_schema='hades'):
		"""primary_keys is a list of one or more key(s)"""
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.batcher = Batcher()
		self.sql_helper = SqlHelper()
		self.table = table
		self.primary_keys = primary_keys
		self.column_data_types = {}
		self.dupe_primary_keys = []
		self.primary_key_batches = None
		self.existing_schema = existing_schema
		self.temp_schema = temp_schema
		self.dupe_rows = []
		self.dupe_rows_df = None


	def get_column_data_types(self):
		self.loggerv3.info('Getting column data types')
		query = f"""
		SELECT
			ordinal_position,
			data_type
        FROM svv_columns
        WHERE
        	table_name = '{self.table}'
        	AND table_schema = '{self.existing_schema}'
        ORDER BY ordinal_position;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.column_data_types[f'col{result[0]-1}'] = self.sql_helper.convert_datatype(result[1])


	def primary_key_creator(self):
		query = ""
		for idx, key in enumerate(self.primary_keys):
			query = key if idx == 0 else f'concat({key}, {query})'
		return query


	def get_dupe_primary_keys(self):
		self.loggerv3.info('Getting dupe primary keys')
		query = f"""
		WITH cte as (
			SELECT
				{self.primary_key_creator()} as pk,
				count(*) as cnt
			FROM {self.existing_schema}.{self.table}
			GROUP BY 1
		)
		SELECT
			pk
			cnt
		FROM cte
    	WHERE cnt > 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.dupe_primary_keys.append(result[0])
		self.loggerv3.info(f'Distinct Primary Key Dupes: {len(self.dupe_primary_keys)}')


	def batch_dupe_primary_keys(self):
		self.loggerv3.info('Batching dupe primary keys')
		self.primary_key_batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=self.dupe_primary_keys)


	def get_dupes_from_existing(self):
		self.loggerv3.info('Getting dupe rows from existing table')

		for batch in self.primary_key_batches:
			target_keys = ','.join([f"'{x}'" for x in batch])
			query = f"""
    		SELECT *
    		FROM {self.existing_schema}.{self.table}
    		WHERE {self.primary_key_creator()} in ({target_keys});
			"""
			results = self.db_connector.read_redshift(query)
			for result in results:
				result_len = len(result)
				row = {}
				for i in range(result_len):
					row[f'col{i}'] = result[i]
				self.dupe_rows.append(row)
			self.loggerv3.info(f'Dupe Rows: {len(self.dupe_rows)}')


	def write_dupes_to_temp(self):
		self.loggerv3.info('Writing dupe rows to temp table')
		self.dupe_rows_df = pd.DataFrame(self.dupe_rows)
		self.dupe_rows_df.drop_duplicates(inplace=True)
		self.db_connector.write_to_sql(self.dupe_rows_df, self.table, self.db_connector.sv2_engine(), schema=self.temp_schema, method='multi', chunksize=5000, index=False, if_exists='append', dtype=self.column_data_types)


	def remove_dupes_from_existing(self):
		self.loggerv3.info('Removing dupe rows from existing table')

		for batch in self.primary_key_batches:
			target_keys = ','.join([f"'{x}'" for x in batch])
			query = f"""
			DELETE FROM {self.existing_schema}.{self.table}
		    WHERE {self.primary_key_creator()} in ({target_keys});
			"""
			self.db_connector.write_redshift(query)


	def insert_back_into_existing(self):
		self.loggerv3.info('Inserting deduped rows back into existing')
		query = f"""
		INSERT INTO {self.existing_schema}.{self.table}
		SELECT * FROM {self.temp_schema}.{self.table};
		"""
		self.db_connector.write_redshift(query)


	def drop_temp_table(self):
		self.loggerv3.info('Dropping temp table')
		query = f"""
		DROP TABLE {self.temp_schema}.{self.table};
		"""
		self.db_connector.write_redshift(query)


	def execute(self):
		self.loggerv3.info(f'Deduping {self.table}')
		self.get_column_data_types()
		self.get_dupe_primary_keys()
		if len(self.dupe_primary_keys) == 0:
			self.loggerv3.info('No dupes in table')
			return
		self.batch_dupe_primary_keys()
		self.get_dupes_from_existing()
		self.write_dupes_to_temp()
		self.remove_dupes_from_existing()
		self.insert_back_into_existing()
		self.drop_temp_table()
		self.loggerv3.success("All Processing Complete!")
