import config
import pandas as pd
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class CastMapBuilder:

	def __init__(self):
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.table_name = 'cast_map'
		self.backend_records = []
		self.airtable_records = []
		self.final_dataframe = None


	def get_svod_be_cast_members(self):
		self.loggerv3.info('Getting SVOD BE cast members')
		query = """
		SELECT
			cm.first_name,
			cm.last_name,
			e.uuid
		FROM cast_members cm
		LEFT JOIN cast_member_maps cmm ON cmm.cast_member_uuid = cm.uuid
		LEFT JOIN episodes e ON cmm.cast_memberable_id = e.uuid
		WHERE cm.last_name NOT IN ('Kovic', 'Haywood') AND e.title is not null
		GROUP BY 1, 2, 3;
		"""
		results = self.db_connector.query_svod_be_db_connection(query)
		for result in results:
			self.backend_records.append({
					"cast_member": f"{result[0]} {result[1]}",
					"episode_uuid": result[2]
				})


	def get_airtable_cast_members(self):
		self.loggerv3.info('Getting airtable cast members')
		query = """
		SELECT
			member,
			episode_key
		FROM warehouse.funhaus_cast_map
		WHERE role = 'cast'
		GROUP BY 1, 2;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.airtable_records.append({
				"cast_member": result[0],
				"episode_uuid": result[1]
			})


	def build_final_dataframe(self):
		self.loggerv3.info('Joining cast members')
		self.final_dataframe = pd.concat([pd.DataFrame(self.backend_records), pd.DataFrame(self.airtable_records)])
		self.final_dataframe.loc[self.final_dataframe["cast_member"].isin(['Kylah Cooke', 'DefinedBy Ky', 'ky cooke', 'Ky Cooke']), "cast_member"] = "Ky"
		self.final_dataframe.loc[self.final_dataframe["cast_member"].isin(['Gabie Jackman', 'Black Krystel']), "cast_member"] = "BK"
		self.final_dataframe.loc[self.final_dataframe["cast_member"] == 'Maggie Tomminey', "cast_member"] = "Maggie Tominey"
		self.final_dataframe.loc[self.final_dataframe["cast_member"] == 'Omar deArmas', "cast_member"] = "Omar de Armas"


	def truncate_table(self):
		self.loggerv3.info('Truncating table')
		query = f"TRUNCATE TABLE warehouse.{self.table_name}"
		self.db_connector.write_redshift(query)


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Redshift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Cast Map Builder")
		self.get_svod_be_cast_members()
		self.get_airtable_cast_members()
		self.build_final_dataframe()
		self.truncate_table()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
		