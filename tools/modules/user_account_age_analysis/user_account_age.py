import config
import pandas as pd
from datetime import datetime
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class UserAccountAge:

	def __init__(self, input_schema, input_table, reference_date, file_name):
		self.db_connector = DatabaseConnector(file_location=config.file_location)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=config.local_mode)
		self.input_schema = input_schema
		self.input_table = input_table
		self.input_users = []
		self.user_accounts = []
		self.REFERENCE_DATE = datetime.strptime(reference_date, '%Y-%m-%d')
		self.records_df = None
		self.file_name = file_name

		self.loggerv3.disable_alerting()


	def get_users_inputs(self):
		self.loggerv3.info('Getting user inputs')
		query = f"""
			SELECT uuid
			FROM {self.input_schema}.{self.input_table}
			GROUP BY 1;
		"""
		results = self.db_connector.read_redshift(query)

		for result in results:
			self.input_users.append({
				'user_id': result[0]
			})


	def get_user_account_creations(self):
		self.loggerv3.info('Getting user account creations')
		query = f"""
			SELECT uuid, created_at 
		 	FROM users;
		"""
		results = self.db_connector.query_v2_db(query)

		for result in results:
			self.user_accounts.append({
				'user_id': result[0],
				'created_at': result[1]
				})


	def join_data(self):
		self.loggerv3.info("Joining data")
		input_users_df = pd.DataFrame(self.input_users)
		accounts_df = pd.DataFrame(self.user_accounts)

		self.records_df = input_users_df.merge(accounts_df, on='user_id', how='left')

		self.records_df['viewership_date'] = self.REFERENCE_DATE
		self.records_df['age'] = (self.records_df['viewership_date'] - self.records_df['created_at']).dt.days
		# Remove null age rows
		self.records_df = self.records_df[~self.records_df.age.isna()]


	def output(self):
		self.loggerv3.info('Output')
		self.records_df.to_csv(f'tools/modules/output/{self.file_name}.csv')


	def execute(self):
		self.loggerv3.info(f"Running User Account Age")
		self.get_users_inputs()
		self.get_user_account_creations()
		self.join_data()
		self.output()
		self.loggerv3.success("All Processing Complete!")
