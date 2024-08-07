import pandas as pd
from tools.modules.user_renewal_analysis.risk_prediction_v2 import RiskPredictionV2
from utils.connectors.database_connector import DatabaseConnector
from utils.components.backfill_by_dt import backfill_by_date
from utils.components.loggerv3 import Loggerv3


class RiskRunner:

	def __init__(self, start_date, end_date):
		self.db_connector = DatabaseConnector('')
		self.thresholds = [120, 150, 180, 210, 240, 270, 300, 330, 360]
		self.day_ranges = [7, 14, 21]
		self.dates = backfill_by_date(earliest_date=start_date, latest_date=end_date, input_format='%Y-%m-%d', output_format='%Y-%m-%d')
		self.final_data_structure = []
		self.table_name = 'renewal_risk_prediction_cohorts_v2'
		self.excluded_users = []
		self.final_dataframe = None
		self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
		self.loggerv3.alert = False


	def populate_excluded_users(self):
		self.loggerv3.info('Populating excluded users')
		results = self.db_connector.read_redshift(f"""
													WITH non_renewals as (
														SELECT user_key,
															   signup_date,
															   sub_type,
															   avg(vod_time_watched_min) as avg_vod_min
														FROM warehouse.daily_user_non_renewal_cohorts_v2
														WHERE total_sub_days <= 31
														  and days_into_sub <= 31
														GROUP BY 1, 2, 3
													), renewals as (
														SELECT user_key,
															   signup_date,
															   sub_type,
															   avg(vod_time_watched_min) as avg_vod_min
														FROM warehouse.daily_user_renewal_cohorts_v2
														WHERE total_sub_days > 31
														  and days_into_sub <= 30
														GROUP BY 1, 2, 3
													)
													SELECT distinct user_key
													FROM non_renewals
													WHERE avg_vod_min < 1
													   or avg_vod_min > 300
													UNION
													SELECT distinct user_key
													FROM renewals
													WHERE avg_vod_min < 1
													   or avg_vod_min > 300;
													""")

		for result in results:
			self.excluded_users.append(str(result[0]))


	def populate_data(self):
		self.loggerv3.info('Populating data')
		for date in self.dates:
			for threshold in self.thresholds:
				for day_range in self.day_ranges:
					rp = RiskPredictionV2(start_date=date, threshold=threshold, day_range=day_range, excluded_users=self.excluded_users)
					rp.execute()
					self.final_data_structure.extend(rp.final_data_structure)


	def build_final_dataframe(self):
		self.final_dataframe = pd.DataFrame(self.final_data_structure)


	def write_to_redshift(self):
		self.loggerv3.info('Writing to redshift')
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')


	def execute(self):
		self.loggerv3.info('Running Risk Runner')
		self.populate_excluded_users()
		self.populate_data()
		self.build_final_dataframe()
		self.write_to_redshift()
		self.db_connector.update_redshift_table_permissions(self.table_name)
		self.loggerv3.success("All Processing Complete!")
