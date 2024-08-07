import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class SubscriptionCountJobV2(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'subscription_count_v2')
		self.schema = 'warehouse'
		self.target_date = target_date
		self.target_date_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.previous_date = datetime.strftime(self.target_date_dt - timedelta(1), '%Y-%m-%d')
		self.output = {
			self.target_date: {},
			self.previous_date: {}
		}
		self.gained_subs = []
		self.lost_subs = []
		self.current_subs_df = None
		self.previous_subs_df = None
		self.subs_df = None
		self.final_dataframe = None


	def load_active_subs(self, date):
		self.loggerv3.info(f'Loading active subs for {date}')
		active_subs = []
		results = self.db_connector.query_business_service_db_connection(f"""
			SELECT concat(user_uuid, uid)
			FROM user_subscriptions
			WHERE state in ('canceled', 'paying', 'payment_pending', 'trialing', 'expired')
				AND user_uuid is not NULL
				AND ends_at > '{date}'
				AND started_at <= '{date}'
			GROUP BY 1
		""")

		for result in results:
			active_subs.append(result[0])

		return active_subs


	def load_trial_subs(self, date):
		self.loggerv3.info(f'Loading trial subs for {date}')
		trial_subs = []
		results = self.db_connector.query_business_service_db_connection(f""" 
			SELECT concat(user_uuid, original_order_id) as id
			FROM android_subscriptions
			WHERE '{date}' BETWEEN trial_started_at and trial_ends_at
				AND user_uuid is NOT NULL
			GROUP BY 1
			
			UNION
			
			SELECT concat(user_uuid, original_transaction_id) as id
			FROM itunes_subscriptions
			WHERE '{date}' BETWEEN trial_started_at and trial_ends_at
				AND user_uuid is NOT NULL
			GROUP BY 1
			
			UNION
			
			SELECT concat(account_code, uuid) as id
			FROM subscriptions
			WHERE '{date}' BETWEEN trial_started_at and trial_ends_at
				AND uuid is NOT NULL
			GROUP BY 1
			
			UNION
			
			SELECT concat(user_uuid, original_transaction_id) as id
			FROM roku_subscriptions
			WHERE '{date}' BETWEEN trial_started_at and trial_ends_at
				AND user_uuid is NOT NULL
			GROUP BY 1;
		""")
		for result in results:
			trial_subs.append(result[0])

		return trial_subs


	def update_active_subs(self, date, active_subs, trial_subs):
		self.loggerv3.info(f'Updating active subs for {date}')
		active_total = []
		for user in active_subs:
			if user not in trial_subs:
				active_total.append(user)

		return active_total


	def load_data(self):
		for date in [self.target_date, self.previous_date]:
			active_subs = self.load_active_subs(date)
			trial_subs = self.load_trial_subs(date)
			active_subs_total = self.update_active_subs(date, active_subs, trial_subs)

			self.output[date]['active_subs_total'] = active_subs_total
			self.output[date]['trial_subs_total'] = trial_subs


	def calculate_net_paid(self):
		self.loggerv3.info('Calculating net paid')

		self.current_subs_df = pd.DataFrame(self.output[self.target_date]['active_subs_total'], columns=['user'])
		self.current_subs_df['run_date'] = self.target_date

		self.previous_subs_df = pd.DataFrame(self.output[self.previous_date]['active_subs_total'], columns=['user'])
		self.previous_subs_df['run_date'] = self.target_date

		self.subs_df = pd.merge(self.current_subs_df, self.previous_subs_df, on='user', how='outer', indicator=True)

		self.gained_subs = self.subs_df[self.subs_df._merge == 'left_only']
		self.lost_subs = self.subs_df[self.subs_df._merge == 'right_only']


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		current_subs = self.output[self.target_date]['active_subs_total']
		previous_subs = self.output[self.previous_date]['active_subs_total']
		current_trials = self.output[self.target_date]['trial_subs_total']
		record = {
			"run_date": self.target_date,
			"active": len(current_subs),
			"trial": len(current_trials),
			"gained": len(self.gained_subs),
			"lost": len(self.lost_subs),
			"net": len(current_subs) - len(previous_subs)
		}
		self.final_dataframe = pd.DataFrame([record])


	def write_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


	def execute(self):
		self.loggerv3.info(f"Running Subscription Count Job v2 for {self.target_date}")
		self.load_data()
		self.calculate_net_paid()
		self.build_final_dataframe()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
