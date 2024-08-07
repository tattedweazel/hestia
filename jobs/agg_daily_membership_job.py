import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggDailyMembershipJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_daily_segment_membership')

		self.Dater = DateHandler()
		self.target_day = self.target_date.replace('-','')
		self.next_day = self.Dater.find_next_day(self.target_day)
		self.six_ago = self.Dater.find_x_days_ago(self.target_day, 6)
		self.seven_ago = self.Dater.find_x_days_ago(self.target_day, 7)
		self.formatted_dates = self.get_formatted_dates()
		self.target_events = ('Trial', 'New Paid', 'Returning Paid', 'FTP Conversion', 'Trial Cancel Requested', 'Paid Cancel Requested', 'Trial Churn', 'Paid Churn', 'Paid Terminated', 'Paid Renewal')


	def get_formatted_dates(self):
		return {
			"target": self.Dater.format_date(self.target_day),
			"next": self.Dater.format_date(self.next_day),
			"six_ago": self.Dater.format_date(self.six_ago),
			"seven_ago": self.Dater.format_date(self.seven_ago)
		}


	def get_account_creations_for_target_day(self):
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']

		query = (f"""SELECT
						id, uuid, created_at 
					 FROM 
						users 
					 WHERE 
						created_at >= '{target_day}' AND 
						created_at < '{next_day}'
				""")
		results = self.db_connector.query_v2_db(query)
		creations = []
		for result in results:
			creations.append({
				'id': result[0],
				'uuid': result[1],
				'created_at': result[2]
				})
		return creations

	def get_subscription_event_uuids_from_sv2(self):
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f"""SELECT
											    sub.user_key,
											    du.user_id
											FROM
											    warehouse.subscription sub
											LEFT JOIN
											    warehouse.dim_user du
											ON du.user_key = sub.user_key
											WHERE
											    sub.start_timestamp >= '{target_day}' AND
											    sub.start_timestamp < '{next_day}'
											""")
		uuids = []
		for result in results:
			uuids.append(result[1])
		return uuids


	def get_all_target_day_events_from_sv2(self, target_day, next_day):
		query = f"""SELECT
											    sub.subscription_event_type, 
											    count(*)
											FROM
											     warehouse.subscription sub
											WHERE
											    sub.subscription_event_type in {self.target_events} AND
											    sub.event_timestamp >= '{target_day}' AND
											    sub.event_timestamp < '{next_day}'
											GROUP BY sub.subscription_event_type;
											"""
		results = self.db_connector.read_redshift(query)

		return results

	def get_target_day_event_from_sv2(self, target_day, next_day, target_event):
		query = f"""
			SELECT
		    count(*)
		FROM
		     warehouse.subscription sub
		WHERE
		    sub.subscription_event_type = '{target_event}' AND
		    sub.event_timestamp >= '{target_day}' AND
		    sub.event_timestamp < '{next_day}'
		"""
		results = self.db_connector.read_redshift(query)

		for result in results:
			return result[0]


	def get_free_signups(self):
		creations = self.get_account_creations_for_target_day()
		creation_uuids = []
		for record in creations:
			creation_uuids.append(record['uuid'])
		subscription_event_uuids = self.get_subscription_event_uuids_from_sv2()
		non_sub_uuids = []
		for uuid in creation_uuids:
			if uuid not in subscription_event_uuids and uuid not in non_sub_uuids:
				non_sub_uuids.append(uuid)
		return len(non_sub_uuids)


	def process(self):
		self.loggerv3.info("Collecting Data...")

		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		six_ago = self.formatted_dates['six_ago']
		seven_ago = self.formatted_dates['seven_ago']
		target_day_event_results = self.get_all_target_day_events_from_sv2(target_day, next_day)
		event_results = {}
		for result in target_day_event_results:
			event_results[result[0]] = result[1]

		free_signups = self.get_free_signups()

		original_trials = self.get_target_day_event_from_sv2(seven_ago, six_ago, 'Trial')

		if original_trials > 1:
			conversion_rate = int((self.get_events(event_results,'FTP Conversion') / original_trials) * 100)
		else:
			conversion_rate = 0

		self.subs_df = pd.DataFrame([{
			'date': target_day,
			'free_signups': free_signups,
			'trial_signups': self.get_events(event_results, 'Trial'),
			'new_paid_signups': self.get_events(event_results, 'New Paid'),
			'returning_paid_signups': self.get_events(event_results, 'Returning Paid'),
			'ftp_converts': self.get_events(event_results, 'FTP Conversion'),
			'trial_cancels': self.get_events(event_results, 'Trial Cancel Requested'),
			'paid_cancels': self.get_events(event_results, 'Paid Cancel Requested'),
			'trial_churn': self.get_events(event_results, 'Trial Churn'),
			'voluntary_paid_churn': self.get_events(event_results, 'Paid Churn'),
			'involuntary_paid_churn': self.get_events(event_results, 'Paid Terminated'),
			'paid_renewals': self.get_events(event_results, 'Paid Renewal'),
			'conversion_rate': conversion_rate
		}])


	def get_events(self, events, event_type):
		return events[event_type] if event_type in events else 0


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.subs_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Daily Membership for {self.formatted_dates['target']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
