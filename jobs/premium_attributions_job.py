import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class PremiumAttributionsJob(EtlJobV3):

		def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
			super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'premium_attributions_v2')
			""" V2 job for signup and returning attributions"""
			self.target_date_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
			self.next_date_dt = self.target_date_dt + timedelta(days=1)
			self.two_days_later_dt = self.target_date_dt + timedelta(days=2)
			self.previous_date_dt = self.target_date_dt - timedelta(days=1)
			self.subscription_event_types = ['Trial', 'Returning Paid', 'New Paid']
			self.attributed_user_viewership = []
			self.final_dataframe = None


		def get_attributions(self):
			self.loggerv3.info('Getting attributions')
			for subscription_event_type in self.subscription_event_types:
				self.get_attributions_by_type(subscription_event_type)


		def get_attributions_by_type(self, subscription_event_type):
			self.loggerv3.info(f"Getting attributions for {subscription_event_type}")
			query = f"""
			WITH trials as (
				SELECT sub.user_key,
					   sub.start_timestamp as start
				FROM warehouse.subscription sub
				WHERE sub.subscription_event_type = '{subscription_event_type}'
				  AND sub.event_timestamp >= '{self.target_date}'
				  AND sub.event_timestamp < '{self.next_date_dt}'
			), previous as (
				SELECT vv.user_key,
					   vv.start_timestamp,
					   de.episode_key
				FROM warehouse.vod_viewership vv
				LEFT JOIN warehouse.dim_segment_episode de ON vv.episode_key = de.episode_key
				INNER JOIN trials t ON t.user_key = vv.user_key AND vv.start_timestamp < t.start
				WHERE
				  vv.start_timestamp > '{self.previous_date_dt}'
				ORDER BY vv.start_timestamp DESC
			), following as (
				SELECT
					vv.user_key,
					vv.start_timestamp,
					de.episode_key
				FROM warehouse.vod_viewership vv
				LEFT JOIN warehouse.dim_segment_episode de ON vv.episode_key = de.episode_key
				INNER JOIN trials t ON t.user_key = vv.user_key AND vv.start_timestamp >= t.start
				WHERE
					vv.start_timestamp < '{self.two_days_later_dt}'
				ORDER BY vv.start_timestamp ASC
			),  rank as (
				SELECT t.user_key,
					   p.start_timestamp as previous_start,
					   p.episode_key as previous_episode_key,
					   f.start_timestamp as following_start,
					   f.episode_key as following_episode_key,
					   row_number() OVER (PARTITION BY t.user_key ORDER BY f.start_timestamp ASC, p.start_timestamp DESC) AS row_number
				FROM trials t
						 LEFT JOIN previous p ON p.user_key = t.user_key
						 LEFT JOIN following f ON f.user_key = t.user_key
				WHERE (p.start_timestamp is NOT NULL or f.start_timestamp is NOT NULL)
			)
			SELECT
				   user_key,
				   coalesce(following_episode_key, previous_episode_key) as episode
			FROM rank
			where row_number = 1;
			"""
			results = self.db_connector.read_redshift(query)
			for result in results:
				conversion = {
					'run_date': self.target_date,
					'subscription_event_type': subscription_event_type,
					'user_key': result[0],
					'episode_key': result[1]
				}
				self.attributed_user_viewership.append(conversion)


		def build_final_dataframe(self):
			self.loggerv3.info("Building final dataframe")
			self.final_dataframe = pd.DataFrame(self.attributed_user_viewership)


		def write_all_results_to_redshift(self):
			self.loggerv3.info("Writing results to Red Shift")
			self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
			self.db_connector.update_redshift_table_permissions(self.table_name)


		def execute(self):
			self.loggerv3.info(f"Running Premium Attributions for {self.target_date}")
			self.get_attributions()
			self.build_final_dataframe()
			self.write_all_results_to_redshift()
			self.loggerv3.success("All Processing Complete!")
