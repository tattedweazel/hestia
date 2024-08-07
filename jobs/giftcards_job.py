import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta



class GiftcardsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'giftcards')

		self.target_dt = datetime.strptime(f"{self.target_date} 07:00", '%Y-%m-%d %H:%M')
		self.next_date = datetime.strftime(self.target_dt + timedelta(1), '%Y-%m-%d')

		self.purchases = None
		self.redemptions = None


	def load_purchases(self):
		results = self.db_connector.query_business_service_db_connection(f""" SELECT
									    count(*),
									    sum(unit_amount_in_cents) as amount
									FROM gift_cards
									WHERE
									    created_at >= '{self.target_date}' AND
									    created_at < '{self.next_date}';
			""")
		for record in results:
			if record[1] is None:
				total = 0
			else:
				total = int(record[1])
			self.purchases = {
				"cards_purchased": int(record[0]),
				"purchase_total": total,
			}


	def load_redemptions(self):
		results = self.db_connector.query_business_service_db_connection(f""" SELECT
									    count(*),
    									sum(unit_amount_in_cents - balance_in_cents) as amount_redeemed
									FROM gift_cards
									WHERE
									    redeemed_at >= '{self.target_date}' AND
									    redeemed_at < '{self.next_date}';
			""")
		for record in results:
			if record[1] is None:
				total = 0
			else:
				total = int(record[1])
			self.redemptions = {
				"cards_redeemed": int(record[0]),
				"redemption_total": total,
			}


	def combine_results(self):
		combined_record = {
			"run_date": self.target_dt,
			"cards_purchased": self.purchases['cards_purchased'],
			"purchase_total": self.purchases['purchase_total'],
			"cards_redeemed": self.redemptions['cards_redeemed'],
			"redemption_total": self.redemptions['redemption_total'],
		}
		self.final_dataframe = pd.DataFrame([combined_record])


	def write_all_results_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Giftcards for {self.target_date}")
		self.load_purchases()
		self.load_redemptions()
		self.combine_results()
		self.write_all_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
