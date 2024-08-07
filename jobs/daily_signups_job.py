import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.classes.signup_journey import SignupJourney


class DailySignupsJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'daily_signups')
		self.schema = 'warehouse'
		self.target_date_dt = datetime.strptime(target_date, '%Y-%m-%d')
		self.start_date_dt = self.target_date_dt - timedelta(days=1)
		self.start_date = self.start_date_dt.strftime('%Y-%m-%d')
		self.end_date_dt = self.target_date_dt + timedelta(days=2)
		self.end_date = self.end_date_dt.strftime('%Y-%m-%d')
		self.events = []
		self.records = {}
		self.keys = []
		self.journeys = []
		self.sequence_counts = []
		self.valid_records = []
		self.existing_user_keys = []
		self.final_df = None


	def load_events(self):
		self.loggerv3.info("Loading Signup Events...")
		query = f""" 
					SELECT 
						event_timestamp,
						user_id,
						user_tier,
						anonymous_id,
						platform,
						state,
						step,
						option_selected
					FROM warehouse.signup_flow_event
					WHERE
						event_timestamp >= '{self.start_date}' AND
						event_timestamp < '{self.end_date}';
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.events.append({
				"event_timestamp": result[0],
				"user_id": result[1],
				"user_tier": result[2],
				"anonymous_id": result[3],
				"platform": result[4],
				"state": result[5],
				"step": result[6],
				"option_selected": result[7]
			})


	def create_records(self):
		self.loggerv3.info('Creating records')
		for event in self.events:
			anon_id = event['anonymous_id']
			if anon_id not in self.records:
				self.records[anon_id] = [event]
			else:
				self.records[anon_id].append(event)


	def build_journeys(self):
		self.loggerv3.info('Building journeys')
		for user_id in self.records:
			self.journeys.append(SignupJourney(user_id, self.records[user_id], self.target_date))


	def determine_valid_records(self):
		self.loggerv3.info('Determining valid records')
		for journey in self.journeys:
			if journey.valid:
				self.valid_records.append(journey)


	def load_keys_from_journeys(self):
		self.loggerv3.info('Loading keys from journeys')
		keys = set()
		for journey in self.journeys:
			if journey.valid:
				if journey.user_key is not None:
					keys.add(journey.user_key)
		self.keys = list(keys)


	def load_existing_user_keys(self):
		self.loggerv3.info('Loading existing user keys')
		target_keys = ','.join([f"'{x}'" for x in self.keys])
		query = f"""
			SELECT uuid
			FROM users
			WHERE created_at < '{self.target_date}' and uuid in ({target_keys});
		"""
		results = self.db_connector.query_v2_db(query)
		for result in results:
			self.existing_user_keys.append(result[0])


	def update_journey_result_based_on_existing_users(self):
		self.loggerv3.info('Updating journey results')
		for journey in self.valid_records:
			if journey.user_key in self.existing_user_keys:
				if journey.result == 'anon_to_free':
					journey.result = 'free_login'
				if journey.result == 'free_to_premium' or journey.result == 'anon_to_premium':
					journey.result = 'premium_login'


	def perform_rollup(self):
		self.loggerv3.info('Performing rollups')
		a2p_upgraders = self.get_journeys_with_result('anon_to_premium')
		f2p_upgraders = self.get_journeys_with_result('free_to_premium')
		rollup = {
			'eligible': {
				'anon': len(self.get_anon_travellers()),
				'free': len(self.get_free_travellers())
			}, 
			'upgrades': {
				'anon_to_free': len(self.get_journeys_with_result('anon_to_free')),
				'anon_to_premium': len(a2p_upgraders),
				'free_to_premium': len(f2p_upgraders)
			},
			'bounces': {
				'anon': len(self.get_journey_bounces_by_tier('anon')),
				'free': len(self.get_journey_bounces_by_tier('free'))
			},
			'logins': {
				'free': len(self.get_journey_logins_by_tier('free')),
				'premium': len(self.get_journey_logins_by_tier('premium'))
			}
		}

		total_travelers = 0
		for k,v in rollup['eligible'].items():
			total_travelers += v
		total_anon = rollup['eligible']['anon']
		total_free = rollup['eligible']['free']

		total_upgrades = 0
		for k,v in rollup['upgrades'].items():
			total_upgrades += v
		anon_to_free = rollup['upgrades']['anon_to_free']
		anon_to_premium = rollup['upgrades']['anon_to_premium']
		free_to_premium = rollup['upgrades']['free_to_premium']

		anon_upgrades = anon_to_free + anon_to_premium
		free_upgrades = free_to_premium

		total_logins = 0
		for k,v in rollup['logins'].items():
			total_logins += v
		free_logins = rollup['logins']['free']
		premium_logins = rollup['logins']['premium']

		total_bounces = total_travelers - total_upgrades - total_logins
		anon_bounces = total_anon - anon_to_free - anon_to_premium
		free_bounces = total_free - free_to_premium - free_logins

		anon_to_premium_results = self.get_signup_options(a2p_upgraders)
		a2p_1year = anon_to_premium_results['1Year']
		a2p_6month = anon_to_premium_results['6month']
		a2p_1month = anon_to_premium_results['1month']

		free_to_premium_results = self.get_signup_options(f2p_upgraders)
		f2p_1year = free_to_premium_results['1Year']
		f2p_6month = free_to_premium_results['6month']
		f2p_1month = free_to_premium_results['1month']

		return {
			"signup_date": self.target_date,
			"total_travelers": total_travelers,
			"total_anon": total_anon,
			"total_free": total_free,
			"total_upgrades": total_upgrades,
			"anon_to_free": anon_to_free,
			"anon_to_premium": anon_to_premium,
			"free_to_premium": free_to_premium,
			"total_bounces": total_bounces,
			"anon_bounces": anon_bounces,
			"free_bounces": free_bounces,
			"total_logins": total_logins,
			"free_logins": free_logins,
			"premium_logins": premium_logins,
			"total_conversion_rate": total_upgrades / (total_travelers - total_logins),
			"anon_conversion_rate": anon_upgrades / total_anon,
			"free_conversion_rate": free_upgrades / (total_free - free_logins),
			"total_bounce_rate": total_bounces / (total_travelers - total_logins),
			"anon_bounce_rate": anon_bounces / total_anon,
			"free_bounce_rate": free_bounces / (total_free - free_logins),
			"anon_1year_signups": a2p_1year,
			"anon_6month_signups": a2p_6month,
			"anon_month_signups": a2p_1month,
			"free_1year_signups": f2p_1year,
			"free_6month_signups": f2p_6month,
			"free_1month_signups": f2p_1month
		}


	def get_traveller_count(self, tier):
		count = 0
		for journey in self.valid_records:
			if journey.tiers[0] == tier.lower():
				count += 1
		return count


	def get_anon_travellers(self):
		travellers = []
		for journey in self.valid_records:
			if len(journey.tiers) == 1 and journey.tiers[0] == 'anon':
				travellers.append(journey)
		return travellers


	def get_free_travellers(self):
		travellers = []
		for journey in self.valid_records:
			if journey.tiers[0] == 'free':
				if len(journey.tiers) == 1:
					travellers.append(journey)
			elif journey.tiers[0] == 'anon':
				if len(journey.tiers) == 2 and journey.tiers[1] == 'free':
					travellers.append(journey)
		return travellers


	def get_journeys_with_result(self, result):
		results = []
		for journey in self.valid_records:
			if journey.result == result:
				results.append(journey)
		return results


	def get_journey_bounces_by_tier(self, tier):
		results = []
		for journey in self.valid_records:
			if journey.result == f"{tier}_bounce":
				results.append(journey)
		return results


	def get_journey_logins_by_tier(self, tier):
		results = []
		for journey in self.valid_records:
			if journey.result == f"{tier}_login":
				results.append(journey)
		return results


	def get_sequence_counts(self, journeys = None):
		if not journeys:
			journeys = self.journeys
		sequences = {}
		for journey in journeys:
			sequence = journey.get_sequence()
			if sequence in sequences:
				sequences[sequence] += 1
			else:
				sequences[sequence] = 1
		ordered_seq = []
		for seq, count in sequences.items():
			ordered_seq.append({
				'sequence': seq,
				'occurences': count
				})

		return sorted(ordered_seq, key=lambda d: d['occurences'], reverse=True)


	def get_signup_options(self, journeys):
		results = {
			'1Year': 0,
			'6month': 0,
			'1month': 0
		}
		for journey in journeys:
			final_result = None
			for record in journey.records:
			        if record['step'] == 2:
			                if record['option_selected'] and record['state'] == 'exited':
			                        final_result = record['option_selected']
			if final_result not in results:
			        results[final_result] = 1
			else:
			        results[final_result] += 1
		return results


	def display_overview(self):
		for key, value in self.perform_rollup().items():
			self.loggerv3.info(f"{key}: {value}")


	def build_final_dataframe(self):
		self.loggerv3.info('Building final dataframe')
		data = self.perform_rollup()
		self.final_df = pd.DataFrame([data])
		

	def write_to_red_shift(self):
		self.loggerv3.info("Writing to Red Shift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


	def execute(self):
		self.loggerv3.start(f"Running Signup Flow ETL for {self.target_date}")
		self.load_events()
		self.create_records()
		self.build_journeys()
		self.determine_valid_records()
		self.load_keys_from_journeys()
		self.load_existing_user_keys()
		self.update_journey_result_based_on_existing_users()
		self.build_final_dataframe()
		self.write_to_red_shift()
		self.loggerv3.success("All Processing Complete!")






