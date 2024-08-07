import pandas as pd
from base.etl_jobv3 import EtlJobV3
from collections import OrderedDict
from datetime import datetime, timedelta


class SignUpFlowJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'signup_flow', local_mode=True)

		self.target_date_dt = datetime.strptime(target_date, '%Y-%m-%d')
		self.start_date_dt = self.target_date_dt - timedelta(days=1)
		self.start_date = self.start_date_dt.strftime('%Y-%m-%d')
		self.end_date_dt = self.target_date_dt + timedelta(days=2)
		self.end_date = self.end_date_dt.strftime('%Y-%m-%d')
		self.events = []
		self.records = {}
		self.journeys = []
		self.sequence_counts = []
		self.valid_records = []
		self.existing_user_keys = []
		self.final_df = None
		self.debug = []


	def load_events(self):
		self.loggerv3.info("Loading Signup Events...")
		query = f""" 
					SELECT 
						event_timestamp,
						user_id,
						user_tier,
						anonymous_id,
						user_agent,
						platform,
						state,
						step,
						option_selected
					FROM warehouse.signup_flow_event
					WHERE
						event_timestamp >= '{self.start_date}' AND
						event_timestamp < '{self.end_date}'
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.events.append({
				"event_timestamp": result[0],
				"user_id": result[1],
				"user_tier": result[2],
				"anonymous_id": result[3],
				"user_agent": result[4],
				"platform": result[5],
				"state": result[6],
				"step": result[7],
				"option_selected": result[8]
			})


	def load_existing_user_keys(self):
		query = f"SELECT uuid FROM users WHERE created_at >= '{self.target_date}' and created_at < '{self.end_date}';"
		results = self.db_connector.query_v2_db(query)
		for result in results:
			self.existing_user_keys.append(result[0])


	def create_records(self):
		for event in self.events:
			anon_id = event['anonymous_id']
			if anon_id not in self.records:
				self.records[anon_id] = [event]
			else:
				self.records[anon_id].append(event)


	def build_journeys(self):
		for user_id in self.records:
			self.journeys.append(SignupJourney(user_id, self.records[user_id], self.target_date))


	def determine_valid_records(self):
		for journey in self.journeys:
			if journey.valid:
					self.valid_records.append(journey)


	def get_eligible_count(self):
		eligible = 0
		for record in self.valid_records:
			if record.tiers[0] != 'premium':
				eligible += 1
		return eligible


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


	def get_full_upgrade_totals(self):
		upgrades = {
			"anon_to_free_upgrades": 0,
			"anon_to_premium_upgrades": 0,
			"free_to_premium_upgrades":0
		}
		for record in self.valid_records:
			if len(record.tiers) < 2:
				continue
			if record.tiers[0] == 'anon':
				for key in record.keys:
					if key in self.existing_user_keys:
						if record.tiers[-1] == 'free':
							upgrades["anon_to_free_upgrades"] += 1
						elif record.tiers[-1] == 'premium':
							upgrades["anon_to_premium_upgrades"] += 1
					else: # TODO -- see if any of these are valid, and if they are... why?
						self.debug.append(record)
			elif record.tiers[0] == 'free':
				if record.tiers[-1] == 'premium':
					upgrades["free_to_premium_upgrades"] += 1
		return upgrades



	def get_conversion_rate(self):
		eligible = self.get_eligible_count()
		converts = 0
		for _, count in self.get_full_upgrade_totals().items():
			converts += count
		return round(converts / eligible, 4)


	def get_login_count(self):
		count = 0
		for journey in self.valid_records:
			sequence = journey.sequence[-1]
			option = journey.records[-1]['option_selected']
			if sequence == 'X1' and option == 'login':
				count += 1
		return count


	def get_bounce_count(self):
		counts = {}
		total = 0
		for journey in self.valid_records:
			if len(journey.tiers) == 1 and journey.tiers[0] != 'premium':
					tier = journey.tiers[0]
					if tier not in counts:
						counts[tier] = 1
					else:
						counts[tier] += 1
					total += 1
		counts['total'] = total
		return counts


	def get_bounce_rate(self):
		eligible = self.get_eligible_count()
		bounces = self.get_bounce_count()['total']
		return round(bounces / eligible, 4)


	def get_traveller_count(self, tier):
		count = 0
		for journey in self.valid_records:
			if journey.tiers[0] == tier.lower():
				count += 1
		return count


	def get_overview(self):
		full_upgrades = self.get_full_upgrade_totals()

		total_anons = self.get_traveller_count('anon')
		total_frees = self.get_traveller_count('free')

		total_upgrades = 0
		for tier, count in full_upgrades.items():
			total_upgrades += count


		# TODO: Add check - coversion_rate should = bounce_rate
		# TODO: Make sure that anon->free 

		return {
			"signup_date": self.target_date,
			"total_travellers": self.get_eligible_count(),
			"conversions": total_upgrades,
			"conversion_rate": self.get_conversion_rate(),
			"total_anon": total_anons,
			"total_free": total_frees,
			"anon_to_free_upgrades": full_upgrades['anon_to_free_upgrades'],
			"anon_to_premium_upgrades": full_upgrades['anon_to_premium_upgrades'],
			"free_to_premium_upgrades": full_upgrades['free_to_premium_upgrades'],
			"logins": self.get_login_count(),
			"overall_bounce_rate": self.get_bounce_rate(),
			"anon_bounce_rate": self.get_bounce_count()['anon'] / (total_anons),
			"free_bounce_rate": self.get_bounce_count()['free'] / (total_frees),
			"total_bounces": self.get_bounce_count()['total'],
			"free_bounces": self.get_bounce_count()['free'],
			"anon_bounces": self.get_bounce_count()['anon']
		}


	def display_overview(self):
		overview = self.get_overview()
		for key, value in overview.items():
			self.loggerv3.info(f"{key}: {value}")
		

	def write_to_redshift(self):
		self.loggerv3.info("Writing to Redshift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')


	def execute(self):
		self.loggerv3.info(f"Running Signup Flow ETL for {self.target_date}")
		self.load_events()
		self.load_existing_user_keys()
		self.create_records()
		self.build_journeys()
		self.determine_valid_records()
		self.display_overview()
		# self.write_to_red_shift()
		# self.db_connector.update_redshift_table_permissions(self.table_name)
		#self.loggerv3.success("All Processing Complete!")



class SignupJourney:

	def __init__(self, user_id, records, valid_date):
		self.user_id = user_id
		self.records = sorted(records, key=lambda d: d['event_timestamp'])
		self.valid_date = valid_date
		self.valid = False
		self.flagged = False
		self.keys = []
		self.tiers = []
		self.window = []
		self.steps = []
		self.sequence = self.parse_sequence()
		self.step_timing = self.parse_timing()
		self.audit()


	def parse_sequence(self):
		sequence = []
		keys = set()
		tiers = set()
		window = set()
		steps = set()
		for record in self.records:
			keys.add(record['user_id'])
			tiers.add(record['user_tier'])
			window.add(record['event_timestamp'].strftime('%Y-%m-%d'))
			steps.add(record['step'])
			if record['state'] == 'entered':
				mod = 'N'
			elif record['state'] == 'exited':
				mod = 'X'
			else:
				mod = 'U'
			sequence.append(f"{mod}{record['step']}")
		self.keys = list(keys)
		self.tiers = sorted(list(tiers))
		self.window = sorted(window)
		self.steps = sorted(list(steps))
		self.min_step = self.steps[0]
		self.max_step = self.steps[-1]
		return sequence


	def get_sequence(self):
		return '-'.join(self.sequence)


	def parse_timing(self):
		time_map = OrderedDict()
		timings = []
		previous_time = None
		for record in self.records:
			current_time = record['event_timestamp']
			if not previous_time:
				timings.append(0.0)
			else:
				delta = current_time - previous_time
				timings.append(delta.total_seconds())
			previous_time = current_time
		idx = 0
		for step in self.sequence:
			time_map[step] = timings[idx]
			idx += 1
		return time_map


	def audit(self):
		# Check if events start on target day, if they don't start until the next day, we're not looking for this record
		if self.window[0] == self.valid_date:
			self.valid = True

		# Make sure the sequence starts at a valid starting point
		if self.sequence[0] not in ('N1', 'X1', 'N2', 'N4'):
			self.flagged = True

		# Make sure no steps in the sequence are missing a state - Indicated by having a U
		for step in self.sequence:
			if 'U' in step:
				self.flagged = True

		# Make sure there are no more than 2 keys with any journey (anon, user)
		if len(self.keys) > 2:
			self.flagged = True







