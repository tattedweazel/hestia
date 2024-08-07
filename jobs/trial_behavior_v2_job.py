import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.components.statisizer import Statisizer


class TrialBehaviorV2Job(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'trial_behavior_v2')
		"""Latest target date must be 10 days ago (trial + dunning) from current date"""
		self.process_started = datetime.now()
		self.start_date = target_date
		self.next_day = None
		self.view_end_date = None
		self.sub_end_date = None

		self.user_keys = []
		self.sub_events = []
		self.trialers = {}
		self.view_events = []
		self.visit_events = []
		self.converted = []
		self.did_not_convert = []
		self.stats = {
			'converted': {},
			'terminated': {}
		}
		self.final_dataframe = None


	def set_dates(self):
		self.loggerv3.info('Setting dates')
		start_date_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
		next_day_dt = start_date_dt + timedelta(1)
		view_end_date_dt = start_date_dt + timedelta(7)
		sub_end_date_dt = start_date_dt + timedelta(11)

		self.next_day = datetime.strftime(next_day_dt, '%Y-%m-%d')
		self.view_end_date = datetime.strftime(view_end_date_dt, '%Y-%m-%d')
		self.sub_end_date = datetime.strftime(sub_end_date_dt, '%Y-%m-%d')


	def load_trial_users(self):
		self.loggerv3.info("Loading Trial Starts...")
		query = f"""
		SELECT
			user_key
		FROM warehouse.subscription
		WHERE
			subscription_event_type = 'Trial' AND
			start_timestamp >= '{self.start_date}' AND
			start_timestamp < '{self.next_day}';
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.user_keys.append(result[0])


	def load_subscription_events(self):
		self.loggerv3.info("Loading Subscription Events...")
		user_keys = ','.join([str(key) for key in self.user_keys])
		query = f"""
		SELECT
			user_key,
			event_timestamp,
			subscription_event_type
		FROM warehouse.subscription
		WHERE
			user_key in ({user_keys}) AND
			start_timestamp >= '{self.start_date}' AND
			start_timestamp < '{self.sub_end_date}'
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.sub_events.append({
					"user_key": result[0],
					"timestamp": result[1],
					"event_type": result[2]
				})


	def build_trialers_from_sub_events(self):
		self.loggerv3.info("Building Trialers...")
		for event in self.sub_events:
			if event['user_key'] not in self.trialers:
				self.trialers[event['user_key']] = {
					'sub_events': [event],
					'views': {},
					'visits': {},
					'converted': False,
					'platforms': set(),
					'channels': set(),
					'series': set(),
					'episodes': set(),
					'total_visits': 0,
					'days_viewed': 0,
					'days_visited': 0,
					'days_active': 0,
					'seconds_watched': 0,
					'hours_watched': 0,
					'total_views': 0,
					'platforms_watched': 0,
					'channels_watched': 0,
					'series_watched': 0,
					'episodes_watched': 0,
				}
			else:
				self.trialers[event['user_key']]['sub_events'].append(event)


	def load_view_events(self):
		self.loggerv3.info("Loading View Events...")
		user_keys = ','.join([str(key) for key in self.user_keys])
		query = f"""
		SELECT
			vv.user_key,
			cast(vv.start_timestamp as varchar(10)) as date,
			vv.active_seconds,
			vv.episode_key,
			dse.episode_title,
			dse.series_title,
			dse.channel_title,
			vv.platform
		FROM warehouse.vod_viewership vv
		INNER JOIN warehouse.dim_segment_episode dse ON dse.episode_key = vv.episode_key
		WHERE
			user_key in ({user_keys}) AND
			start_timestamp >= '{self.start_date}' AND
			start_timestamp < '{self.view_end_date}'
		"""

		results = self.db_connector.read_redshift(query)
		for result in results:
			self.view_events.append({
				"user_key": result[0],
				"date": result[1],
				"active_seconds": result[2],
				"episode_key": result[3],
				"episode_title": result[4],
				"series_title": result[5],
				"channel_title": result[6],
				"platform": result[7]
			})


	def populate_views(self):
		self.loggerv3.info("Populating Views...")
		for event in self.view_events:
			if event['date'] not in self.trialers[event['user_key']]['views']:
				self.trialers[event['user_key']]['views'][event['date']] = [event]
			else:
				self.trialers[event['user_key']]['views'][event['date']].append(event)


	def load_visit_events(self):
		self.loggerv3.info("Loading Visit Events...")
		user_keys = ','.join([str(key) for key in self.user_keys])
		query = f"""
		SELECT
			user_key,
			cast(timestamp as varchar(10)),
			count(*)
		FROM warehouse.fact_visits
		WHERE
			user_key in ({user_keys}) AND
			timestamp >= '{self.start_date}' AND
			timestamp < '{self.view_end_date}'
		GROUP BY 1, 2
		ORDER BY 1, 2;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.visit_events.append({
					"user_key": result[0],
					"date": result[1],
					"total": result[2]
				})


	def populate_visits(self):
		self.loggerv3.info("Populating Visits...")
		for event in self.visit_events:
			self.trialers[event['user_key']]['visits'][event['date']] = event['total']


	def determine_conversions(self):
		self.loggerv3.info("Determining Conversions...")
		for trialer in self.trialers:
			converted = False
			for sub_event in self.trialers[trialer]['sub_events']:
				if sub_event['event_type'] == 'FTP Conversion':
					converted = True
			self.trialers[trialer]['converted'] = converted


	def calculate_aggregates(self):
		self.loggerv3.info("Calculating Aggregates...")
		for trialer in self.trialers:
			active_days = set()
			# Viewership
			for date in self.trialers[trialer]['views']:
				self.trialers[trialer]['days_viewed'] += 1
				for event in self.trialers[trialer]['views'][date]:
					self.trialers[trialer]['platforms'].add(event['platform'])
					self.trialers[trialer]['channels'].add(event['channel_title'])
					self.trialers[trialer]['series'].add(event['series_title'])
					self.trialers[trialer]['episodes'].add(event['episode_title'])
					self.trialers[trialer]['seconds_watched'] += event['active_seconds']
					self.trialers[trialer]['total_views'] += 1
					active_days.add(date)
			self.trialers[trialer]['hours_watched'] = round(self.trialers[trialer]['seconds_watched'] / 3600.0, 1)
			self.trialers[trialer]['platforms_watched'] = len(self.trialers[trialer]['platforms'])
			self.trialers[trialer]['channels_watched'] = len(self.trialers[trialer]['channels'])
			self.trialers[trialer]['series_watched'] = len(self.trialers[trialer]['series'])
			self.trialers[trialer]['episodes_watched'] = len(self.trialers[trialer]['episodes'])
			# Visits
			for date in self.trialers[trialer]['visits']:
				self.trialers[trialer]['days_visited'] += 1
				self.trialers[trialer]['total_visits'] = self.trialers[trialer]['visits'][date]
				active_days.add(date)
			# Active
			self.trialers[trialer]['days_active'] = len(active_days)


	def break_out_conversions(self):
		self.loggerv3.info("Breaking out conversions...")
		for trialer in self.trialers:
			if self.trialers[trialer]['converted']:
				self.converted.append(self.trialers[trialer])
			else:
				self.did_not_convert.append(self.trialers[trialer])


	def statisize(self):
		self.loggerv3.info("Statisizing...")
		cohort_size = len(self.converted) + len(self.did_not_convert)
		converted = Statisizer(
			records=self.converted,
			cohort_size=cohort_size,
			cohort_date=self.start_date
		)
		converted.execute()
		did_not_convert = Statisizer(
			records=self.did_not_convert,
			cohort_size=cohort_size,
			cohort_date=self.start_date
		)
		did_not_convert.execute()
		self.stats['converted'] = converted.as_object()
		self.stats['converted']['result'] = 'converted'
		self.stats['terminated'] = did_not_convert.as_object()
		self.stats['terminated']['result'] = 'terminated'


	def write_results_to_redshift(self):
		self.loggerv3.info("Writing to Redshift...")
		records = []
		for item in self.stats:
			records.append(self.stats[item])
		self.final_dataframe = pd.DataFrame(records)
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')


	def execute(self):
		self.loggerv3.info(f"Running Trial Attribution V2 for {self.start_date}")
		self.set_dates()
		self.load_trial_users()
		self.load_subscription_events()
		self.build_trialers_from_sub_events()
		self.load_view_events()
		self.populate_views()
		self.load_visit_events()
		self.populate_visits()
		self.determine_conversions()
		self.calculate_aggregates()
		self.break_out_conversions()
		self.statisize()
		self.write_results_to_redshift()
		self.loggerv3.success("All Processing Complete!")
