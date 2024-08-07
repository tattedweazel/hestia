import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggWeeklyGeneralEngagementJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_weekly_segment_general_engagement')
		
		self.Dater = DateHandler()
		self.end_day = self.target_date.replace('-','')
		self.next_day = self.Dater.find_next_day(self.end_day)
		self.start_day = self.Dater.find_x_days_ago(self.next_day, 7)
		self.formatted_dates = self.get_formatted_dates()
		self.vod_viewers = {}
		self.live_viewers = {}


	def get_formatted_dates(self):
		return {
			"start": self.Dater.format_date(self.start_day),
			"end": self.Dater.format_date(self.end_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def get_unique_visitors(self):
		results = self.db_connector.read_redshift(f""" SELECT
												    count(distinct anonymous_id)
												FROM
												    warehouse.fact_visits
												WHERE
												    timestamp >= '{self.formatted_dates['start']}' AND
												    timestamp < '{self.formatted_dates['next']}';
											""")

		for result in results:
			return result[0]


	def set_viewers(self, type):
		if type == 'vod':
			table = 'warehouse.vod_viewership'
		elif type == 'live':
			table = 'warehouse.livestream_viewership'
		else:
			return
		results = self.db_connector.read_redshift(f""" select
												    anonymous_id,
												    user_key,
												    user_tier
												from
												    {table}
												WHERE
												    start_timestamp >= '{self.formatted_dates['start']}' AND
												    start_timestamp < '{self.formatted_dates['next']}' AND
												    user_tier not in ('grant', 'unknown');
											""")

		for result in results:
			anon_key = result[0]
			user_key = result[1]
			tier = result[2]
			if tier == 'double_gold':
				tier = 'premium'
			if user_key is None:
				key = anon_key
				tier = 'anon'
			else:
				key = user_key
			if type == 'vod':
				self.vod_viewers[key] = tier
			else:
				self.live_viewers[key] = tier


	def process(self):
		self.loggerv3.info("Collecting Data")
		self.set_viewers('vod')
		self.set_viewers('live')
		total_unique_viewers = set()
		viewership = {
			"vod_only": {
				"anon": set(),
				"free": set(),
				"trial": set(),
				"premium": set(),
				"total": set()
			},
			"live_only": {
				"anon": set(),
				"free": set(),
				"trial": set(),
				"premium": set(),
				"total": set()
			},
			"multi": {
				"anon": set(),
				"free": set(),
				"trial": set(),
				"premium": set(),
				"total": set()
			}
		}
		for viewer_id in self.vod_viewers:
			total_unique_viewers.add(viewer_id)
			if viewer_id not in self.live_viewers:
				viewership['vod_only']['total'].add(viewer_id)
				viewership['vod_only'][self.vod_viewers[viewer_id]].add(viewer_id)
				
			else:
				viewership['multi']['total'].add(viewer_id)
				viewership['multi'][self.vod_viewers[viewer_id]].add(viewer_id)
		for viewer_id in self.live_viewers:
			total_unique_viewers.add(viewer_id)
			if viewer_id not in self.vod_viewers:
				viewership['live_only']['total'].add(viewer_id)
				viewership['live_only'][self.live_viewers[viewer_id]].add(viewer_id)
				
			else:
				viewership['multi']['total'].add(viewer_id)
				viewership['multi'][self.live_viewers[viewer_id]].add(viewer_id)
		multi_premium = len(viewership['multi']['premium'])
		multi_trial = len(viewership['multi']['trial'])
		multi_free = len(viewership['multi']['free'])
		multi_anon = len(viewership['multi']['anon'])
		multi_total = multi_premium + multi_trial + multi_free + multi_anon
		total_unique_viewers = len(viewership['vod_only']['total']) + len(viewership['live_only']['total']) + multi_total
		self.ge_df = pd.DataFrame([{
			"week_ending": self.formatted_dates['end'],
			"week_starting": self.formatted_dates['start'],
			"total_unique_visitors": self.get_unique_visitors(),
			"total_unique_viewers": total_unique_viewers,
			"vod_only_total_viewers": len(viewership['vod_only']['total']),
			"vod_only_premium_viewers": len(viewership['vod_only']['premium']),
			"vod_only_trial_viewers": len(viewership['vod_only']['trial']),
			"vod_only_free_viewers": len(viewership['vod_only']['free']),
			"vod_only_anon_viewers": len(viewership['vod_only']['anon']),
			"live_only_total_viewers": len(viewership['live_only']['total']),
			"live_only_premium_viewers": len(viewership['live_only']['premium']),
			"live_only_trial_viewers": len(viewership['live_only']['trial']),
			"live_only_free_viewers": len(viewership['live_only']['free']),
			"live_only_anon_viewers": len(viewership['live_only']['anon']),
			"multi_total_viewers": multi_total,
			"multi_premium_viewers": multi_premium,
			"multi_trial_viewers": multi_trial,
			"multi_free_viewers": multi_free,
			"multi_anon_viewers": multi_anon	
		}])


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.ge_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running Weekly General Engagement for {self.formatted_dates['end']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
