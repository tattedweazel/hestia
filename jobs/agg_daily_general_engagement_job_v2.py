import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class AggDailyGeneralEngagementJobV2(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'agg_daily_general_engagement')

		self.Dater = DateHandler()
		self.target_day = self.target_date.replace('-','')
		self.next_day = self.Dater.find_next_day(self.target_day)
		self.formatted_dates = self.get_formatted_dates()
		self.vod_viewers = {}
		self.live_viewers = {}


	def get_formatted_dates(self):
		return {
			"target": self.Dater.format_date(self.target_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def get_unique_visitors(self):
		results = self.db_connector.read_redshift(f""" SELECT
												    count(distinct anonymous_id)
												FROM
												    warehouse.fact_visits
												WHERE
												    timestamp >= '{self.formatted_dates['target']}' AND
												    timestamp < '{self.formatted_dates['next']}';
											""")

		for result in results:
			return result[0]


	def set_vod_viewers(self):
		results = self.db_connector.read_redshift(f""" 
												SELECT 
													vv.anonymous_id, 
													du.user_id, 
													vv.user_tier, 
													sum(vv.active_seconds) as active_seconds
												FROM warehouse.vod_viewership vv
												LEFT JOIN warehouse.dim_user du
												ON du.user_key = vv.user_key
												WHERE
												    start_timestamp >= '{self.formatted_dates['target']}' AND
												    start_timestamp < '{self.formatted_dates['next']}' AND
												    user_tier not in ('grant', 'unknown')
												GROUP BY 1,2,3;
												""")

		for result in results:
			anon_key = result[0]
			user_key = result[1]
			tier = result[2]
			seconds = result[3]
			if tier == 'double_gold':
				tier = 'premium'
			if tier == 'anon':
				key = anon_key
			else:
				key = user_key
			self.vod_viewers[key] = {
				'tier': tier,
				'seconds': seconds
			}


	def set_live_viewers(self):
		results = self.db_connector.read_redshift(f""" 
												SELECT 
													user_id, 
													user_tier, 
													sum(active_seconds) as active_seconds
												FROM warehouse.livestream_viewership
												WHERE
												    start_timestamp >= '{self.formatted_dates['target']}' AND
												    start_timestamp < '{self.formatted_dates['next']}' AND
												    user_tier not in ('grant', 'unknown')
											    GROUP BY 1,2;
												""")

		for result in results:
			user_key = result[0]
			tier = result[1]
			seconds = result[2]
			if tier == 'double_gold':
				tier = 'premium'
			self.live_viewers[user_key] = {
				'tier': tier,
				'seconds': seconds
			}


	def process(self):
		self.loggerv3.info("Collecting Data")
		self.set_vod_viewers()
		self.set_live_viewers()
		viewership = {
			"vod_only": {
				"anon": {
					"viewers": set(),
					"seconds": 0
				},
				"free": {
					"viewers": set(),
					"seconds": 0
				},
				"trial": {
					"viewers": set(),
					"seconds": 0
				},
				"premium": {
					"viewers": set(),
					"seconds": 0
				},
				"total": {
					"viewers": set(),
					"seconds": 0
				}
			},
			"live_only": {
				"anon": {
					"viewers": set(),
					"seconds": 0
				},
				"free": {
					"viewers": set(),
					"seconds": 0
				},
				"trial": {
					"viewers": set(),
					"seconds": 0
				},
				"premium": {
					"viewers": set(),
					"seconds": 0
				},
				"total": {
					"viewers": set(),
					"seconds": 0
				}
			},
			"multi": {
				"anon": {
					"viewers": set(),
					"seconds": 0
				},
				"free": {
					"viewers": set(),
					"seconds": 0
				},
				"trial": {
					"viewers": set(),
					"seconds": 0
				},
				"premium": {
					"viewers": set(),
					"seconds": 0
				},
				"total": {
					"viewers": set(),
					"seconds": 0
				}
			},
		}
		for viewer_id in self.vod_viewers:
			if viewer_id not in self.live_viewers:
				viewership['vod_only']['total']['viewers'].add(viewer_id)
				viewership['vod_only']['total']['seconds'] += self.vod_viewers[viewer_id]['seconds']
				viewership['vod_only'][self.vod_viewers[viewer_id]['tier']]['viewers'].add(viewer_id)
				viewership['vod_only'][self.vod_viewers[viewer_id]['tier']]['seconds'] += self.vod_viewers[viewer_id]['seconds']
			else:
				viewership['multi']['total']['viewers'].add(viewer_id)
				viewership['multi']['total']['seconds'] += self.vod_viewers[viewer_id]['seconds']
				viewership['multi'][self.vod_viewers[viewer_id]['tier']]['viewers'].add(viewer_id)
				viewership['multi'][self.vod_viewers[viewer_id]['tier']]['seconds'] += self.vod_viewers[viewer_id]['seconds']
		for viewer_id in self.live_viewers:
			if viewer_id not in self.vod_viewers:
				viewership['live_only']['total']['viewers'].add(viewer_id)
				viewership['live_only']['total']['seconds'] += self.live_viewers[viewer_id]['seconds']
				viewership['live_only'][self.live_viewers[viewer_id]['tier']]['viewers'].add(viewer_id)
				viewership['live_only'][self.live_viewers[viewer_id]['tier']]['seconds'] += self.live_viewers[viewer_id]['seconds']
			else:
				viewership['multi']['total']['viewers'].add(viewer_id)
				viewership['multi']['total']['seconds'] += self.live_viewers[viewer_id]['seconds']
				viewership['multi'][self.live_viewers[viewer_id]['tier']]['viewers'].add(viewer_id)
				viewership['multi'][self.live_viewers[viewer_id]['tier']]['seconds'] += self.live_viewers[viewer_id]['seconds']

		multi_premium = len(viewership['multi']['premium']['viewers'])
		multi_trial = len(viewership['multi']['trial']['viewers'])
		multi_free = len(viewership['multi']['free']['viewers'])
		multi_anon = len(viewership['multi']['anon']['viewers'])
		multi_total = multi_premium + multi_trial + multi_free + multi_anon
		total_unique_viewers = len(viewership['vod_only']['total']['viewers']) + len(viewership['live_only']['total']['viewers']) + multi_total
		total_seconds_viewed = viewership['vod_only']['total']['seconds'] + viewership['live_only']['total']['seconds'] + viewership['multi']['total']['seconds']
		self.ge_df = pd.DataFrame([{
			"date": self.formatted_dates['target'],
			"total_unique_visitors": self.get_unique_visitors(),
			"total_unique_viewers": total_unique_viewers,
			"total_hours_viewed": total_seconds_viewed / 3600.0,
			"vod_only_total_viewers": len(viewership['vod_only']['total']['viewers']),
			"vod_only_total_hours": viewership['vod_only']['total']['seconds'] / 3600.0,
			"vod_only_premium_viewers": len(viewership['vod_only']['premium']['viewers']),
			"vod_only_premium_hours": viewership['vod_only']['premium']['seconds'] / 3600.0,
			"vod_only_trial_viewers": len(viewership['vod_only']['trial']['viewers']),
			"vod_only_trial_hours": viewership['vod_only']['trial']['seconds'] / 3600.0,
			"vod_only_free_viewers": len(viewership['vod_only']['free']['viewers']),
			"vod_only_free_hours": viewership['vod_only']['free']['seconds'] / 3600.0,
			"vod_only_anon_viewers": len(viewership['vod_only']['anon']['viewers']),
			"vod_only_anon_hours": viewership['vod_only']['anon']['seconds'] / 3600.0,
			"live_only_total_viewers": len(viewership['live_only']['total']['viewers']),
			"live_only_total_hours": viewership['live_only']['total']['seconds'] / 3600.0,
			"live_only_premium_viewers": len(viewership['live_only']['premium']['viewers']),
			"live_only_premium_hours": viewership['live_only']['premium']['seconds'] / 3600.0,
			"live_only_trial_viewers": len(viewership['live_only']['trial']['viewers']),
			"live_only_trial_hours": viewership['live_only']['trial']['seconds'] / 3600.0,
			"live_only_free_viewers": len(viewership['live_only']['free']['viewers']),
			"live_only_free_hours": viewership['live_only']['free']['seconds'] / 3600.0,
			"live_only_anon_viewers": len(viewership['live_only']['anon']['viewers']),
			"live_only_anon_hours": viewership['live_only']['anon']['seconds'] / 3600.0,
			"multi_total_viewers": multi_total,
			"multi_total_hours": viewership['multi']['total']['seconds'] / 3600.0,
			"multi_premium_viewers": multi_premium,
			"multi_premium_hours": viewership['multi']['premium']['seconds'] / 3600.0,
			"multi_trial_viewers": multi_trial,
			"multi_trial_hours": viewership['multi']['trial']['seconds'] / 3600.0,
			"multi_free_viewers": multi_free,
			"multi_free_hours": viewership['multi']['free']['seconds'] / 3600.0,
			"multi_anon_viewers": multi_anon,
			"multi_anon_hours": viewership['multi']['anon']['seconds'] / 3600.0	
		}])


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.ge_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.start(f"Running General Engagement for {self.formatted_dates['target']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
