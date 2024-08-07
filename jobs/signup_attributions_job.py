import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class SignupAttributionsJob(EtlJobV3):

		def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
			super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'signup_attributions')

			self.Dater = DateHandler()
			self.target_day = self.target_date.replace('-','')
			self.next_day = self.Dater.find_next_day(self.target_day)
			self.previous_day = self.Dater.find_previous_day(self.target_day)
			self.formatted_dates = self.get_formatted_dates()

			self.converted_users = []
			self.series_attributions = {}
			self.records = []


		def get_formatted_dates(self):
			return {
				"previous": self.Dater.format_date(self.previous_day),
				"target": self.Dater.format_date(self.target_day),
				"next": self.Dater.format_date(self.next_day)
			}


		def get_conversions(self):
			self.loggerv3.info(f"Loading conversions for {self.formatted_dates['target']}...")
			results = self.db_connector.read_redshift(f"""SELECT
													sub.user_key, 
													sub.start_timestamp
												FROM warehouse.subscription sub
												WHERE sub.subscription_event_type = 'Trial' 
													AND sub.event_timestamp >= '{self.formatted_dates['target']}'
													AND sub.event_timestamp < '{self.formatted_dates['next']}'
												""")
			for result in results:
				conversion = {
					"user_id": result[0],
					"start": result[1]
				}
				if conversion not in self.converted_users:
					self.converted_users.append(conversion)


		def populate_views(self):
			self.loggerv3.info(f"Populating views for {len(self.converted_users)} conversions...")

			for conversion in self.converted_users:
				previous_views = self.get_previous_series_views(conversion)
				following_views = self.get_following_series_views(conversion)
				conversion['previous'] = previous_views
				conversion['following'] = following_views
				conversion['attributed_watch'] = None


		def get_previous_series_views(self, conversion):
			results = self.db_connector.read_redshift(f"""SELECT
												vv.user_key, 
												vv.start_timestamp, 
												de.episode_title, 
												de.series_title, 
												de.season_title,
												de.channel_title
												FROM warehouse.vod_viewership vv
												LEFT JOIN warehouse.dim_segment_episode de
												ON vv.episode_key = de.episode_key
												WHERE user_key = {conversion['user_id']} 
												AND vv.start_timestamp > '{self.formatted_dates['previous'] + ' 00:00'}' 
												AND vv.start_timestamp < '{conversion['start']}'
												ORDER BY vv.start_timestamp desc
												LIMIT 3""")
			events = []
			for result in results:
					event = {
						"user_id": result[0],
						"start_time": result[1],
						"episode": result[2],
						"series": result[3],
						"season": result[4],
						"channel": result[5]
					}
					if event not in events:
						events.append(event)

			return events


		def get_following_series_views(self, conversion):
			results = self.db_connector.read_redshift(f"""SELECT
													vv.user_key, 
													vv.start_timestamp, 
													de.episode_title, 
													de.series_title,
													de.season_title, 
													de.channel_title
												FROM warehouse.vod_viewership vv
												LEFT JOIN warehouse.dim_segment_episode de
												ON vv.episode_key = de.episode_key
												WHERE user_key = {conversion['user_id']} 
												AND vv.start_timestamp >= '{conversion['start']}' 
												AND vv.start_timestamp < '{self.formatted_dates['next'] + ' 00:00'}'
												ORDER BY vv.start_timestamp asc
												LIMIT 3""")
			events = []
			for result in results:
					event = {
						"user_id": result[0],
						"start_time": result[1],
						"episode": result[2],
						"series": result[3],
						"season": result[4],
						"channel": result[5]
					}
					if event not in events:
						events.append(event)

			return events


		def set_attributions(self):
			self.loggerv3.info("Determining Attribution...")

			for record in self.converted_users:
				previous_watch = None
				following_watch = None
				attributed_watch = None

				if len(record['previous']) > 0:
					previous_watch = record['previous'][0]
				if len(record['following']) > 0:
					following_watch = record['following'][0]

				if previous_watch is None and following_watch is not None:
					attributed_watch = following_watch
				elif previous_watch is not None and following_watch is None:
					attributed_watch = previous_watch
				elif previous_watch is not None and following_watch is not None:
					attributed_watch = following_watch
				record['attributed_watch'] = attributed_watch


		def roll_up_attributions(self):
			self.loggerv3.info("Rolling up Attributions")
			for record in self.converted_users:
				if record['attributed_watch'] is None:
					continue
				event = {
					"run_date": self.formatted_dates['target'],
					"user_key": record['user_id'],
					"episode": record['attributed_watch']['episode'],
					"series": record['attributed_watch']['series'],
					"season": record['attributed_watch']['season'],
					"channel": record['attributed_watch']['channel']
				}
				self.records.append(event)
			self.final_dataframe = pd.DataFrame(self.records)


		def write_all_results_to_redshift(self):
			self.loggerv3.info("Writing results to Red Shift")
			self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
			self.db_connector.update_redshift_table_permissions(self.table_name)


		def execute(self):
			self.loggerv3.info("Running Signup Attributions")
			self.get_conversions()
			self.populate_views()
			self.set_attributions()
			self.roll_up_attributions()
			self.write_all_results_to_redshift()
			self.loggerv3.success("All Processing Complete!")
