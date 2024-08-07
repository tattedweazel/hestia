import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class ViewerGraphGenerator(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = '_')

		self.data = {'raw':[]}
		self.artifacts = {}
		self.start_date = '2022-10-01'
		self.cap_date = '2022-11-01'

		###### DANGER ######
		self.loggerv3.disable_alerting()
		###### DANGER ######

	def get_raw_data(self):
		self.loggerv3.info("Loading Raw Data")
		query = f""" SELECT
					    vv.session_id,
					    vv.user_key,
					    vv.anonymous_id,
					    vv.user_tier,
					    vv.episode_key,
					    vv.active_seconds,
					    cast(vv.start_timestamp as char(10)) as start_timestamp,
					    vv.platform,
					    dse.series_title,
					    dse.series_id,
					    dse.season_title,
					    dse.season_id,
					    dse.length_in_seconds as episode_length_s,
					    cast(dse.air_date as char(10)) as episode_air_date,
					    dse.episode_title,
					    dse.channel_title,
					    dse.channel_id,
					    dse.episode_number
					FROM warehouse.vod_viewership vv
					INNER JOIN warehouse.dim_segment_episode dse
					ON vv.episode_key = dse.episode_key
					WHERE
						vv.user_key is not null AND
					    vv.start_timestamp >= '{self.start_date}' AND
					    vv.start_timestamp < '{self.cap_date}';"""

		results = self.db_connector.read_redshift(query)
		for result in results:
			self.data['raw'].append({
					'session_id': 								result[0],
				    'user_key': 								result[1],
				    'anonymous_id': 							result[2],
				    'user_tier': 								result[3],
				    'episode_key': 								result[4],
				    'active_seconds': 							result[5],
				    'start_timestamp': 							result[6],
				    'platform': 								result[7],
				    'series_title': 							result[8],
				    'series_id': 								result[9],
				    'season_title': 							result[10],
				    'season_id': 								result[11],
				    'episode_length_s': 						result[12],
				    'episode_air_date': 						result[13],
				    'episode_title':							result[14],
				    'channel_title': 							result[15],
				    'channel_id': 								result[16],
				    'episode_number':							result[17]
			})


	def parse_data(self):
		self.loggerv3.info("Parsing data")
		self.get_users()
		self.get_watches()
		self.get_episodes()
		self.get_seasons()
		self.get_series()
		self.get_channels()
		self.get_watched()
		self.get_view_of()
		self.get_belongs_to()
		self.data['raw'] = []


	def get_users(self):
		users = set()
		for item in self.data['raw']:
			users.add((item['user_key']))
		self.data['users'] = list(users)


	def get_watches(self):
		watches = set()
		for item in self.data['raw']:
			# Handle 0 second length episodes...
			if item['episode_length_s'] < 1:
				pct_watched = 0.0
			else:
				pct_watched = round( (item['active_seconds'] * 1.0) / item['episode_length_s'], 3)
			watches.add((
							item['session_id'],			# ~id
							item['start_timestamp'],	# watch_date:Date
							item['user_tier'],			# watch_tier:String
							item['platform'],			# platform:String
							item['active_seconds'],		# length_s:Int
							pct_watched,				# pct_watched:Float
							'Watch'						# ~label
						))
		self.data['watches'] = list(watches)


	def get_episodes(self):
		episodes = set()
		for item in self.data['raw']:
			episodes.add((
							item['episode_key'],		# ~id
							item['episode_title'],		# title:String
							item['episode_air_date'],	# air_date:Date
							item['episode_length_s'],	# length_s:Int
							'Episode'					# ~label
						))
		self.data['episodes'] = list(episodes)


	def get_seasons(self):
		seasons = set()
		for item in self.data['raw']:
			if item['season_id']: 
				seasons.add((
					item['season_id'],		# ~id
					item['season_title'],	# title:String
					'Season'				# ~label
				))
		self.data['seasons'] = list(seasons)


	def get_series(self):
		series = set()
		for item in self.data['raw']:
			series.add((
				item['series_id'],		# ~id
				item['series_title'],	# title:String
				'Series'				# ~label
			))
		self.data['series'] = list(series)


	def get_channels(self):
		channels = set()
		for item in self.data['raw']:
			channels.add((
				item['channel_id'],		# ~id
				item['channel_title'],	# title:String
				'Channel'				# ~label
			))
		self.data['channels'] = list(channels)


	def get_watched(self):
		watched = set()
		for item in self.data['raw']:
			# Handle 0 second length episodes...
			if item['episode_length_s'] < 1:
				pct_watched = 0.0
			else:
				pct_watched = round( (item['active_seconds'] * 1.0) / item['episode_length_s'], 3)
			# User _watched_ Watch
			watched.add((
							f"{int(item['user_key'])}{item['session_id']}", 	# ~id
							int(item['user_key']),								# ~from
							item['session_id'],									# ~to
							'_watched_',										# ~label
							pct_watched											# weight:Float
						))
		self.data['watched'] = list(watched)


	def get_view_of(self):
		view_of = set()
		for item in self.data['raw']:
			# Watch _view_of_ Episode
			view_of.add((
							f"{item['session_id']}{item['episode_key']}",	# ~id
							item['session_id'],								# ~from
							item['episode_key'],							# ~to
							'_view_of_',									# ~label
							item['episode_number']							# weight:Int
						))
		self.data['view_of'] = list(view_of)


	def get_belongs_to(self):
		belongs_to = set()
		for item in self.data['raw']:
			# Episode _belongs_to_ Season
			if item['season_id'] is not None:
				belongs_to.add((
					f"{item['episode_key']}{item['season_id']}", 	# ~id
					item['episode_key'],							# ~from
					item['season_id'],								# ~to
					'_belongs_to_',									# ~label
					1												# weight:Int
				))
			# Episode _belongs_to_ Series
			belongs_to.add((
				f"{item['episode_key']}{item['series_id']}", 		# ~id
				item['episode_key'],								# ~from
				item['series_id'],									# ~to
				'_belongs_to_',										# ~label
				1													# weight:Int
			))
			# Episode _belongs_to_ Channel
			belongs_to.add((
				f"{item['episode_key']}{item['channel_id']}", 		# ~id
				item['episode_key'],								# ~from
				item['channel_id'],									# ~to
				'_belongs_to_',										# ~label
				1													# weight:Int
			))
			# Season _belongs_to_ Series
			if item['season_id'] is not None:
				belongs_to.add((
					f"{item['season_id']}{item['series_id']}", 		# ~id
					item['season_id'],								# ~from
					item['series_id'],								# ~to
					'_belongs_to_',									# ~label
					item['episode_number']							# weight:Int
				))
			# Series _belongs_to_ Channel
			if item['series_id'] is not None:
				belongs_to.add((
					f"{item['series_id']}{item['channel_id']}", 	# ~id
					item['series_id'],								# ~from
					item['channel_id'],								# ~to
					'_belongs_to_',									# ~label
					1												# weight:Int
				))
		self.data['belongs_to'] = list(belongs_to)


	def build_artifacts(self):
		self.loggerv3.info("Building artifacts")
		self.build_vertices()
		self.build_edges()
	

	def build_vertices(self):
		self.build_users()
		self.build_watches()
		self.build_episodes()
		self.build_seasons()
		self.build_series()
		self.build_channels()


	def build_users(self):
		users = []
		for user_id in self.data['users']:
			users.append({
				'~id': int(user_id),
				'~label': 'User'
			})
		self.artifacts['Users'] = pd.DataFrame(users)


	def build_watches(self):
		watches = []
		for watch in self.data['watches']:
			watches.append({
				'~id': watch[0],
				'watch_date:Date': watch[1],
				'watch_tier:String': watch[2],
				'platform:String': watch[3],
				'length_s:Int': watch[4],
				'pct_watched:Float': watch[5],
				'~label': watch[6]
			})
		self.artifacts['Watches'] = pd.DataFrame(watches)


	def build_episodes(self):
		episodes = []
		for episode in self.data['episodes']:
			episodes.append({
				'~id': episode[0],
				'title:String': episode[1],
				'air_date:Date': episode[2],
				'length_s:Int': episode[3],
				'~label': episode[4]
			})
		self.artifacts['Episodes'] = pd.DataFrame(episodes)


	def build_seasons(self):
		seasons = []
		for season in self.data['seasons']:
			seasons.append({
				'~id': season[0],
				'title:String': season[1],
				'~label': season[2]
			})
		self.artifacts['Seasons'] = pd.DataFrame(seasons)


	def build_series(self):
		series = []
		for item in self.data['series']:
			series.append({
				'~id': item[0],
				'title:String': item[1],
				'~label': item[2]
			})
		self.artifacts['Series'] = pd.DataFrame(series)


	def build_channels(self):
		channels = []
		for item in self.data['channels']:
			channels.append({
				'~id': item[0],
				'title:String': item[1],
				'~label': item[2]
			})
		self.artifacts['Channels'] = pd.DataFrame(channels)


	def build_edges(self):
		self.build_watched()
		self.build_view_of()
		self.build_belongs_to()


	def build_watched(self):
		watched = []
		for item in self.data['watched']:
			watched.append({
				'~id': item[0],
				'~from': item[1],
				'~to': item[2],
				'~label': item[3],
				'weight:Float': item[4]
			})
		self.artifacts['_watched_'] = pd.DataFrame(watched)


	def build_view_of(self):
		view_of = []
		for item in self.data['view_of']:
			view_of.append({
				'~id': item[0],
				'~from': item[1],
				'~to': item[2],
				'~label': item[3],
				'weight:Int': item[4]
			})
		self.artifacts['_view_of_'] = pd.DataFrame(view_of)


	def build_belongs_to(self):
		belongs_to = []
		for item in self.data['belongs_to']:
			belongs_to.append({
				'~id': item[0],
				'~from': item[1],
				'~to': item[2],
				'~label': item[3],
				'weight:Int': item[4]
			})
		self.artifacts['_belongs_to_'] = pd.DataFrame(belongs_to)


	def output_artifacts(self):
		self.loggerv3.info(f"{len(self.artifacts)} artifacts to write...")
		for key in self.artifacts:
			self.artifacts[key].to_csv(f"artifacts/{key}.csv", index=False)


	def execute(self):
		self.get_raw_data()
		self.parse_data()
		self.build_artifacts()
		self.output_artifacts()
		self.loggerv3.info("Complete!")



