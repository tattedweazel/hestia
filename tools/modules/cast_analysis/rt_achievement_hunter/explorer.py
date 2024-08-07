from datetime import datetime
from tools.modules.cast_analysis.base.episode import Episode
from tools.modules.cast_analysis.base.series import Series
from utils.connectors.database_connector import DatabaseConnector
from utils.components.logger import Logger

class Explorer:

	def __init__(self, min_air_date, max_air_date = None):
		self.db_connector = DatabaseConnector('')
		self.logger = Logger()
		self.min_air_date = min_air_date
		if max_air_date is None:
			self.max_air_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
		else:
			self.max_air_date = max_air_date
		self.series = []
		self.episode_data = {}
		self.batches = None

	def load_series_episode_cast(self):
		self.logger.log("Loading Cast")
		series_episode_cast = {}
		query = f"""
				SELECT
			        cm.cast_member,
			        cm.episode_title,
			        cm.episode_uuid,
			        cm.series_title
			    FROM warehouse.cast_map cm
			    INNER JOIN warehouse.dim_segment_episode dse
			    ON dse.episode_key = cm.episode_uuid
			    WHERE 
			    	dse.air_date >= '{self.min_air_date}' AND
			    	dse.air_date < '{self.max_air_date}';
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			cast_member = result[0]
			episode_str = f"{result[2]}/-/{result[1]}"
			series_name = result[3]
			if series_name not in series_episode_cast:
				series_episode_cast[series_name] = {episode_str: [cast_member]}
			elif episode_str not in series_episode_cast[series_name]:
				series_episode_cast[series_name][episode_str] = [cast_member]
			else:
				series_episode_cast[series_name][episode_str].append(cast_member)
		return series_episode_cast


	def populate_data(self):
		collection = self.load_series_episode_cast()
		for series_record in collection:
			series = Series(series_record)
			for episode_uuid in collection[series_record]:
					episode = Episode(episode_uuid)
					for cast_member in collection[series_record][episode_uuid]:
						episode.add_to_cast(cast_member)
					series.add_episode(episode)
			self.series.append(series)


	def populate_episode_data(self):
		self.logger.log("Loading Episode Data")
		episode_uuids = set()
		for series in self.series:
			for episode in series.episodes:
				episode_uuids.add(f"'{episode.uuid}'")
		self.batches = self.batch_items(list(episode_uuids))
		for batch in self.batches:
			uuids = ','.join(batch)
			query = f"""
				SELECT
					dse.episode_key,
					dse.air_date,
					dse.length_in_seconds,
					sum(vv.active_seconds) / 3600.0 as hours_viewed,
					count(*) as views,
					count(distinct vv.user_key) as unique_viewers
				FROM warehouse.dim_segment_episode dse
				LEFT JOIN warehouse.vod_viewership vv
				ON dse.episode_key = vv.episode_key
				WHERE dse.episode_key IN ({uuids}) AND
					  vv.start_timestamp >= '{self.min_air_date}' AND
					  vv.start_timestamp < '{self.max_air_date}' AND
					  dse.air_date >= '{self.min_air_date}' AND
					  dse.air_date < '{self.max_air_date}'
				GROUP BY 1,2,3"""
			results = self.db_connector.read_redshift(query)
			for result in results:
				self.episode_data[result[0]] = {
					"air_date": result[1],
					"length": result[2],
					"hours_viewed": result[3],
					"views": result[4],
					"uvs": result[5]
				}


	def batch_items(self, items=[], batch_size=500):
		batches = []
		current_batch = []
		for item in items:
			if len(current_batch) == batch_size:
				batches.append(current_batch)
				current_batch = []
			current_batch.append(item)
		batches.append(current_batch)
		return batches


	def hydrate_episodes(self):
		self.logger.log("Hydrating Episodes")
		for series in self.series:
			for episode in series.episodes:
				if episode.uuid in self.episode_data:
					episode.hydrate(self.episode_data[episode.uuid])


	def hydrate_series(self):
		self.logger.log("Hydrating Series")
		for series in self.series:
			series.hydrate()


	def find_series(self, target_name):
		for series in self.series:
			if series.title.lower() == target_name.lower():
				return series


	def display_series(self):
		self.series.sort(key=lambda x: x.avg_uvs_per_appearance, reverse=True)
		for series in self.series:
			print(f"{series.title}: {series.avg_uvs_per_appearance}")


	def populate(self):
		self.populate_data()
		self.populate_episode_data()
		self.hydrate_episodes()
		self.hydrate_series()
		self.logger.log("Process Complete.")



