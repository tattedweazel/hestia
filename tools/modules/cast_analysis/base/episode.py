from utils.connectors.database_connector import DatabaseConnector


class Episode:

	def __init__(self, episode_str):
		self.db_connector = DatabaseConnector('')
		episode_pieces = episode_str.split('/-/')
		self.uuid = episode_pieces[0]
		self.title = episode_pieces[1]
		self.cast = []
		self.air_date = None
		self.episode_length = None
		self.hours_viewed = None
		self.views = None
		self.uvs = None


	def add_to_cast(self, cast_member):
		self.cast.append(cast_member)

	def display(self):
		print(f"Title: {self.title}")
		print(f"Air Date: {self.air_date}")
		print(f"Cast: {', '.join(self.cast)}")
		print(f"Unique Viewers: {self.uvs}")


	def output(self):
		return {
			'title': self.title,
			'air_date': self.air_date,
			'cast': ', '.join(self.cast),
			'cast_members': len(self.cast),
			'uvs': self.uvs,
			'hours_viewed': self.hours_viewed,
			'views': self.views
		}


	def hydrate(self, metadata):
		self.air_date = metadata['air_date']
		self.episode_length = metadata['length']
		self.hours_viewed = metadata['hours_viewed']
		self.views = metadata['views']
		self.uvs = metadata['uvs']
