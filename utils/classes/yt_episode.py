
class YTEpisode:

	def __init__(self, episode_str):
		episode_pieces = episode_str.split('/-/')
		self.uuid = episode_pieces[0]
		self.title = episode_pieces[1]
		self.video_id = episode_pieces[2]
		self.cast = []
		self.air_date = None
		self.episode_length = None
		self.hours_viewed = None
		self.views = None
		self.game_name = None
		self.show_name = None


	def add_to_cast(self, cast_member):
		self.cast.append(cast_member)


	def display(self):
		print(f"UUID: {self.uuid}")
		print(f"Video ID: {self.video_id}")
		print(f"Title: {self.title}")
		print(f"YT Air Date: {self.air_date}")
		print(f"Cast: {', '.join(self.cast)}")
		print(f"Views: {self.views}")
		print(f"Hours Viewed: {self.hours_viewed}")
		print(f"Game Name: {self.game_name}")
		print(f"Show Name: {self.show_name}")


	def output(self):
		return {
			'title': self.title,
			'yt_air_date': self.air_date,
			'cast': ', '.join(self.cast),
			'cast_members': len(self.cast),
			'views': self.views,
			'hours_viewed': self.hours_viewed,
			'game_name': self.game_name,
			'show_name': self.show_name
		}


	def hydrate(self, metadata):
		self.air_date = metadata['air_date']
		self.hours_viewed = metadata['hours_viewed']
		self.views = metadata['views']
		self.game_name = metadata['game_name'] if 'game_name' in metadata else None
		self.show_name = metadata['show_name'] if 'show_name' in metadata else None
