from collections import OrderedDict
from operator import getitem
import statistics as stats


class YTSeries:

	def __init__(self, title):
		self.title = title
		self.episodes = []
		self.null_data_episodes = []
		self.cast_combo_rollups = {}
		self.avg_episode_hours = 0
		self.median_episode_hours = 0
		self.avg_episode_views = 0
		self.median_episode_views = 0
		self.avg_cast_count = 0
		self.median_cast_count = 0
		self.avg_minutes_per_view = 0
		self.cast = {}


	def has_episode(self, video_id):
		for episode in self.episodes:
			if episode.video_id == video_id:
				return True
		return False


	def add_episode(self, episode):
		if not self.has_episode(episode.video_id):
			self.episodes.append(episode)


	def clean_episodes(self):
		self.null_data_episodes = [episode for episode in self.episodes if episode.views is None]
		self.episodes = [episode for episode in self.episodes if episode.views is not None]


	def populate_cast_combo_rollups(self):
		for episode in self.episodes:
			if len(episode.cast) > 1:
				episode.cast.sort()
				cast_str = '-'.join(episode.cast).replace(' ', '_')
				if cast_str not in self.cast_combo_rollups:
					self.cast_combo_rollups[cast_str] = {
						"occurrences": 1,
						"hours": 0,
						"views": 0,
						"episodes": [episode.title]
					}
					self.cast_combo_rollups[cast_str]['hours'] = int(episode.hours_viewed) if episode.hours_viewed else 0
					self.cast_combo_rollups[cast_str]['views'] = episode.views if episode.views else 0
				else:
					self.cast_combo_rollups[cast_str]['occurrences'] += 1
					self.cast_combo_rollups[cast_str]['hours'] += int(episode.hours_viewed) if episode.hours_viewed else 0
					self.cast_combo_rollups[cast_str]['views'] += episode.views if episode.views else 0
					self.cast_combo_rollups[cast_str]['episodes'].append(episode.title)
				self.cast_combo_rollups[cast_str]['avg_hours'] = int(self.cast_combo_rollups[cast_str]['hours'] / self.cast_combo_rollups[cast_str]['occurrences'])
				self.cast_combo_rollups[cast_str]['avg_views'] = int(self.cast_combo_rollups[cast_str]['views'] / self.cast_combo_rollups[cast_str]['occurrences'])


	def populate_stats(self):
		hours = []
		views = []
		cast_count = []
		for episode in self.episodes:
			if episode.hours_viewed:
				hours.append(episode.hours_viewed)
			if episode.views:
				views.append(episode.views)
			cast_count.append(len(episode.cast))
		self.avg_episode_hours = stats.mean(hours)
		self.median_episode_hours = stats.median(hours)
		self.avg_episode_views = stats.mean(views)
		self.median_episode_views = stats.median(views)
		self.avg_cast_count = stats.mean(cast_count)
		self.median_cast_count = stats.median(cast_count)
		self.avg_minutes_per_view = (sum(hours) / sum(views)) * 60


	def populate_cast(self):
		for episode in self.episodes:
			for cast_member in episode.cast:
				if cast_member not in self.cast:
					self.cast[cast_member] = {
						"appearances": 1,
						"attributed_hours": episode.hours_viewed if episode.hours_viewed else 0,
						"attributed_views": episode.views if episode.views else 0,
						"episode_air_dates": [episode.air_date]
					}
				else:
					self.cast[cast_member]["appearances"] += 1
					self.cast[cast_member]["attributed_hours"] += episode.hours_viewed if episode.hours_viewed else 0
					self.cast[cast_member]["attributed_views"] += episode.views if episode.views else 0
					self.cast[cast_member]["episode_air_dates"].append(episode.air_date)
				self.cast[cast_member]['views_per_appearance'] = int(self.cast[cast_member]["attributed_views"] / self.cast[cast_member]["appearances"]) if self.cast[cast_member]["appearances"] else 0
				self.cast[cast_member]['hours_per_appearance'] = int(self.cast[cast_member]["attributed_hours"] / self.cast[cast_member]["appearances"]) if self.cast[cast_member]["appearances"] else 0
				self.cast[cast_member]['avg_minutes_per_view'] = int((self.cast[cast_member]["hours_per_appearance"] / self.cast[cast_member]['views_per_appearance']) * 60)


	def score_cast(self):
		self.cast = OrderedDict(sorted(self.cast.items(), key = lambda x: getitem(x[1], 'views_per_appearance'), reverse=True))
		current_score = len(self.cast) + 1
		current_value = 0
		for member in self.cast:
			if self.cast[member]['views_per_appearance'] != current_value:
				current_score -= 1
				current_value = self.cast[member]['views_per_appearance']
			self.cast[member]['view_score'] = current_score
			self.cast[member]['views_per_appearance'] = self.cast[member]['views_per_appearance']

		self.cast = OrderedDict(sorted(self.cast.items(), key = lambda x: getitem(x[1], 'avg_minutes_per_view'), reverse=True))
		current_score = len(self.cast)
		current_value = 0
		for member in self.cast:
			if self.cast[member]['avg_minutes_per_view'] != current_value:
				current_score -= 1
				current_value = self.cast[member]['avg_minutes_per_view']
			self.cast[member]['minutes_score'] = current_score
			self.cast[member]['avg_minutes_per_view'] = self.cast[member]['avg_minutes_per_view']

		for member in self.cast:
			self.cast[member]['composite_score'] = stats.mean([self.cast[member]['view_score'], self.cast[member]['minutes_score']])


	def display_episodes(self):
		print(f"\n\nEpisodes:")
		self.episodes.sort(key=lambda x: x.views, reverse=True)
		for episode in self.episodes:
			episode.display()
			print("\n")


	def display_appearances(self):
		print("\nCast Appearances:")
		self.cast = OrderedDict(sorted(self.cast.items(), key=lambda x: getitem(x[1], 'appearances'), reverse=True))
		for member in self.cast:
			print(f"{member}: {self.cast[member]['appearances']}")


	def display_cast_combo_rollups(self):
		print("\n\nCast Combo Roll-up:")
		self.cast_combo_rollups = OrderedDict(sorted(self.cast_combo_rollups.items(), key = lambda x: getitem(x[1], 'avg_views'), reverse=True))
		for combo in self.cast_combo_rollups:
			combo_split = combo.split('-')
			members = []
			for member in combo_split:
				members.append(member.split('_')[0])
			print(f"{', '.join(members)}: {self.cast_combo_rollups[combo]['avg_views']} ({self.cast_combo_rollups[combo]['occurrences']})")


	def display_cast_scores(self):
		results = OrderedDict(sorted(self.cast.items(), key = lambda x: getitem(x[1], 'view_score'), reverse=True))
		print("\n\nSorted by Views per Appearance")
		for name in results:
			print(f"{name},{results[name]['views_per_appearance']}")

		results = OrderedDict(sorted(self.cast.items(), key = lambda x: getitem(x[1], 'minutes_score'), reverse=True))
		print("\nSorted by Avg Minutes per View")
		for name in results:
			print(f"{name},{results[name]['avg_minutes_per_view']}")

		results = OrderedDict(sorted(self.cast.items(), key = lambda x: getitem(x[1], 'composite_score'), reverse=True))
		print("\nSorted by Composite")
		for name in results:
			print(f"{name},{results[name]['composite_score']}")


	def report(self, display_episodes=True):
		if display_episodes:
			self.display_episodes()
		self.display_appearances()
		self.display_cast_combo_rollups()
		self.display_cast_scores()


	def hydrate(self):
		self.clean_episodes()
		if len(self.episodes) > 0:
			self.populate_cast_combo_rollups()
			self.populate_cast()
			self.populate_stats()
			self.score_cast()



