from collections import OrderedDict
from operator import getitem
import statistics as stats


class Series:

	def __init__(self, title):
		self.title = title
		self.episodes = []
		self.null_data_episodes = []
		self.cast_combo_rollups = {}
		self.cast_combos = []
		self.avg_episode_hours = 0
		self.median_episode_hours = 0
		self.avg_episode_views = 0
		self.median_episode_views = 0
		self.number_of_episodes = 0
		self.avg_cast_count = 0
		self.median_cast_count = 0
		self.max_uvs_per_appearance = 0
		self.avg_uvs_per_appearance = 0
		self.median_uvs_per_appearance = 0
		self.min_uvs_per_appearance = 0
		self.avg_minutes_per_uv = 0
		self.cast = {}


	def has_episode(self, uuid):
		for episode in self.episodes:
			if episode.uuid == uuid:
				return True
		return False


	def add_episode(self, episode):
		if not self.has_episode(episode.uuid):
			self.episodes.append(episode)


	def clean_episodes(self):
		self.null_data_episodes = [episode for episode in self.episodes if episode.views is None]
		self.episodes = [episode for episode in self.episodes if episode.views is not None]


	def populate_cast_combos(self):
		for episode in self.episodes:
			episode.cast.sort()
			cast_str = '-'.join(episode.cast).replace(' ', '_')
			self.cast_combos.append({
				"combo": cast_str,
				"hours": int(episode.hours_viewed) if episode.hours_viewed else 0,
				"views": episode.views if episode.views else 0,
				"uvs": episode.uvs if episode.uvs else 0
			})


	def populate_cast_combo_rollups(self):
		for episode in self.episodes:
			episode.cast.sort()
			cast_str = '-'.join(episode.cast).replace(' ', '_')
			if cast_str not in self.cast_combo_rollups:
				self.cast_combo_rollups[cast_str] = {
					"occurrences": 1,
					"hours": 0,
					"views": 0,
					"uvs": 0,
					"episodes": [episode.title]
				}
				self.cast_combo_rollups[cast_str]['hours'] = int(episode.hours_viewed) if episode.hours_viewed else 0
				self.cast_combo_rollups[cast_str]['views'] = episode.views if episode.views else 0
				self.cast_combo_rollups[cast_str]['uvs'] = episode.uvs if episode.uvs else 0
			else:
				self.cast_combo_rollups[cast_str]['occurrences'] += 1
				self.cast_combo_rollups[cast_str]['hours'] += int(episode.hours_viewed) if episode.hours_viewed else 0
				self.cast_combo_rollups[cast_str]['views'] += episode.views if episode.views else 0
				self.cast_combo_rollups[cast_str]['uvs'] += episode.uvs if episode.uvs else 0
				self.cast_combo_rollups[cast_str]['episodes'].append(episode.title)
			self.cast_combo_rollups[cast_str]['avg_hours'] = int(self.cast_combo_rollups[cast_str]['hours'] / self.cast_combo_rollups[cast_str]['occurrences'])
			self.cast_combo_rollups[cast_str]['avg_views'] = int(self.cast_combo_rollups[cast_str]['views'] / self.cast_combo_rollups[cast_str]['occurrences'])
			self.cast_combo_rollups[cast_str]['avg_uvs'] = int(self.cast_combo_rollups[cast_str]['uvs'] / self.cast_combo_rollups[cast_str]['occurrences'])


	def populate_stats(self):
		hours = []
		views = []
		cast_count = []
		uvs_per_appearance = []
		for episode in self.episodes:
			if episode.hours_viewed:
				hours.append(episode.hours_viewed)
			if episode.views:
				views.append(episode.views)
			cast_count.append(len(episode.cast))
		for combo in self.cast_combo_rollups:
			uvs_per_appearance.append(self.cast_combo_rollups[combo]['uvs'])
		self.number_of_episodes = len(self.episodes)
		self.avg_episode_hours = stats.mean(hours)
		self.median_episode_hours = stats.median(hours)
		self.avg_episode_views = stats.mean(views)
		self.median_episode_views = stats.median(views)
		self.avg_cast_count = stats.mean(cast_count)
		self.median_cast_count = stats.median(cast_count)
		self.max_uvs_per_appearance = max(uvs_per_appearance)
		self.avg_uvs_per_appearance = stats.mean(uvs_per_appearance)
		self.median_uvs_per_appearance = stats.median(uvs_per_appearance)
		self.min_uvs_per_appearance = min(uvs_per_appearance)
		self.avg_minutes_per_uv = int((int(self.avg_episode_hours) / int(self.avg_uvs_per_appearance)) * 60)


	def populate_cast(self):
		for episode in self.episodes:
			for cast_member in episode.cast:
				if cast_member not in self.cast:
					self.cast[cast_member] = {
						"appearances": 1,
						"attributed_hours": episode.hours_viewed if episode.hours_viewed else 0,
						"attributed_views": episode.views if episode.views else 0,
						"uvs": episode.uvs
					}
				else:
					self.cast[cast_member]["appearances"] += 1
					self.cast[cast_member]["attributed_hours"] += episode.hours_viewed if episode.hours_viewed else 0
					self.cast[cast_member]["attributed_views"] += episode.views if episode.views else 0
					self.cast[cast_member]["uvs"] += episode.uvs
				self.cast[cast_member]['uvs_per_appearance'] = int(self.cast[cast_member]["uvs"] / self.cast[cast_member]["appearances"]) if self.cast[cast_member]["appearances"] else 0
				self.cast[cast_member]['hours_per_appearance'] = int(self.cast[cast_member]["attributed_hours"] / self.cast[cast_member]["appearances"]) if self.cast[cast_member]["appearances"] else 0
				self.cast[cast_member]['avg_minutes_per_viewer'] = int((self.cast[cast_member]["hours_per_appearance"] / self.cast[cast_member]['uvs_per_appearance']) * 60)


	def get_stats(self):
		return {
			'Number Of Episodes': self.number_of_episodes,
			'Avg Cast Count': round(self.avg_cast_count),
			'Avg Unique Viewers Per Episode': round(self.avg_uvs_per_appearance)
		}


	def score_cast(self):
		self.cast = OrderedDict(sorted(self.cast.items(), key = lambda x: getitem(x[1], 'uvs_per_appearance'), reverse=True))
		current_score = len(self.cast) + 1
		current_value = 0
		for member in self.cast:
			if self.cast[member]['uvs_per_appearance'] != current_value:
				current_score -= 1
				current_value = self.cast[member]['uvs_per_appearance']
			self.cast[member]['uv_score'] = current_score
			self.cast[member]['uvs_per_appearance'] = self.cast[member]['uvs_per_appearance']

		self.cast = OrderedDict(sorted(self.cast.items(), key = lambda x: getitem(x[1], 'avg_minutes_per_viewer'), reverse=True))
		current_score = len(self.cast)
		current_value = 0
		for member in self.cast:
			if self.cast[member]['avg_minutes_per_viewer'] != current_value:
				current_score -= 1
				current_value = self.cast[member]['avg_minutes_per_viewer']
			self.cast[member]['minutes_score'] = current_score
			self.cast[member]['avg_minutes_per_viewer'] = self.cast[member]['avg_minutes_per_viewer']

		for member in self.cast:
			self.cast[member]['composite_score'] = stats.mean([self.cast[member]['uv_score'], self.cast[member]['minutes_score']])


	def display_stats(self):
		print(f"\n\nStats for {self.title}:")
		series_stats = self.get_stats()
		for stat in series_stats:
			print(f"{stat.replace('_', ' ').title()}: {series_stats[stat]}")


	def display_episodes(self):
		print(f"\n\nEpisodes:")
		self.episodes.sort(key=lambda x: x.uvs, reverse=True)
		for episode in self.episodes:
			episode.display()
			print("\n")


	def display_appearances(self):
		print("\nCast Appearances:")
		for member in self.cast:
			print(f"{member}: {self.cast[member]['appearances']}")


	def display_cast_combos(self):
		print("\n\nAll Cast Combos:")
		self.cast_combos.sort(key=lambda x: x['uvs'], reverse=True)
		for combo in self.cast_combos:
			combo_split = combo['combo'].split('-')
			members = []
			for member in combo_split:
				members.append(member.split('_')[0])
			print(f"{', '.join(members)}: {combo['uvs']}")


	def display_cast_combo_rollups(self):
		print("\n\nCast Combo Roll-up:")
		self.cast_combo_rollups = OrderedDict(sorted(self.cast_combo_rollups.items(), key = lambda x: getitem(x[1], 'avg_uvs'), reverse=True))
		for combo in self.cast_combo_rollups:
			combo_split = combo.split('-')
			members = []
			for member in combo_split:
				members.append(member.split('_')[0])
			print(f"{', '.join(members)}: {self.cast_combo_rollups[combo]['avg_uvs']} ({self.cast_combo_rollups[combo]['occurrences']})")



	def report(self):
		self.display_stats()
		self.display_episodes()
		self.display_appearances()
		self.display_cast_combo_rollups()
		self.display_cast_combos()


	def hydrate(self):
		self.clean_episodes()
		self.populate_cast_combos()
		self.populate_cast_combo_rollups()
		self.populate_cast()
		self.populate_stats()
		self.score_cast()
