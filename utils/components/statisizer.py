import statistics as st


class Statisizer:

	def __init__(self, records, cohort_size, cohort_date):
		self.records = records
		self.total_cohort_size = cohort_size
		self.cohort_date = cohort_date

		self.sample_size = len(records)
		self.median_platforms = 0
		self.median_channels = 0
		self.median_series = 0
		self.median_episodes = 0
		self.median_views = 0
		self.median_visits = 0
		self.median_hours = 0
		self.median_active_view_days = 0
		self.median_active_visit_days = 0
		self.median_active_days = 0


	def tally_platforms(self):
		totals = []
		for record in self.records:
			totals.append(record['platforms_watched'])
		self.median_platforms = st.median(totals)


	def tally_channels(self):
		totals = []
		for record in self.records:
			totals.append(record['channels_watched'])
		self.median_channels = st.median(totals)


	def tally_series(self):
		totals = []
		for record in self.records:
			totals.append(record['series_watched'])
		self.median_series = st.median(totals)


	def tally_episodes(self):
		totals = []
		for record in self.records:
			totals.append(record['episodes_watched'])
		self.median_episodes = st.median(totals)


	def tally_views(self):
		totals = []
		for record in self.records:
			totals.append(record['total_views'])
		self.median_views = st.median(totals)


	def tally_visits(self):
		totals = []
		for record in self.records:
			totals.append(record['total_visits'])
		self.median_visits = st.median(totals)


	def tally_hours(self):
		totals = []
		for record in self.records:
			totals.append(record['hours_watched'])
		self.median_hours = st.median(totals)


	def tally_active_view_days(self):
		totals = []
		for record in self.records:
			totals.append(record['days_viewed'])
		self.median_active_view_days = st.median(totals)


	def tally_active_visit_days(self):
		totals = []
		for record in self.records:
			totals.append(record['days_visited'])
		self.median_active_visit_days = st.median(totals)


	def tally_active_days(self):
		totals = []
		for record in self.records:
			totals.append(record['days_active'])
		self.median_active_days = st.median(totals)


	def as_object(self):
		return {
			"cohort_date": self.cohort_date,
			"total_cohort_size": self.total_cohort_size,
			"sample_size": self.sample_size,
			"median_platforms": self.median_platforms,
			"median_channels": self.median_channels,
			"median_series": self.median_series,
			"median_episodes": self.median_episodes,
			"median_views": self.median_views,
			"median_visits": self.median_visits,
			"median_hours": self.median_hours,
			"median_days_viewed": self.median_active_view_days,
			"median_days_visited": self.median_active_visit_days,
			"median_active_days": self.median_active_days
		}


	def execute(self):
		self.tally_platforms()
		self.tally_channels()
		self.tally_series()
		self.tally_episodes()
		self.tally_views()
		self.tally_visits()
		self.tally_hours()
		self.tally_active_view_days()
		self.tally_active_visit_days()
		self.tally_active_days()
