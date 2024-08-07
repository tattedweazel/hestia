from utils.components.number_formatter import num_format as nf
from base.etl_jobv3 import EtlJobV3
from utils.connectors.database_connector import DatabaseConnector  # TODO: REMOVE AFTER TESTING


class AudioWeeklyNarrativesJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = '', local_mode=True)
		self.trajectories_df = {}
		self.weekly_content_df = {}
		self.weekly_totals_df = {}
		self.PODCASTS = ('30 morbid minutes', 'anma', 'so... alright', 'f**kface', 'red web', 'face jam',
						 'death battle cast', 'tales from the stinky dragon', 'rooster teeth podcast')
		# Thresholds
		self.WEEK_AVERAGE = 16
		self.MIN_DAYS_TO_CONSIDER_EPISODE = 5
		self.POSITIVE_TREND = 2
		self.NEGATIVE_TREND = -2
		self.VIEWS_DELTA_THRESHOLD = 1000  # Only call out the views delta between latest week and average if at least this many views
		self.CHAR_LIMIT = 40  # The number of characters to show for a VOD title



		self.db_connector = DatabaseConnector('')  # TODO: REMOVE AFTER TESTING


	def trajectories(self):
		self.loggerv3.info('Trajectories')
		query = f"""
		WITH cte as (
			SELECT
				cast(run_date as timestamp) as run_date,
				podcast_title,
				trajectory,
				row_number() over (partition by podcast_title order by run_date desc) as row_num
			FROM warehouse.audio_trajectory
			WHERE lower(podcast_title) in {self.PODCASTS}
		)
		SELECT
			podcast_title,
			trajectory
		FROM cte
		WHERE row_num = 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			series = result[0].strip()
			self.trajectories_df[series] = int(result[1]*100)


	def weekly_content(self):
		self.loggerv3.info('Weekly Content')
		query = f"""
		WITH last_16_cte as (
			SELECT
				series_title,
				episode_title,
				episode_id,
				air_date,
				max(days_since_pub) as max_days_since_pub,
				sum(views) as views
			FROM warehouse.agg_daily_sales_metrics
			WHERE platform NOT IN ('rooster_teeth', 'youtube')
				AND lower(series_title) in {self.PODCASTS}
				AND air_date > current_date - interval '{self.WEEK_AVERAGE} week'
				AND days_since_pub <= 45
			GROUP BY 1, 2, 3, 4
		), latest_week_cte as (
			SELECT
				l.series_title,
				l.episode_title,
				l.air_date,
				l.views,
				smef.forecast as forcasted_views,
				l.max_days_since_pub
			FROM last_16_cte l
			LEFT JOIN warehouse.sales_metrics_episode_forecasts smef on smef.episode_id = l.episode_id
			WHERE max_days_since_pub >= 3 and max_days_since_pub <= 7
		), avgs_cte as (
			SELECT
				l.series_title,
				avg(views + (CASE WHEN smef.forecast is NULL THEN 0 ELSE smef.forecast END)) as avg_views
			FROM last_16_cte l
			LEFT JOIN warehouse.sales_metrics_episode_forecasts smef on smef.episode_id = l.episode_id
			WHERE max_days_since_pub >= 7
			GROUP BY 1
		)
		SELECT
			lw.series_title,
			lw.episode_title,
			lw.views,
			lw.forcasted_views,
			a.avg_views
		FROM latest_week_cte lw
		INNER JOIN avgs_cte a on a.series_title = lw.series_title
		ORDER BY series_title, views desc;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			series = result[0].strip()
			data = {
				'episode_title': result[1],
				'views': result[2],
				'forecasted_views': result[3],
				'avg_views': result[4],
				'forecasted_plus_actual': result[2] + result[3]
			}
			self.weekly_content_df[series] = data


	def weekly_podcast_totals(self):
		self.loggerv3.info('Weekly Podcast Totals')
		query = f"""
		WITH last_16_cte as (
			SELECT
				series_title,
				date_part('year', cast(viewership_date as date)) as year,
				(CASE
					WHEN date_part('dayofweek', cast(viewership_date as date)) = 0 AND date_part('week', cast(viewership_date as date)) = 52 THEN 1
					WHEN date_part('dayofweek', cast(viewership_date as date)) = 0 THEN date_part('week', cast(viewership_date as date)) + 1
					ELSE date_part('week', cast(viewership_date as date))
				END) as week,
				sum(views) as views
			FROM warehouse.agg_daily_sales_metrics
			WHERE platform NOT IN ('rooster_teeth', 'youtube')
				AND lower(series_title) in {self.PODCASTS}
				AND viewership_date > current_date - interval '{self.WEEK_AVERAGE} week'
				AND viewership_date < current_date - cast(date_part('dow', current_date) as int)
			GROUP BY 1, 2, 3
		), identify_latest_week_cte as (
			SELECT
				series_title,
				year,
				week,
				views,
				lead(views, 1) over (partition by series_title order by year desc, week desc) as prev_views,
				row_number() over (partition by series_title order by year desc, week desc) as row_num
			FROM last_16_cte
		), latest_week_cte as (
			SELECT series_title, views
			FROM identify_latest_week_cte
			WHERE row_num = 1
		), avgs_cte as (
			SELECT
				series_title,
				avg(views) as avg_views
			FROM last_16_cte
			GROUP BY 1
		), indicator_cte as (
			SELECT
				series_title,
				year,
				week,
				views,
				prev_views,
				(CASE
					WHEN ((1.0 * (views - prev_views)) / prev_views) * 100 >= 1 THEN 1
					WHEN ((1.0 * (views - prev_views)) / prev_views) * 100 <= -1 THEN -1
					ELSE 0
				END) as indicator
			FROM identify_latest_week_cte
		), trend_cte as (
			SELECT
				series_title,
				views,
				prev_views,
				indicator + lead(indicator, 1) over (partition by series_title order by year desc, week desc) as trend,
				row_number() over (partition by series_title order by year desc, week desc) as row_num
			FROM indicator_cte
		), latest_trend_cte as (
			SELECT
				series_title,
				trend
			FROM trend_cte
			WHERE row_num = 1
		)
		SELECT
			lw.series_title,
			lw.views,
			a.avg_views,
			lt.trend
		FROM latest_week_cte lw
		INNER JOIN avgs_cte a on a.series_title = lw.series_title
		INNER JOIN latest_trend_cte lt on lt.series_title = lw.series_title
		ORDER BY series_title desc;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			series = result[0].strip()
			data = {
				'views': result[1],
				'avg_views': result[2],
				'trend': result[3]
			}
			self.weekly_totals_df[series] = data


	def build_narrative(self, series):
		self.loggerv3.info(f'Building narrative for {series}')
		series_lower = series.lower().strip()
		narrative = f"~AUDIO~\n\n"

		# Trajectories
		narrative += f"Downloads from the last month were "
		if self.trajectories_df[series] < 0:
			narrative += f"{self.trajectories_df[series]}% below averages. "
		elif self.trajectories_df[series] > 0:
			narrative += f"{self.trajectories_df[series]}% above averages. "
		else:
			narrative += "steady compared to averages. "

		# VOD Weekly Content
		if series_lower in self.weekly_content_df:
			weekly_data = self.weekly_content_df[series_lower]
			narrative += "\n\n"
			narrative += f"""Last week's episode \"{weekly_data['episode_title'][:self.CHAR_LIMIT]}...\" is expected to hit {nf(weekly_data['forecasted_plus_actual'], 0)} downloads, """
			if weekly_data['forecasted_plus_actual'] > weekly_data['avg_views']:
				narrative += f"""{nf(weekly_data['forecasted_plus_actual'] - weekly_data['avg_views'], 1)} above the 45-day expected average. """
			elif weekly_data['forecasted_plus_actual'] < weekly_data['avg_views']:
				narrative += f"""{nf(weekly_data['forecasted_plus_actual'] - weekly_data['avg_views'], 1)} below the 45-day expected average. """
			else:
				narrative += f"at the 45-day expected average. "

		# Weekly Totals
		if series_lower in self.weekly_totals_df:
			totals_data = self.weekly_totals_df[series_lower]
			narrative += f"""The total weekly podcast downloads is at {nf(totals_data['views'], 0)}, """
			if totals_data['views'] - totals_data['avg_views'] > self.VIEWS_DELTA_THRESHOLD:
				narrative += f"""{nf(totals_data['views'] - totals_data['avg_views'], 1)} above averages"""
			elif totals_data['views'] - totals_data['avg_views'] < -1* self.VIEWS_DELTA_THRESHOLD:
				narrative += f"""{nf(totals_data['views'] - totals_data['avg_views'], 1)} below averages"""
			else:
				narrative += f"at averages"

			# UV Trend
			if totals_data['trend'] >= self.POSITIVE_TREND:
				if totals_data['views'] > totals_data['avg_views']:
					narrative += " and is "
				else:
					narrative += " despite being "
				narrative += f"""on a {totals_data['trend']}-week upward trend. """
			elif totals_data['trend'] <= self.NEGATIVE_TREND:
				if totals_data['views'] > totals_data['avg_views']:
					narrative += " despite being "
				else:
					narrative += " and is "
				narrative += f"""on a {abs(totals_data['trend'])}-week downward trend. """
			else:
				narrative += ". "

		# Write Initial Narratives to File
		narrative += '\n\n'
		narrative += '-'*125
		narrative += '\n\n'
		with open(f'downloads/narratives/audio/{series_lower}.txt', 'a+') as f:
			f.write(f'{narrative}\n\n')


	def execute(self):
		self.loggerv3.info(f"Running Audio Weekly Narratives Job")
		self.trajectories()
		self.weekly_content()
		self.weekly_podcast_totals()
		for series in self.trajectories_df.keys():
			self.build_narrative(series)
		self.loggerv3.success("All Processing Complete!")
