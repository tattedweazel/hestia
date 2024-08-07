from utils.components.number_formatter import num_format as nf
from base.etl_jobv3 import EtlJobV3
from utils.connectors.database_connector import DatabaseConnector  # TODO: REMOVE AFTER TESTING


class LivestreamWeeklyNarrativesJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = '', local_mode=True)
		self.twitch_livestreams_df = {}
		self.youtube_livestreams_df = {}
		# Thresholds
		self.WEEK_AVERAGE = 16
		self.DAYS_FOR_LIVE_CONTENT = 16  # This is the max number of days that is available between the start week of the latest scrape and when we run this job

		self.db_connector = DatabaseConnector('')  # TODO: REMOVE AFTER TESTING


	def twitch_livestreams(self):
		self.loggerv3.info('Twitch Livestreams')
		query = f"""
		WITH last_16_cte_pt1 as (
			SELECT
				channel,
				date_part('year', stream_start) as year,
				(CASE
					WHEN date_part('dayofweek', stream_start) = 0 AND date_part('week', stream_start) = 52 THEN 1
					WHEN date_part('dayofweek', stream_start) = 0 THEN date_part('week', stream_start) + 1
					ELSE date_part('week', stream_start)
				END) as week,
				avg(unique_viewers) as avg_uvs,
				avg(average_concurrents) as avg_concurrents
			FROM warehouse.twitch_stream_metrics
			WHERE stream_start > current_date - interval '{self.WEEK_AVERAGE} week'
				AND channel in ('rwby vt', 'inside gaming')
			GROUP BY 1, 2, 3
		), last_16_cte_pt2 as (
			SELECT *,
				   lead(avg_uvs, 1) over (partition by channel order by year desc, week desc) as prev_avg_uvs,
				   lead(avg_concurrents, 1) over (partition by channel order by year desc, week desc) as prev_avg_concurrents,
				   row_number() over (partition by channel order by year desc, week desc) as row_num
			FROM last_16_cte_pt1
		), latest_week_cte as (
			SELECT
				channel,
				avg_uvs as latest_avg_uvs,
				avg_concurrents as latest_avg_concurrents
			FROM last_16_cte_pt2
			WHERE row_num = 1
		), stats_cte as (
			SELECT
				channel,
				avg(avg_uvs) as avg_avg_uvs,
				avg(avg_concurrents) as avg_avg_concurrents
			FROM last_16_cte_pt2
			GROUP BY 1
		)
		SELECT
			s.channel,
			s.avg_avg_uvs,
			s.avg_avg_concurrents,
			lw.latest_avg_uvs,
			lw.latest_avg_concurrents
		FROM stats_cte s
		INNER JOIN latest_week_cte lw on lw.channel = s.channel;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			channel = result[0].strip()
			data = {
				'avg_avg_uvs': result[1],
				'avg_avg_concurrents': result[2],
				'latest_avg_uvs': result[3],
				'latest_avg_concurrents': result[4]
			}
			self.twitch_livestreams_df[channel] = data


	def youtube_livestreams(self):
		self.loggerv3.info('YouTube Livestreams')
		query = f"""
		WITH last_16_cte as (
			SELECT
				channel_title,
				start_week,
				avg(unique_viewers) as avg_uvs,
				avg(avg_concurrents) as avg_acvs
			FROM warehouse.yt_studio_weekly_metrics
			WHERE (channel_title in ('Funhaus') and date_part('dayofweek', published_at) IN (2, 4, 5))
				AND start_week > current_date - interval '{self.WEEK_AVERAGE} week'
				AND duration_in_seconds > 1800
				AND avg_concurrents > 0
			GROUP BY 1, 2
		), latest_week_cte as (
			SELECT
				channel_title,
				start_week,
				avg_uvs as latest_avg_uvs,
				avg_acvs as latest_avg_concurrents
			FROM last_16_cte
			WHERE date_diff('days', cast(start_week as date), current_date) <= {self.DAYS_FOR_LIVE_CONTENT}
		), avgs_cte as (
			SELECT
				channel_title,
				avg(avg_uvs) as avg_avg_uvs,
				avg(avg_acvs) as avg_avg_concurrents
			FROM last_16_cte
			GROUP BY 1
		)
		SELECT
			a.channel_title,
			a.avg_avg_uvs,
			a.avg_avg_concurrents,
			lw.latest_avg_uvs,
			lw.latest_avg_concurrents
		FROM latest_week_cte lw
		INNER JOIN avgs_cte a on a.channel_title = lw.channel_title;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			channel = result[0].strip()
			data = {
				'avg_avg_uvs': result[1],
				'avg_avg_concurrents': result[2],
				'latest_avg_uvs': result[3],
				'latest_avg_concurrents': result[4]
			}
			self.youtube_livestreams_df[channel] = data


	def build_narrative(self):
		self.loggerv3.info('Building Narrative')
		narrative = f"~LIVESTREAM~\n\n"

		# Trajectories
		narrative += f"Downloads from the last month were "
		if self.trajectories_df[series] < 0:
			narrative += f"down {self.trajectories_df[series]}% below averages. "
		elif self.trajectories_df[series] > 0:
			narrative += f"up {self.trajectories_df[series]}% above averages. "
		else:
			narrative += "steady compared with averages. "

		# VOD Weekly Content
		if series_lower in self.weekly_content_df:
			weekly_data = self.weekly_content_df[series_lower]
			narrative += "\n\n"
			narrative += f"""Last week's episode \"{weekly_data['episode_title'][:self.CHAR_LIMIT]}...\" is expected to hit {nf(weekly_data['forecasted_plus_actual'], 0)} downloads, """
			if weekly_data['forecasted_plus_actual'] - weekly_data['target'] > self.VIEWS_DELTA_THRESHOLD:
				narrative += f"""{nf(weekly_data['forecasted_plus_actual'] - weekly_data['target'], 1)} above the target. """
			elif weekly_data['forecasted_plus_actual'] - weekly_data['target'] < -1 * self.VIEWS_DELTA_THRESHOLD:
				narrative += f"""{nf(weekly_data['forecasted_plus_actual'] - weekly_data['target'], 1)} below the target. """
			else:
				narrative += f"hitting the sales target. "

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
		self.loggerv3.info(f"Running Livestream Weekly Narratives Job")
		self.twitch_livestreams()
		self.youtube_livestreams()
		self.build_narrative()
		self.loggerv3.success("All Processing Complete!")
