from utils.components.number_formatter import num_format as nf
from base.etl_jobv3 import EtlJobV3
from utils.connectors.database_connector import DatabaseConnector  # TODO: REMOVE AFTER TESTING


class SiteWeeklyNarrativesJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = '', local_mode=True)
		self.trajectories_df = {}
		self.uv_overview_df = {}
		self.uv_deep_dive_df = {}
		self.vod_weekly_content_df = {}
		self.STAND_ALONE_FILTERED_SERIES = ('Let''s Play', '30 Morbid Minutes', 'ANMA Podcast', 'Face Jam', 'Rooster Teeth Podcast', 'So...Alright', 'Hypothetical Nonsense')
		self.STAND_ALONE_SERIES = ['Let\'s Play', '30 Morbid Minutes', 'ANMA Podcast', 'Face Jam', 'Rooster Teeth Podcast', 'So...Alright', 'Hypothetical Nonsense']
		# Thresholds
		self.WEEK_AVERAGE = 16
		self.DAYS_FOR_VOD_CONTENT = 16  # This is the max number of days that is available between the start week of the latest scrape and when we run this job
		self.TREND_THRESHOLD = 2
		self.UV_DELTA_THRESHOLD = 5  # Only call out the UV delta between latest week and average if at least this %
		self.COMPLETION_RATE_DELTA_THRESHOLD = 2  # Only call out the completion rate delta between latest week and average if at least this %
		self.NEW_VIEWER_PCT_THRESHOLD = 5  # Onl call out New Viewer % delta between latest week and average if at least this %
		self.CHAR_LIMIT = 35  # The number of characters to show for a VOD title



		self.db_connector = DatabaseConnector('')  # TODO: REMOVE AFTER TESTING


	def trajectories(self):
		self.loggerv3.info('Trajectories')
		query = f"""
		WITH cte as (
			SELECT
				cast(run_date as timestamp) as run_date,
				channel_title,
				trajectory,
				row_number() over (partition by channel_title order by run_date desc) as row_num
			FROM warehouse.site_channel_trajectory
			WHERE channel_title NOT IN ('Squad Team Force', 'Kinda Funny', 'Friends of RT')
			UNION
			SELECT
				cast(run_date as timestamp) as run_date,
				series_title as channel_title,
				trajectory,
				row_number() over (partition by series_title order by run_date desc) as row_num
			FROM warehouse.site_series_catalog_trajectory
			WHERE series_title IN {self.STAND_ALONE_FILTERED_SERIES} OR series_title = 'Let'\'s Play'
		)
		SELECT
			channel_title,
			trajectory
		FROM cte
		WHERE row_num = 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			channel = result[0].strip()
			self.trajectories_df[channel] = int(result[1]*100)


	def uv_overview(self):
		self.loggerv3.info('UV Overview')
		query = f"""
		WITH last_16_cte_channel as (
			SELECT
				dse.channel_title,
				date_part('year', vv.start_timestamp) as year,
				(CASE
					WHEN date_part('dayofweek', cast(vv.start_timestamp as date)) = 0 AND date_part('week', cast(vv.start_timestamp as date)) = 52 THEN 1
					WHEN date_part('dayofweek', cast(vv.start_timestamp as date)) = 0 THEN date_part('week', cast(vv.start_timestamp as date)) + 1
					ELSE date_part('week', cast(vv.start_timestamp as date))
				END) as week,
				count(distinct cast(vv.start_timestamp as varchar(10))) as days_in_week,
				count(distinct coalesce(cast(vv.user_key as varchar), vv.anonymous_id)) as unique_viewers
			FROM warehouse.vod_viewership vv
			INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
			WHERE channel_title NOT IN ('Squad Team Force', 'Kinda Funny', 'Friends of RT')
				AND start_timestamp > current_date - interval '{self.WEEK_AVERAGE} week'
			GROUP BY 1, 2, 3
		), last_16_cte_series as (
				SELECT
				dse.series_title as channel_title,
				(CASE
					WHEN date_part('dayofweek', cast(vv.start_timestamp as date)) = 0 AND date_part('week', cast(vv.start_timestamp as date)) = 52 THEN date_part('year', vv.start_timestamp) + 1
					ELSE date_part('year', vv.start_timestamp)
				END) as year,
				(CASE
					WHEN date_part('dayofweek', cast(vv.start_timestamp as date)) = 0 AND date_part('week', cast(vv.start_timestamp as date)) = 52 THEN 1
					WHEN date_part('dayofweek', cast(vv.start_timestamp as date)) = 0 THEN date_part('week', cast(vv.start_timestamp as date)) + 1
					ELSE date_part('week', cast(vv.start_timestamp as date))
				END) as week,
				count(distinct cast(vv.start_timestamp as varchar(10))) as days_in_week,
				count(distinct coalesce(cast(vv.user_key as varchar), vv.anonymous_id)) as unique_viewers
			FROM warehouse.vod_viewership vv
			INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
			WHERE (series_title IN {self.STAND_ALONE_FILTERED_SERIES} OR series_title = 'Let'\'s Play')
				AND start_timestamp > current_date - interval '{self.WEEK_AVERAGE} week'
			GROUP BY 1, 2, 3
		), last_16_cte_pt1 as (
			SELECT *
			FROM last_16_cte_channel
			UNION
			SELECT *
			FROM last_16_cte_series
		), last_16_cte_pt2 as (
			SELECT *,
				   lead(unique_viewers, 1) over (partition by channel_title order by year desc, week desc) as prev_unique_viewers,
				   row_number() over (partition by channel_title order by year desc, week desc) as row_num
			FROM last_16_cte_pt1
			WHERE days_in_week = 7
		), latest_week_cte as (
			SELECT
				channel_title,
				unique_viewers as latest_unique_viewers
			FROM last_16_cte_pt2
			WHERE row_num = 1
		), stats_cte as (
			SELECT
				channel_title,
				max(unique_viewers) as max_unique_viewers,
				avg(unique_viewers) as avg_unique_viewers,
				min(unique_viewers) as min_unique_viewers
			FROM last_16_cte_pt2
			GROUP BY 1
		), second_highest_cte as (
			SELECT
				l.channel_title,
				max(l.unique_viewers) as second_highest_uvs
			FROM last_16_cte_pt2 l
			INNER JOIN stats_cte s on s.channel_title = l.channel_title and s.max_unique_viewers != l.unique_viewers
			GROUP BY 1
		), second_lowest_cte as (
			SELECT
				l.channel_title,
				min(l.unique_viewers) as second_lowest_uvs
			FROM last_16_cte_pt2 l
			INNER JOIN stats_cte s on s.channel_title = l.channel_title and s.min_unique_viewers != l.unique_viewers
			GROUP BY 1
		), indicator_cte as (
			SELECT
				channel_title,
				year,
				week,
				unique_viewers,
				prev_unique_viewers,
				(CASE
					WHEN ((1.0 * (unique_viewers - prev_unique_viewers)) / prev_unique_viewers) * 100 >= 1 THEN 1
					WHEN ((1.0 * (unique_viewers - prev_unique_viewers)) / prev_unique_viewers) * 100 <= -1 THEN -1
					ELSE 0
				END) as indicator
			FROM last_16_cte_pt2
		), trend_cte as (
			SELECT
				channel_title,
				year,
				week,
				unique_viewers,
				prev_unique_viewers,
				indicator + lead(indicator, 1) over (partition by channel_title order by year desc, week desc) as trend,
				row_number() over (partition by channel_title order by year desc, week desc) as row_num
			FROM indicator_cte
		), latest_trend_cte as (
			SELECT
				channel_title,
				trend
			FROM trend_cte
			WHERE row_num = 1
		)
		SELECT
			s.channel_title,
			s.max_unique_viewers,
			s.min_unique_viewers,
			s.avg_unique_viewers,
			lw.latest_unique_viewers,
			lt.trend,
			shc.second_highest_uvs,
			slc.second_lowest_uvs
		FROM stats_cte s
		INNER JOIN latest_week_cte lw on lw.channel_title = s.channel_title
		INNER JOIN latest_trend_cte lt on lt.channel_title = s.channel_title
		INNER JOIN second_highest_cte shc on shc.channel_title = s.channel_title
		INNER JOIN second_lowest_cte slc on slc.channel_title = s.channel_title;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			channel = result[0].strip()
			self.uv_overview_df[channel] = {
				'max_unique_viewers': result[1],
				'min_unique_viewers': result[2],
				'avg_unique_viewers': result[3],
				'latest_unique_viewers': result[4],
				'uv_trend': result[5],
				'unique_viewers_delta': result[4] - result[3],
				'unique_viewers_pct_change': int(round(((result[4] - result[3]) / result[3]) * 100, 0)),
				'second_highest_uvs': result[6],
				'second_lowest_uvs': result[7]
			}


	def uv_deep_dive(self):
		self.loggerv3.info('UV Deep Dive')
		query = f"""
		WITH last_16_cte_pt1 as (
			SELECT
				dse.channel_title,
				(CASE
					WHEN user_tier in ('premium', 'trial') THEN 'SVOD'
					ELSE 'AVOD'
				END) as avod_svod,
				(CASE
            		WHEN date_part('dayofweek', cast(vv.start_timestamp as date)) = 0 AND date_part('week', cast(vv.start_timestamp as date)) = 52 THEN date_part('year', vv.start_timestamp) + 1
            		ELSE date_part('year', vv.start_timestamp)
        		END) as year,
				(CASE
					WHEN date_part('dayofweek', cast(vv.start_timestamp as date)) = 0 AND date_part('week', cast(vv.start_timestamp as date)) = 52 THEN 1
					WHEN date_part('dayofweek', cast(vv.start_timestamp as date)) = 0 THEN date_part('week', cast(vv.start_timestamp as date)) + 1
					ELSE date_part('week', cast(vv.start_timestamp as date))
				END) as week,
				count(distinct cast(vv.start_timestamp as varchar(10))) as days_in_week,
				count(distinct coalesce(cast(vv.user_key as varchar), vv.anonymous_id)) as unique_viewers
			FROM warehouse.vod_viewership vv
			INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
			WHERE channel_title NOT IN ('Squad Team Force', 'Kinda Funny', 'Friends of RT')
				AND start_timestamp > current_date - interval '{self.WEEK_AVERAGE} week'
			GROUP BY 1, 2, 3, 4
		), last_16_cte_pt2 as (
			SELECT *,
				   row_number() over (partition by channel_title, avod_svod order by year desc, week desc) as row_num
			FROM last_16_cte_pt1
			WHERE days_in_week = 7
		), latest_week_cte as (
			SELECT
				channel_title,
				avod_svod,
				unique_viewers as latest_unique_viewers
			FROM last_16_cte_pt2
			WHERE row_num = 1
		), stats_cte as (
			SELECT
				channel_title,
				avod_svod,
				avg(unique_viewers) as avg_unique_viewers
			FROM last_16_cte_pt2
			GROUP BY 1, 2
		), joined_cte as (
			SELECT
				s.channel_title,
				s.avod_svod,
				lw.latest_unique_viewers,
				s.avg_unique_viewers
			FROM stats_cte s
			INNER JOIN latest_week_cte lw on lw.channel_title = s.channel_title and lw.avod_svod = s.avod_svod
		), avod_cte as (
			SELECT channel_title,
				   latest_unique_viewers as lw_avod_uvs,
				   avg_unique_viewers as avg_avod_uvs
			FROM joined_cte
			WHERE avod_svod = 'AVOD'
		), svod_cte as (
			SELECT channel_title,
				   latest_unique_viewers as lw_svod_uvs,
				   avg_unique_viewers as avg_svod_uvs
			FROM joined_cte
			WHERE avod_svod = 'SVOD'
		)
		SELECT s.channel_title,
			   s.lw_svod_uvs,
			   s.avg_svod_uvs,
			   a.lw_avod_uvs,
			   a.avg_avod_uvs
		FROM svod_cte s
		INNER JOIN avod_cte a on a.channel_title = s.channel_title;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			channel = result[0].strip()
			self.uv_deep_dive_df[channel] = {
				'lw_svod_uvs': result[1],
				'avg_svod_uvs': result[2],
				'lw_avod_uvs': result[3],
				'avg_avod_uvs': result[4],
				'svod_delta': result[1] - result[2],
				'avod_delta': result[3] - result[4]
			}


	def vod_weekly_content(self):
		self.loggerv3.info('VOD Weekly Content')
		query = f"""
		WITH last_16_cte as (
			SELECT
				channel_title,
				series_title,
				week_start,
				episode_title,
				viewers,
				avg_pct_consumed,
				pct_new_viewers
			FROM warehouse.agg_weekly_site_apps_viewership
			WHERE channel_title NOT IN ('Squad Team Force', 'Kinda Funny', 'Friends of RT')
				AND week_start > current_date - interval '{self.WEEK_AVERAGE} week'
		), latest_week_cte as (
			SELECT
				channel_title,
				series_title,
				week_start,
				episode_title,
				viewers,
				avg_pct_consumed,
				pct_new_viewers
			FROM last_16_cte
			WHERE date_diff('days', cast(week_start as date), current_date) <= {self.DAYS_FOR_VOD_CONTENT}
		), avgs_cte as (
			SELECT
				channel_title,
				series_title,
				avg(viewers) as avg_unique_viewers,
				avg(avg_pct_consumed) as avg_completion_rate,
				avg(pct_new_viewers) as avg_new_viewer_pct
			FROM last_16_cte
			GROUP BY 1, 2
		)
		SELECT
			lw.channel_title,
			lw.series_title,
			lw.episode_title,
			lw.viewers,
			lw.avg_pct_consumed,
			lw.pct_new_viewers,
			a.avg_unique_viewers,
			a.avg_completion_rate as avg_avg_completion_rate,
			a.avg_new_viewer_pct
		FROM latest_week_cte lw
		INNER JOIN avgs_cte a on a.channel_title = lw.channel_title and a.series_title = lw.series_title
		ORDER BY channel_title, viewers desc;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			channel = result[0].strip()
			series = result[1]
			data = {
				'video_title': result[2],
				'unique_viewers': result[3],
				'avg_completion_rate': int(result[4]*100),
				'new_viewer_pct': round(result[5]*100, 0),
				'avg_unique_viewers': result[6],
				'avg_avg_completion_rate': int(result[7]*100),
				'avg_new_viewer_pct': round(result[8]*100, 0),
				'uv_delta': 100 * ((result[3] - result[6]) / result[6]),
				'comp_rate_delta': round(100 * abs(result[4] - result[7]))
			}
			if channel not in self.vod_weekly_content_df:
				self.vod_weekly_content_df[channel] = {series: [data]}
			else:
				if series not in self.vod_weekly_content_df[channel]:
					self.vod_weekly_content_df[channel][series] = [data]
				else:
					self.vod_weekly_content_df[channel][series].append(data)


	def build_high_level_narrative(self, channel):
		self.loggerv3.info(f'Building narrative for {channel}')
		narrative = f"~SITE~\n\n"

		# Trajectories
		trajectory = self.trajectories_df[channel]
		narrative += f"{channel} viewership from the last month was "
		if trajectory < 0:
			narrative += f"{trajectory}% below average. "
		elif trajectory > 0:
			narrative += f"+{trajectory}% above average. "
		else:
			narrative += "steady compared with average. "

		# UV Overview
		uv_overview = self.uv_overview_df[channel]
		narrative += "Last week's UVs "
		if uv_overview['latest_unique_viewers'] == uv_overview['max_unique_viewers']:
			narrative += f"""were the highest they've been in the last 16 weeks with {nf(uv_overview['latest_unique_viewers'], 1)} UVs, """
			narrative += f"""+{uv_overview['unique_viewers_pct_change']}% above average"""

		elif uv_overview['latest_unique_viewers'] == uv_overview['min_unique_viewers']:
			narrative += f"""were the lowest they've been in the last 16 weeks with {nf(uv_overview['latest_unique_viewers'], 1)} UVs, """
			narrative += f"""{uv_overview['unique_viewers_pct_change']}% below average"""

		elif uv_overview['latest_unique_viewers'] == uv_overview['second_highest_uvs']:
			narrative += f"""were the 2nd highest they've been in the last 16 weeks with {nf(uv_overview['latest_unique_viewers'], 1)} UVs, """
			narrative += f"""+{uv_overview['unique_viewers_pct_change']}% above average"""

		elif uv_overview['latest_unique_viewers'] == uv_overview['second_lowest_uvs']:
			narrative += f"""were the 2nd lowest they've been in the last 16 weeks with {nf(uv_overview['latest_unique_viewers'], 1)} UVs, """
			narrative += f"""{uv_overview['unique_viewers_pct_change']}% below average"""

		elif uv_overview['latest_unique_viewers'] > uv_overview['avg_unique_viewers']:
			narrative += f"""were +{uv_overview['unique_viewers_pct_change']}% above average with """
			narrative += f"""{nf(uv_overview['latest_unique_viewers'], 1)} UVs"""

		elif uv_overview['latest_unique_viewers'] < uv_overview['avg_unique_viewers']:
			narrative += f"""were {uv_overview['unique_viewers_pct_change']}% below average with """
			narrative += f"""{nf(uv_overview['latest_unique_viewers'], 1)} UVs"""

		# UV Trend
		if uv_overview['uv_trend'] >= self.TREND_THRESHOLD:
			if uv_overview['latest_unique_viewers'] > uv_overview['avg_unique_viewers']:
				narrative += " and "
			else:
				narrative += " despite being "
			narrative += f"""on a {uv_overview['uv_trend']}-week upward trend. """
		elif uv_overview['uv_trend'] <= -1 * self.TREND_THRESHOLD:
			if uv_overview['latest_unique_viewers'] > uv_overview['avg_unique_viewers']:
				narrative += " despite being "
			else:
				narrative += " and "
			narrative += f"""on a {abs(uv_overview['uv_trend'])}-week downward trend. """
		else:
			narrative += ". "

		# UV Deep Dive
		if channel in self.uv_deep_dive_df:
			deep_dive = self.uv_deep_dive_df[channel]
			if uv_overview['latest_unique_viewers'] > uv_overview['avg_unique_viewers']:
				narrative += "The above average UVs are primarily driven by "
				if deep_dive['svod_delta'] > deep_dive['avod_delta']:
					narrative += f"SVOD. "
				else:
					narrative += f"AVOD. "
			else:
				narrative += "The below average UVs are primarily driven by "
				if deep_dive['svod_delta'] < deep_dive['avod_delta']:
					narrative += f"SVOD. "
				else:
					narrative += f"AVOD. "

		# Write Initial Narratives to File
		channel_file_name = channel.lower().strip()
		with open(f'downloads/narratives/site/{channel_file_name}.txt', 'a+') as f:
			f.write(f'{narrative}')


	def build_vod_narrative(self):
		self.loggerv3.info('Building VOD narratives')
		# VOD Weekly Content
		for channel_key, series_dict in self.vod_weekly_content_df.items():
			for series, series_vods in series_dict.items():
				vods_len = len(series_vods)
				series_narrative = "\n\n"
				top_vod = series_vods[0]

				if series not in self.STAND_ALONE_SERIES and series != channel_key:
					series_narrative += f"""For {series}, """

				# Top VOD
				if vods_len > 2:
					series_narrative += f"\"{top_vod['video_title'][:self.CHAR_LIMIT]}...\" had the highest UVs with {nf(top_vod['unique_viewers'], 1)}, "
				else:
					series_narrative += f"\"{top_vod['video_title'][:self.CHAR_LIMIT]}...\" got {nf(top_vod['unique_viewers'], 1)} UVs, """
				if top_vod['uv_delta'] >= self.UV_DELTA_THRESHOLD:
					series_narrative += "above "
				elif top_vod['uv_delta'] <= -1 * self.UV_DELTA_THRESHOLD:
					if vods_len > 2:
						series_narrative += "despite being "
					series_narrative += "below "
				else:
					series_narrative += "at/around "
				series_narrative += f"the {nf(top_vod['avg_unique_viewers'], 1)} average. "

				# Completion Rate
				if top_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD and \
						top_vod['avg_completion_rate'] > top_vod['avg_avg_completion_rate'] and \
						top_vod['uv_delta'] <= -1 * self.UV_DELTA_THRESHOLD:
					series_narrative += "However, the "
				else:
					series_narrative += "The "
				series_narrative += "completion rate was "
				if top_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD:
					if top_vod['avg_completion_rate'] > top_vod['avg_avg_completion_rate']:
						series_narrative += "above "
					elif top_vod['avg_completion_rate'] < top_vod['avg_avg_completion_rate']:
						series_narrative += "below "
				else:
					series_narrative += "around "
				series_narrative += f"the {top_vod['avg_avg_completion_rate']}% average. "

				# New Viewer %
				if top_vod['new_viewer_pct'] - top_vod['avg_new_viewer_pct'] >= self.NEW_VIEWER_PCT_THRESHOLD:
					series_narrative += f"It also brought in higher than average new viewers. "

				# Bottom VOD
				if vods_len > 1:
					bottom_vod = series_vods[-1]
					if vods_len > 2:
						series_narrative += f"\"{bottom_vod['video_title'][:self.CHAR_LIMIT]}...\" had the lowest UVs with {nf(bottom_vod['unique_viewers'], 1)}, "
					else:
						series_narrative += f"\"{bottom_vod['video_title'][:self.CHAR_LIMIT]}...\" got {nf(bottom_vod['unique_viewers'], 1)} UVs, "
					if bottom_vod['uv_delta'] >= self.UV_DELTA_THRESHOLD:
						if vods_len > 2:
							series_narrative += "but was still "
						series_narrative += "above "
					elif bottom_vod['uv_delta'] <= -1 * self.UV_DELTA_THRESHOLD:
						series_narrative += "below "
					else:
						series_narrative += "at "
					series_narrative += f"the average. "

					# Completion Rate
					if bottom_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD and \
							bottom_vod['avg_completion_rate'] > bottom_vod['avg_avg_completion_rate'] and \
							bottom_vod['uv_delta'] <= -1 * self.UV_DELTA_THRESHOLD:
						series_narrative += "However, the "
					else:
						series_narrative += "The "
					series_narrative += "completion rate was "
					if bottom_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD:
						if bottom_vod['avg_completion_rate'] > bottom_vod['avg_avg_completion_rate']:
							series_narrative += "above "
						elif bottom_vod['avg_completion_rate'] < bottom_vod['avg_avg_completion_rate']:
							series_narrative += "below "
					else:
						series_narrative += "around "
					series_narrative += f"the average. "


				# Write Series Narrative to File
				if series in self.STAND_ALONE_SERIES:
					narrative = '\n\n'
					narrative += '-' * 125
					narrative += '\n\n'
					series_file_name = series.lower().strip()
					with open(f'downloads/narratives/site/{series_file_name}.txt', 'a+') as f:
						f.write(f'{series_narrative}')
				else:
					channel_file_name = channel_key.lower().strip()
					with open(f'downloads/narratives/site/{channel_file_name}.txt', 'a+') as f:
						f.write(f'{series_narrative}')


	def write_end_of_file(self, channel):
		# Write End of Section to File
		narrative = '\n\n'
		narrative += '-' * 125
		narrative += '\n\n'
		channel_file_name = channel.lower().strip()
		with open(f'downloads/narratives/site/{channel_file_name}.txt', 'a+') as f:
			f.write(f'{narrative}')



	def execute(self):
		self.loggerv3.info(f"Running Site Weekly Narratives Job")
		self.trajectories()
		self.uv_overview()
		self.uv_deep_dive()
		self.vod_weekly_content()
		for channel in self.trajectories_df.keys():
			self.build_high_level_narrative(channel)
		self.build_vod_narrative()
		for channel in self.trajectories_df.keys():
			self.write_end_of_file(channel)
		self.loggerv3.success("All Processing Complete!")
