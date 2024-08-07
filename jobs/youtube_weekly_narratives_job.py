import utils.components.yt_variables as sv
from utils.components.number_formatter import num_format as nf
from base.etl_jobv3 import EtlJobV3
from utils.connectors.database_connector import DatabaseConnector  # TODO: REMOVE AFTER TESTING


class YouTubeWeeklyNarrativesJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = '', local_mode=True)

		self.channels = sv.recap_channels
		self.trajectories_df = {}
		self.uv_overview_df = {}
		self.uv_delta_mappings = []
		self.uv_deltas_df = {}
		self.vod_weekly_content_df = {}
		self.shorts_weekly_content_df = {}
		self.shorts_new_viewers_df = {}
		# Thresholds
		self.WEEK_AVERAGE = 16
		self.DAYS_FOR_VOD_CONTENT = 16  # This is the max number of days that is available between the start week of the latest scrape and when we run this job
		self.TREND_THRESHOLD = 2
		self.UV_PCT_CHANGE = 5
		self.UV_DELTA_THRESHOLD = 5  # Only call out the UV delta between latest week and average if at least this %
		self.COLLECTIVE_MAJORITY_THRESHOLD = .60  # We want to explain at least this % of the total delta between latest week UVs and average UVs
		self.COMPLETION_RATE_DELTA_THRESHOLD = 2  # Only call out the completion rate delta between latest week and average if at least this %
		self.CHAR_LIMIT = 30  # The number of characters to show for a VOD title


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
			FROM warehouse.youtube_channel_trajectory
			WHERE channel_title in {self.channels}
		)
		SELECT
			channel_title,
			trajectory
		FROM cte
		WHERE row_num = 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.trajectories_df[result[0]] = int(result[1]*100)



	def uv_overview(self):
		self.loggerv3.info('UV Overview')
		query = f"""
		WITH last_16_cte as (
			SELECT
				channel_title,
				week_start,
				unique_viewers,
				lead(unique_viewers, 1) over (partition by channel_title order by week_start desc) as prev_unique_viewers,
				row_number() over (partition by channel_title order by week_start desc) as row_num
			FROM warehouse.yt_studio_weekly_channel_metrics
			WHERE channel_title in {self.channels}
				AND week_start > current_date - interval '{self.WEEK_AVERAGE} week'
		), latest_week_cte as (
			SELECT
				channel_title,
				unique_viewers as latest_unique_viewers
			FROM last_16_cte
			WHERE row_num = 1
		), stats_cte as (
			SELECT
				channel_title,
				max(unique_viewers) as max_unique_viewers,
				avg(unique_viewers) as avg_unique_viewers,
				min(unique_viewers) as min_unique_viewers
			FROM last_16_cte
			GROUP BY 1
		), second_highest_cte as (
			SELECT
				l.channel_title,
				max(l.unique_viewers) as second_highest_uvs
			FROM last_16_cte l
			INNER JOIN stats_cte s on s.channel_title = l.channel_title and s.max_unique_viewers != l.unique_viewers
			GROUP BY 1
		), second_lowest_cte as (
			SELECT
				l.channel_title,
				min(l.unique_viewers) as second_lowest_uvs
			FROM last_16_cte l
			INNER JOIN stats_cte s on s.channel_title = l.channel_title and s.min_unique_viewers != l.unique_viewers
			GROUP BY 1
		), indicator_cte as (
			SELECT
				channel_title,
				week_start,
				unique_viewers,
				prev_unique_viewers,
				(CASE
					WHEN ((1.0 * (unique_viewers - prev_unique_viewers)) / prev_unique_viewers) * 100 >= 1 THEN 1
					WHEN ((1.0 * (unique_viewers - prev_unique_viewers)) / prev_unique_viewers) * 100 <= -1 THEN -1
					ELSE 0
				END) as indicator
			FROM last_16_cte
		), trend_cte as (
			SELECT
				channel_title,
				week_start,
				unique_viewers,
				prev_unique_viewers,
				indicator + lead(indicator, 1) over (partition by channel_title order by week_start desc) as trend,
				row_number() over (partition by channel_title order by week_start desc) as row_num
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
			self.uv_overview_df[result[0]] = {
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


	def uv_deltas(self):
		self.loggerv3.info('UV Deep Dive')
		query = f"""
		WITH last_16_cte as (
			SELECT
				channel_title,
				week_start,
				returning_vods_uvs*1.0 as returning_vods_uvs,
				new_vods_uvs*1.0 as new_vods_uvs,
				returning_shorts_uvs*1.0 as returning_shorts_uvs,
				new_shorts_uvs*1.0 as new_shorts_uvs,
				returning_live_uvs*1.0 as returning_live_uvs,
				new_live_uvs*1.0 as new_live_uvs,
				row_number() over (partition by channel_title order by week_start desc) as row_num
			FROM warehouse.yt_studio_weekly_channel_metrics
			WHERE channel_title in {self.channels}
				AND week_start > current_date - interval '{self.WEEK_AVERAGE} week'
		), latest_week_cte as (
			SELECT
				channel_title,
				returning_vods_uvs,
				new_vods_uvs,
				returning_shorts_uvs,
				new_shorts_uvs,
				returning_live_uvs,
				new_live_uvs
			FROM last_16_cte
			WHERE row_num = 1
		), avgs_cte as (
			SELECT
				channel_title,
				avg(returning_vods_uvs) as avg_returning_vods_uvs,
				avg(new_vods_uvs) as avg_new_vods_uvs,
				avg(returning_shorts_uvs) as avg_returning_shorts_uvs,
				avg(new_shorts_uvs) as avg_new_shorts_uvs,
				avg(returning_live_uvs) as avg_returning_live_uvs,
				avg(new_live_uvs) as avg_new_live_uvs
			FROM last_16_cte
			GROUP BY 1
		)
		SELECT
			lw.channel_title,
			returning_vods_uvs - avg_returning_vods_uvs as returning_vod_delta,
			new_vods_uvs - avg_new_vods_uvs as new_vod_delta,
			returning_shorts_uvs - avg_returning_shorts_uvs as returning_shorts_delta,
			new_shorts_uvs - avg_new_shorts_uvs as new_shorts_delta,
			returning_live_uvs - avg_returning_live_uvs as returning_live_delta,
			new_live_uvs - avg_new_live_uvs as new_live_delta
		FROM latest_week_cte lw
		LEFT JOIN avgs_cte a on a.channel_title = lw.channel_title;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			channel = result[0]
			self.shorts_new_viewers_df[channel] = result[4]

			# UV Deltas
			delta = self.uv_overview_df[channel]['unique_viewers_delta']

			self.uv_deltas_df[channel] = {}

			# returning_vod_delta
			if result[1] is not None and ((result[1] > 0 and delta > 0) or (result[1] < 0 and delta < 0)):
				self.uv_deltas_df[channel]['returning VOD viewers'] = int(result[1])

			# new_vod_delta
			if result[2] is not None and ((result[2] > 0 and delta > 0) or (result[2] < 0 and delta < 0)):
				self.uv_deltas_df[channel]['new VOD viewers'] = int(result[2])

			# returning_shorts_delta
			if result[3] is not None and ((result[3] > 0 and delta > 0) or (result[3] < 0 and delta < 0)):
				self.uv_deltas_df[channel]['returning Shorts viewers'] = int(result[3])

			# new_shorts_delta
			if result[4] is not None and ((result[4] > 0 and delta > 0) or (result[4] < 0 and delta < 0)):
				self.uv_deltas_df[channel]['new Shorts viewers'] = int(result[4])

			# returning_live_delta
			if result[5] is not None and ((result[5] > 0 and delta > 0) or (result[5] < 0 and delta < 0)):
				self.uv_deltas_df[channel]['returning Live viewers'] = int(result[5])

			# new_live_delta
			if result[6] is not None and ((result[6] > 0 and delta > 0) or (result[6] < 0 and delta < 0)):
				self.uv_deltas_df[channel]['new Live viewers'] = int(result[6])


	def vod_weekly_content(self):
		self.loggerv3.info('VOD Weekly Content')
		query = f"""
		WITH last_16_cte as (
			SELECT
				channel_title,
				start_week,
				video_title,
				unique_viewers,
				avg_completion_rate,
				unique_viewers- returning_viewers as new_viewers
			FROM warehouse.yt_studio_weekly_metrics
			WHERE channel_title in {self.channels}
				AND start_week > current_date - interval '{self.WEEK_AVERAGE} week'
				AND duration_in_seconds > 60
				AND duration_in_seconds < 7200
				AND visibility_tag = 'Public'
		), latest_week_cte as (
			SELECT
				channel_title,
				start_week,
				video_title,
				unique_viewers,
				avg_completion_rate,
				new_viewers
			FROM last_16_cte
			WHERE date_diff('days', cast(start_week as date), current_date) <= {self.DAYS_FOR_VOD_CONTENT}
		), avgs_cte as (
			SELECT
				channel_title,
				avg(unique_viewers) as avg_unique_viewers,
				avg(avg_completion_rate) as avg_completion_rate,
				avg(new_viewers) as avg_new_viewers
			FROM last_16_cte
			GROUP BY 1
		)
		SELECT
			lw.channel_title,
			lw.video_title,
			lw.unique_viewers,
			lw.avg_completion_rate,
			lw.new_viewers,
			a.avg_unique_viewers,
			a.avg_completion_rate as avg_avg_completion_rate,
			a.avg_new_viewers
		FROM latest_week_cte lw
		INNER JOIN avgs_cte a on a.channel_title = lw.channel_title
		ORDER BY channel_title, unique_viewers desc;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			data = {
				'video_title': result[1],
				'unique_viewers': result[2],
				'avg_completion_rate': int(result[3]*100),
				'new_viewers': result[4],
				'avg_unique_viewers': result[5],
				'avg_avg_completion_rate': int(result[6]*100),
				'avg_new_viewers': int(result[7]),
				'uv_delta': 100 * ((result[2] - result[5]) / result[5]),
				'comp_rate_delta': int((100 * abs(result[3] - result[6])))
			}
			if result[0] not in self.vod_weekly_content_df:
				self.vod_weekly_content_df[result[0]] = [data]
			else:
				self.vod_weekly_content_df[result[0]].append(data)


	def shorts_weekly_content(self):
		self.loggerv3.info('Shorts Weekly Content')
		query = f"""
		WITH last_16_cte as (
			SELECT
				channel_title,
				start_week,
				video_title,
				unique_viewers,
				avg_completion_rate,
				unique_viewers - returning_viewers as new_viewers
			FROM warehouse.yt_studio_weekly_metrics
			WHERE channel_title in {self.channels}
				AND start_week > current_date - interval '{self.WEEK_AVERAGE} week'
				AND duration_in_seconds <= 60
				AND visibility_tag = 'Public'
		), latest_week_cte as (
			SELECT
				channel_title,
				start_week,
				video_title,
				unique_viewers,
				avg_completion_rate,
				new_viewers
			FROM last_16_cte
			WHERE date_diff('days', cast(start_week as date), current_date) <= {self.DAYS_FOR_VOD_CONTENT}
		), avgs_cte as (
			SELECT
				channel_title,
				avg(unique_viewers) as avg_unique_viewers,
				avg(avg_completion_rate) as avg_completion_rate,
				avg(new_viewers) as avg_new_viewers
			FROM last_16_cte
			GROUP BY 1
		), cte as (
			SELECT
				lw.channel_title,
				lw.video_title,
				lw.unique_viewers,
				lw.avg_completion_rate,
				lw.new_viewers,
				a.avg_unique_viewers,
				a.avg_completion_rate as avg_avg_completion_rate,
				a.avg_new_viewers,
				(CASE
					WHEN lw.unique_viewers > a.avg_unique_viewers THEN 1
					ELSE 0
				END) as above_avg_uvs
			FROM latest_week_cte lw
			INNER JOIN avgs_cte a on a.channel_title = lw.channel_title
			ORDER BY channel_title, unique_viewers desc
		)
		SELECT channel_title,
			   sum(above_avg_uvs) as above_avg_uvs,
			   count(*) as shorts_num,
			   min(avg_unique_viewers) as avg_unique_viewers,
			   avg(new_viewers) as lw_avg_new_viewers,
			   min(avg_new_viewers) as avg_new_viewers
		FROM cte
		GROUP BY 1;
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			data = {
				'above_avg_uvs': int(result[1]),
				'shorts_num': int(result[2]),
				'avg_unique_viewers': int(result[3]),
				'lw_avg_new_viewer_pct': round(result[4]*100, 0),
				'avg_new_viewer_pct': round(result[5]*100, 0),
				'new_viewer_pct_delta': round(100 * abs(result[4] - result[5]), 0)
			}
			self.shorts_weekly_content_df[result[0]] = data


	def build_narrative(self, channel):
		self.loggerv3.info(f'Building narrative for {channel}')
		narrative = f"~YOUTUBE~\n\n"

		# Trajectories
		trajectory = self.trajectories_df[channel]
		narrative += f"Viewership in the last 30 days was "
		if trajectory < 0:
			narrative += f"{trajectory}% below average. "
		elif trajectory > 0:
			narrative += f"+{trajectory}% above average. "
		else:
			narrative += "in line with averages. "

		# UV Overview
		uv_overview = self.uv_overview_df[channel]
		narrative += "Last week's UVs "
		if uv_overview['latest_unique_viewers'] == uv_overview['max_unique_viewers']:
			narrative += f"""were the highest they've been in the last 16 weeks with {nf(uv_overview['latest_unique_viewers'], 0)} UVs, """
			narrative += f"""+{uv_overview['unique_viewers_pct_change']}% above average"""

		elif uv_overview['latest_unique_viewers'] == uv_overview['min_unique_viewers']:
			narrative += f"""were the lowest they've been in the last 16 weeks with {nf(uv_overview['latest_unique_viewers'], 0)} UVs, """
			narrative += f"""{uv_overview['unique_viewers_pct_change']}% below average"""

		elif uv_overview['latest_unique_viewers'] == uv_overview['second_highest_uvs']:
			narrative += f"""were the 2nd highest they've been in the last 16 weeks with {nf(uv_overview['latest_unique_viewers'], 0)} UVs, """
			narrative += f"""+{uv_overview['unique_viewers_pct_change']}% above average"""

		elif uv_overview['latest_unique_viewers'] == uv_overview['second_lowest_uvs']:
			narrative += f"""were the 2nd lowest they've been in the last 16 weeks with {nf(uv_overview['latest_unique_viewers'], 0)} UVs, """
			narrative += f"""{uv_overview['unique_viewers_pct_change']}% below average"""

		elif uv_overview['latest_unique_viewers'] > uv_overview['avg_unique_viewers']:
			narrative += f"""were +{uv_overview['unique_viewers_pct_change']}% above average with """
			narrative += f"""{nf(uv_overview['latest_unique_viewers'], 0)} UVs"""

		elif uv_overview['latest_unique_viewers'] < uv_overview['avg_unique_viewers']:
			narrative += f"""were {uv_overview['unique_viewers_pct_change']}% below average with """
			narrative += f"""{nf(uv_overview['latest_unique_viewers'], 0)} UVs"""

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

		# UV Drivers
		drivers = self.uv_deltas_df[channel]
		delta = uv_overview['unique_viewers_delta']
		cumm = 0
		counter = 1
		l = len(drivers)

		if uv_overview['unique_viewers_pct_change'] >= self.UV_PCT_CHANGE:
			drivers = dict(sorted(drivers.items(), key=lambda item: item[1], reverse=True))
			narrative += f"""The above average UVs are driven by """

		elif uv_overview['unique_viewers_pct_change'] <= -1 * self.UV_PCT_CHANGE:
			drivers = dict(sorted(drivers.items(), key=lambda item: item[1]))
			narrative += f"""The below average UVs are driven by """

		if abs(uv_overview['unique_viewers_pct_change']) >= self.UV_PCT_CHANGE:
			for key, val in drivers.items():
				cumm += val
				if counter == 1:
					if val > 0:
						narrative += f"""+{nf(val,0)} {key}"""
					else:
						narrative += f"""{nf(val, 0)} {key}"""
				elif cumm / delta >= self.COLLECTIVE_MAJORITY_THRESHOLD or val / delta <= .05 or counter == l:
					narrative += ". "
					break
				else:
					if val > 0:
						narrative += f""", +{nf(val,0)} {key}"""
					else:
						narrative += f""", {nf(val, 0)} {key}"""

				counter += 1

		# VOD Weekly Content
		if channel in self.vod_weekly_content_df:
			vods = self.vod_weekly_content_df[channel]
			vods_len = len(vods)
			top_vod = vods[0]
			narrative += "\n\n"
			# Top VOD
			if vods_len > 2:
				narrative += f"\"{top_vod['video_title'][:self.CHAR_LIMIT]}...\" had the highest UVs with {nf(top_vod['unique_viewers'], 1)}, "
			else:
				narrative += f"\"{top_vod['video_title'][:self.CHAR_LIMIT]}...\" got {nf(top_vod['unique_viewers'], 1)} UVs, """
			if top_vod['uv_delta'] >= self.UV_DELTA_THRESHOLD:
				narrative += "above "
			elif top_vod['uv_delta'] <= -1 * self.UV_DELTA_THRESHOLD:
				if vods_len > 2:
					narrative += "despite being "
				narrative += "below "
			else:
				narrative += "at/around "
			narrative += f"the {nf(top_vod['avg_unique_viewers'], 1)} average. "

			# Completion Rate
			if top_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD and \
					top_vod['avg_completion_rate'] > top_vod['avg_avg_completion_rate'] and \
					top_vod['uv_delta'] <= -1 * self.UV_DELTA_THRESHOLD:
				narrative += "However, the "
			else:
				narrative += "The "
			narrative += "completion rate was "
			if top_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD:
				if top_vod['avg_completion_rate'] > top_vod['avg_avg_completion_rate']:
					narrative += "above "
				elif top_vod['avg_completion_rate'] < top_vod['avg_avg_completion_rate']:
					narrative += "below "
			else:
				narrative += "around "
			narrative += f"the {top_vod['avg_avg_completion_rate']}% average"

			# New Viewers
			if top_vod['new_viewers'] > top_vod['avg_new_viewers']:
				if top_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD and top_vod['avg_completion_rate'] < top_vod['avg_avg_completion_rate']:
					narrative += ", but "
				else:
					narrative += ", and "
				narrative += f"it brought in above average new viewers of {nf(top_vod['new_viewers'], 1)}"
			narrative += ". "


			# Bottom VOD
			if vods_len > 1:
				narrative += "\n\n"
				bottom_vod = vods[-1]
				if vods_len > 2:
					narrative += f"\"{bottom_vod['video_title'][:self.CHAR_LIMIT]}...\" had the lowest UVs with {nf(bottom_vod['unique_viewers'], 1)}, "
				else:
					narrative += f"\"{bottom_vod['video_title'][:self.CHAR_LIMIT]}...\" got {nf(bottom_vod['unique_viewers'], 1)} UVs, "
				if bottom_vod['uv_delta'] >= self.UV_DELTA_THRESHOLD:
					if vods_len > 2:
						narrative += "but was still "
					narrative += "above "
				elif bottom_vod['uv_delta'] <= -1 * self.UV_DELTA_THRESHOLD:
					narrative += "below "
				else:
					narrative += "at "
				narrative += f"the average. "

				# Completion Rate
				if bottom_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD and \
						bottom_vod['avg_completion_rate'] > bottom_vod['avg_avg_completion_rate'] and \
						bottom_vod['uv_delta'] <= -1 * self.UV_DELTA_THRESHOLD:
					narrative += "However, the "
				else:
					narrative += "The "
				narrative += "completion rate was "
				if bottom_vod['comp_rate_delta'] > self.COMPLETION_RATE_DELTA_THRESHOLD:
					if bottom_vod['avg_completion_rate'] > bottom_vod['avg_avg_completion_rate']:
						narrative += "above "
					elif bottom_vod['avg_completion_rate'] < bottom_vod['avg_avg_completion_rate']:
						narrative += "below "
				else:
					narrative += "around "
				narrative += f"the average."


			# Shorts Weekly Content
			if channel in self.shorts_weekly_content_df:
				shorts_data = self.shorts_weekly_content_df[channel]
				narrative += "\n\n"
				if shorts_data['shorts_num'] == 1:
					narrative += "The 1 Short that was posted last week was "
					if shorts_data['above_avg_uvs'] == shorts_data['shorts_num']:
						narrative += "above "
					else:
						narrative += "below "
				elif shorts_data['shorts_num'] == 2:
					if shorts_data['above_avg_uvs'] == shorts_data['shorts_num']:
						narrative += "Both of the Shorts posted last week were above "
					elif shorts_data['above_avg_uvs'] == 0:
						narrative += "Neither of the Shorts posted last week were above "
					else:
						narrative += "1 of the 2 Shorts posted last week was above "
				elif shorts_data['shorts_num'] >= 3:
					if shorts_data['above_avg_uvs'] == shorts_data['shorts_num']:
						narrative += f"All {shorts_data['shorts_num']} of the Shorts posted last week were above "
					elif shorts_data['above_avg_uvs'] == 0:
						narrative += f"None of the {shorts_data['shorts_num']} Shorts posted last week were above "
					else:
						narrative += f"{shorts_data['above_avg_uvs']} of the {shorts_data['shorts_num']} Shorts posted last week were above "

				narrative += f"the UV average of {nf(shorts_data['avg_unique_viewers'])}. "

				new_shorts_delta = self.shorts_new_viewers_df[channel]
				if new_shorts_delta > 0:
					narrative += f"Shorts overall brought in higher than average New Viewers."


		# Write Narrative to File
		channel_file_name = channel.lower().strip()
		narrative += '\n\n'
		narrative += '-'*125
		narrative += '\n\n'
		with open(f'downloads/narratives/youtube/{channel_file_name}.txt', 'a+') as f:
			f.write(f'{narrative}')


	def execute(self):
		self.loggerv3.info(f"Running YouTube Weekly Narratives Job")
		self.trajectories()
		self.uv_overview()
		self.uv_deltas()
		self.vod_weekly_content()
		self.shorts_weekly_content()
		for channel in self.channels:
			self.build_narrative(channel)
		self.loggerv3.success("All Processing Complete!")
