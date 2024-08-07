import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from utils.components.dater import Dater as DateHandler


class VoteShareCombinedPremiumWindowedJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'vote_share_combined_premium_windowed')

		self.Dater = DateHandler()
		self.period_starting = target_date
		self.period_ending = self.Dater.format_date(self.Dater.get_end_of_month(self.period_starting.replace('-','')))
		self.query_end_cap = self.Dater.format_date(self.Dater.find_next_day(self.period_ending.replace('-','')))
		self.final_dataframe = None
		self.look_back_window = 366


	def load_data(self):
		self.loggerv3.info(f"Loading View data...")
		query = f""" 
			WITH viewers as (
				SELECT
				    fsv.user_key,
				    de.channel_title,
				    de.series_title,
				    de.season_title,
				    sum(fsv.active_seconds) as seconds_viewed,
				    count(*) as user_views
				FROM warehouse.vod_viewership fsv
				LEFT JOIN warehouse.dim_segment_episode de
				ON fsv.episode_key = de.episode_key
				WHERE
				    fsv.start_timestamp >= '{self.period_starting}' AND
				    fsv.start_timestamp < '{self.query_end_cap}' AND
				    fsv.user_tier = 'premium' AND
				    de.air_date >= date_add('days', -{self.look_back_window}, '{self.period_starting}')
				GROUP BY 1,2,3,4
				ORDER BY 1,2,3,4
				), view_totals as (
				SELECT
				    fsv2.user_key,
				    sum(fsv2.active_seconds) as total_seconds_viewed,
				    count(*) as total_views
				FROM
				    warehouse.vod_viewership fsv2
				WHERE
				    fsv2.start_timestamp >= '{self.period_starting}' AND
				    fsv2.start_timestamp < '{self.query_end_cap}' AND
				    fsv2.user_tier = 'premium'
				GROUP BY 1
				), votes as (
				    SELECT v.user_key,
				    	   v.channel_title,
				           v.series_title,
				           v.season_title,
				           cast(v.seconds_viewed as float) / vt.total_seconds_viewed as hours_vote,
				           sum(v.seconds_viewed) as seconds_viewed,
				           cast(v.user_views as float) / vt.total_views as views_vote,
				           sum(v.user_views) as total_views
				    FROM viewers v
				    LEFT JOIN view_totals vt on v.user_key = vt.user_key
				    GROUP BY 1,2,3,4,5,7
				    order by 1, 3 desc
				)
				SELECT
					votes.channel_title,
				    votes.series_title,
				    votes.season_title,
				    sum(votes.hours_vote) as vote_share_hours,
				    sum(votes.seconds_viewed) as seconds_viewed,
				    sum(votes.views_vote) as vote_share_views,
				    sum(votes.total_views) as total_views
				FROM votes
				WHERE votes.hours_vote is not NULL
				GROUP BY 1,2,3
				ORDER BY 4 desc; """
		results = self.db_connector.read_redshift(query)
		records = []
		for result in results:
			record = {
				'period_ending_date': self.period_ending,
				'channel_title': result[0],
				'series_title': result[1],
				'season_title': result[2],
				'vote_share_hours': result[3],
				'seconds_viewed': result[4],
				'vote_share_views': result[5],
				'total_views': result[6]			}

			records.append(record)
		self.final_dataframe = pd.DataFrame(records)


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Vote Share Combined Premium Windowed for {self.target_date}")
		self.load_data()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
