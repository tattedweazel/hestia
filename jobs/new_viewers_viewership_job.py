import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class NewViewersViewershipJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'new_viewers_viewership')

		self.target_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
		self.final_df = None


	def load_viewership(self):
		self.loggerv3.info("Loading New Viewer Viewership...")
		query = f""" 
					WITH last_14_days as (
					    SELECT 
					    	user_key,
					    	user_tier
					    FROM warehouse.vod_viewership
					    WHERE user_key is not null
					      AND user_tier in ('premium', 'trial', 'free')
					      AND start_timestamp >= dateadd('days', -13, '{self.target_date}')
					      AND start_timestamp < dateadd('days', 1, '{self.target_date}')
					     GROUP BY 1,2
					), previous_viewers as (
					    SELECT distinct user_key
					    FROM warehouse.vod_viewership
					    WHERE user_key is not null
					      AND user_tier in ('premium', 'trial', 'free')
					      AND start_timestamp < dateadd('days', -13, '{self.target_date}')
					), users as (
						SELECT
					    	user_key,
					    	user_tier
					  	FROM last_14_days l14d
					  	WHERE user_key not in (SELECT user_key FROM previous_viewers)
					) SELECT
						dateadd('days', -13, '{self.target_date}') as cohort_start_date,
						'{self.target_date}' as cohort_end_date,
						u.user_key,
						u.user_tier,
						dse.series_id,
						dse.season_id,
						vv.episode_key
					  FROM warehouse.vod_viewership vv
					  INNER JOIN users u on vv.user_key = u.user_key and vv.user_tier = u.user_tier
					  INNER JOIN warehouse.dim_segment_episode dse on vv.episode_key = dse.episode_key
					  WHERE 
					  	vv.start_timestamp >= dateadd('days', -13, '{self.target_date}') AND
					    vv.start_timestamp < dateadd('days', 1, '{self.target_date}')
					  GROUP BY 1,2,3,4,5,6,7;

		"""

		results = self.db_connector.read_redshift(query)
		viewership = []
		for result in results:
			viewership.append({
					"cohort_start_date": result[0],
					"cohort_end_date": result[1],
					"user_key": result[2],
					"user_tier": result[3],
					"series_id": result[4],
					"season_id": result[5],
					"episode_key": result[6]
				})
		self.final_df = pd.DataFrame(viewership)


	def write_to_red_shift(self):
		self.loggerv3.info("Writing to Red Shift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, chunksize=5000, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running New Viewers Viewership Job for Cohort ending {self.target_date}")
		self.load_viewership()
		self.write_to_red_shift()
		self.loggerv3.success("All Processing Complete!")
