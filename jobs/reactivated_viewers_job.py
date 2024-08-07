import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime
from dateutil.relativedelta import relativedelta


class ReactivatedViewersJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'reactivated_viewers')
		self.target_date = target_date
		self.target_date_dt = datetime.strptime(target_date, '%Y-%m-%d')
		self.next_date_dt = self.target_date_dt + relativedelta(days=1)
		self.next_date = self.next_date_dt.strftime('%Y-%m-%d')
		self.final_df = None


	def load_new_viewers(self):
		self.loggerv3.info("Loading Reactivated Viewers...")
		records = []
		query = f""" 
			WITH current_vod_viewers AS (
				SELECT user_key,
					   max(user_tier) as user_tier
				FROM warehouse.vod_viewership
				WHERE user_key is not NULL
				  AND user_tier in ('premium', 'trial', 'free')
				  AND start_timestamp >= '{self.target_date}'
				  AND start_timestamp < '{self.next_date}'
				GROUP BY 1
			), current_live_viewers AS (
				SELECT du.user_key,
					   max(user_tier) as user_tier
				FROM warehouse.livestream_viewership lv
				INNER JOIN warehouse.dim_user du on du.user_id = lv.user_id
				WHERE lv.user_id is not NULL
				  AND lv.user_tier in ('premium', 'trial', 'free')
				  AND lv.start_timestamp >= '{self.target_date}'
				  AND lv.start_timestamp < '{self.next_date}'
				GROUP BY 1
			), current_viewers as (
				SELECT user_key, user_tier
				FROM current_vod_viewers
				UNION
				SELECT user_key, user_tier
				FROM current_live_viewers
			), deduped_viewers as (
				SELECT user_key,
					   max(user_tier) as user_tier
				FROM current_viewers
				GROUP BY 1
			),more_than_180_days_vod AS (
				SELECT distinct user_key
				FROM warehouse.vod_viewership
				WHERE user_key is not NULL
				  AND user_tier in ('premium', 'trial', 'free')
				  AND start_timestamp < date_add('day', -180, '{self.target_date}')
				  AND user_key in (SELECT user_key FROM current_viewers)
			), more_than_180_days_live AS (
				SELECT distinct lv.user_id, du.user_key
				FROM warehouse.livestream_viewership lv
				INNER JOIN warehouse.dim_user du on du.user_id = lv.user_id
				WHERE lv.user_id is not NULL
				  AND lv.user_tier in ('premium', 'trial', 'free')
				  AND lv.start_timestamp < date_add('day', -180, '{self.target_date}')
				  AND du.user_key in (SELECT user_key FROM current_viewers)
			), within_180_days_vod AS (
				SELECT distinct user_key
				FROM warehouse.vod_viewership
				WHERE user_key is not NULL
				  AND user_tier in ('premium', 'trial', 'free')
				  AND start_timestamp < '{self.target_date}'
				  AND start_timestamp >= date_add('day', -180, '{self.target_date}')
				  AND user_key in (SELECT user_key FROM current_viewers)
			), within_180_days_live AS (
				SELECT distinct lv.user_id, du.user_key
				FROM warehouse.livestream_viewership lv
				INNER JOIN warehouse.dim_user du on du.user_id = lv.user_id
				WHERE lv.user_id is not NULL
				  AND lv.user_tier in ('premium', 'trial', 'free')
				  AND lv.start_timestamp < '{self.target_date}'
				  AND lv.start_timestamp >= date_add('day', -180, '{self.target_date}')
				  AND du.user_key in (SELECT user_key FROM current_viewers)
			)
			SELECT user_key, 
				   user_tier
			FROM deduped_viewers
			WHERE
				user_key not in (SELECT user_key FROM within_180_days_vod)
				AND user_key not in (SELECT user_key FROM within_180_days_live)
				AND
				(
				   user_key in (SELECT user_key FROM more_than_180_days_vod)
				   OR user_key in (SELECT user_key FROM more_than_180_days_live)
				);
		"""
		results = self.db_connector.read_redshift(query)
		for result in results:
			records.append({
				"run_date": self.target_date,
				"user_key": result[0],
				"user_tier": result[1]
			})

		self.final_df = pd.DataFrame(records)


	def write_to_redshift(self):
		self.loggerv3.info("Writing to Redshift...")
		self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Reactivated Viewers Job for {self.target_date}")
		self.load_new_viewers()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
