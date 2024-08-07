import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class VodViewershipJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'vod_viewership')

		self.Dater = DateHandler()
		self.target_day = self.target_date.replace('-','')
		self.next_day = self.Dater.find_next_day(self.target_day)
		self.formatted_dates = self.get_formatted_dates()
		self.viewers = []
		self.final_dataframe = None


	def get_formatted_dates(self):
		return {
			"target": self.Dater.format_date(self.target_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def load_viewer_records(self):
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f""" SELECT
														    max(session_id) as session_id,
														    cast(user_key as bigint) as user_key,
														    anonymous_id,
														    user_tier,
														    episode_key,
														    sum(CASE
														            WHEN active_seconds > duration_seconds THEN duration_seconds
														            ELSE active_seconds
														        END) as active_seconds,
														    count(*) as sessions,
														    min(start_timestamp) as start_timestamp,
														    max(platform) as platform,
														    max(max_position) as max_position
														FROM warehouse.vod_sessions
														WHERE
														    start_timestamp >= '{target_day}' AND
														    start_timestamp < '{next_day}'
														GROUP BY 2,3,4,5
														ORDER BY 8;""")

		for result in results:
			view_record = {
				"session_id": result[0],
				"user_key": result[1],
				"anonymous_id": result[2],
				"user_tier": result[3],
				"episode_key": result[4],
				"active_seconds": result[5],
				"sessions": result[6],
				"start_timestamp": result[7],
				"platform": result[8],
				"max_position": result[9]
			}
			self.viewers.append(view_record)


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		self.final_dataframe = pd.DataFrame(self.viewers)
		self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, chunksize=5000, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running VOD Viewership for {self.formatted_dates['target']}")
		self.load_viewer_records()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
