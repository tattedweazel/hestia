import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.components.dater import Dater as DateHandler


class LivestreamViewershipJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'livestream_viewership')

		self.Dater = DateHandler()
		self.target_day = self.target_date.replace('-','')
		self.next_day = self.Dater.find_next_day(self.target_day)
		self.formatted_dates = self.get_formatted_dates()
		self.viewers = []


	def get_formatted_dates(self):
		return {
			"target": self.Dater.format_date(self.target_day),
			"next": self.Dater.format_date(self.next_day)
		}


	def load_viewer_records(self):
		target_day = self.formatted_dates['target']
		next_day = self.formatted_dates['next']
		results = self.db_connector.read_redshift(f""" SELECT
												        CASE
												            WHEN user_id is NOT NULL THEN user_id
												            WHEN user_uuid = 'null' THEN anonymous_id
												            WHEN user_uuid is NOT NULL THEN user_uuid
												            ELSE anonymous_id
												        END as user_id,
												        CASE
												            WHEN lower(user_tier) = 'first' THEN 'premium'
												            WHEN lower(user_tier) = 'free' AND user_uuid = 'null' THEN 'anon'
												            WHEN lower(user_tier) = 'free' AND user_uuid is NULL THEN 'anon'
												            WHEN lower(user_tier) = 'free' AND user_id IS NULL AND user_uuid IS NOT NULL AND user_uuid != 'null' THEN 'free'
												            ELSE lower(user_tier)
												        END as user_tier,
												        platform,
												        MIN(event_timestamp) as start_timestamp,
												        MAX(event_timestamp) as end_timestamp,
												        (COUNT(*)::float * 30) as active_seconds,
												        DATEDIFF('s',
												                 DATE_ADD('s',
												                 -30,
												                 MIN(event_timestamp)),
												            MAX(event_timestamp) ) as duration_seconds,
												        COUNT(distinct session_id) as total_sessions
												    FROM warehouse.livestream_heartbeat
												    WHERE
												        event_timestamp >= '{target_day}' AND
												        event_timestamp < '{next_day}'
												    GROUP BY 1,2,3
												    ORDER BY 6 desc
											""")

		for result in results:
			view_record = {
					"run_date": self.target_date,
					"start_timestamp": result[3],
					"end_timestamp": result[4],
					"user_id": result[0],
					"user_tier": result[1],
					"platform": result[2],
					"active_seconds": result[5],
					"duration_seconds": result[6],
					"total_sessions": result[7]
			}
			if view_record['active_seconds'] > view_record['duration_seconds']:
				view_record['active_seconds'] = view_record['duration_seconds']
			self.viewers.append(view_record)


	def process(self):
		self.load_viewer_records()


	def write_to_redshift(self):
		self.loggerv3.info("Writing results to Red Shift")
		df = pd.DataFrame(self.viewers)
		self.db_connector.write_to_sql(df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
		self.db_connector.update_redshift_table_permissions(self.table_name)


	def execute(self):
		self.loggerv3.info(f"Running Fact Livestream for {self.formatted_dates['target']}")
		self.process()
		self.write_to_redshift()
		self.loggerv3.success("All Processing Complete!")
