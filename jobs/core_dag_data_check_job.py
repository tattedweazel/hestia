from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class CoreDagDataCheckJob(EtlJobV3):

	def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
		super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector)
		self.today = datetime.now()
		self.max_vod_viewership = None


	def get_latest_vod_viewership(self):
		self.loggerv3.info('Getting latest vod viewership')
		query = """
				SELECT
		            max(start_timestamp) as max_vod_viewership
		        FROM warehouse.vod_sessions;
		        """
		results = self.db_connector.read_redshift(query)
		for result in results:
			self.max_vod_viewership = result[0]


	def determine_core_run(self):
		self.loggerv3.info('Determining core run')
		delta = self.today - self.max_vod_viewership
		duration_in_seconds = delta.total_seconds()
		duration_in_hours = divmod(duration_in_seconds, 3600)[0]
		if duration_in_hours > 24:
			raise IOError('Core Dag did not run!')



	def execute(self):
		self.loggerv3.info(f"Core Dag Data Check Job for {self.today}")
		self.get_latest_vod_viewership()
		self.determine_core_run()
		self.loggerv3.success("All Processing Complete!")