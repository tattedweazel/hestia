import config
from base.server_process_handler import ServerProcessHandler
from utils.components.dater import Dater
from utils.connectors.database_connector import DatabaseConnector
from jobs.agg_monthly_premium_attribution_job import AggMonthlyPremiumAttributionJob
from jobs.series_brand_mapper_job import SeriesBrandMapperJob
from jobs.log_file_handler_job import LogFileHandlerJob
from utils.components.loggerv3 import Loggerv3



class MonthlyPremiumAttributionsProcessHandler(ServerProcessHandler):

	def __init__(self, local_mode: bool, target_date: str = '2024-02-01', dry_run: bool = False ):
		if local_mode:
			config.file_location = ''
		else:
			config.file_location = '/home/centos/processes/rt-data-hestia/'
		self.target_date = target_date
		self.dater = Dater()
		config.local_mode = local_mode
		config.process_handler = __name__
		self.dry_run = True if config.dry_run is True or dry_run is True else False
		self.connector = DatabaseConnector(config.file_location, self.dry_run)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
		self.loggerv3.disable_alerting()


	def run_jobs(self):
		self.loggerv3.start('Starting Monthly Premium Attributions Process Handler')

		"""Jobs has to be ran at least 5 days after the month, but the target date is still first of the previous month"""

		sbmj = SeriesBrandMapperJob(db_connector = self.connector, target_date = self.target_date)
		sbmj.execute()

		ampaj = AggMonthlyPremiumAttributionJob(db_connector = self.connector, target_date = self.target_date)
		ampaj.execute()

		lfhj = LogFileHandlerJob()
		lfhj.execute()

		self.loggerv3.success("All Processing Complete!")
