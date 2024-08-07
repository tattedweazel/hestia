import config
from base.server_process_handler import ServerProcessHandler
from utils.components.dater import Dater
from utils.connectors.database_connector import DatabaseConnector
from jobs.agg_monthly_general_engagement_job import AggMonthlyGeneralEngagementJob
from jobs.agg_monthly_livestream_job import AggMonthlyLivestreamJob
from jobs.agg_monthly_new_viewers_job import AggMonthlyNewViewersJob
from jobs.agg_monthly_retained_new_viewers_job import AggMonthlyRetainedNewViewersJob
from jobs.agg_monthly_vod_job import AggMonthlyVodJob
from jobs.vote_share_combined_job import VoteShareCombinedJob
from jobs.vote_share_combined_premium_job import VoteShareCombinedPremiumJob
from jobs.vote_share_monthly_job import VoteShareMonthlyJob
from jobs.vote_share_monthly_premium_job import VoteShareMonthlyPremiumJob
from jobs.vote_share_monthly_premium_windowed_job import VoteShareCombinedPremiumWindowedJob
from jobs.log_file_handler_job import LogFileHandlerJob
from utils.components.loggerv3 import Loggerv3



class EdsMonthlyProcessHandler(ServerProcessHandler):

	def __init__(self, local_mode: bool, target_date: str = '2023-09-01', dry_run: bool = False ):
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
		self.loggerv3.start('Starting EDS Monthly Process Handler')

		vscj = VoteShareCombinedJob(db_connector = self.connector, target_date = self.target_date)
		vscj.execute()

		vsmj = VoteShareMonthlyJob(db_connector = self.connector, target_date = self.target_date)
		vsmj.execute()

		vscpj = VoteShareCombinedPremiumJob(db_connector = self.connector, target_date = self.target_date)
		vscpj.execute()

		vsmpj = VoteShareMonthlyPremiumJob(db_connector = self.connector, target_date = self.target_date)
		vsmpj.execute()

		vsmpwj = VoteShareCombinedPremiumWindowedJob(db_connector = self.connector, target_date = self.target_date)
		vsmpwj.execute()

		amgej = AggMonthlyGeneralEngagementJob(db_connector = self.connector, target_date = self.target_date)
		amgej.execute()

		amlj = AggMonthlyLivestreamJob(db_connector = self.connector, target_date = self.target_date)
		amlj.execute()

		amvj = AggMonthlyVodJob(db_connector = self.connector, target_date = self.target_date)
		amvj.execute()

		amnvj = AggMonthlyNewViewersJob(db_connector = self.connector, target_date = self.target_date)
		amnvj.execute()

		amrnvj = AggMonthlyRetainedNewViewersJob(db_connector = self.connector, target_date = self.target_date)
		amrnvj.execute()

		lfhj = LogFileHandlerJob()
		lfhj.execute()

		self.loggerv3.success("All Processing Complete!")
