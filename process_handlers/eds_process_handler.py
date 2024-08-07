import config
from base.server_process_handler import ServerProcessHandler
from jobs.core_dag_data_check_job import CoreDagDataCheckJob
from utils.components.dater import Dater
from utils.connectors.database_connector import DatabaseConnector
from jobs.agg_daily_general_engagement_job_v2 import AggDailyGeneralEngagementJobV2
from jobs.agg_daily_livestream_job_v2 import AggDailyLivestreamJobV2
from jobs.agg_daily_vod_job_v2 import AggDailyVodJobV2
from jobs.agg_weekly_general_engagement_job_v2 import AggWeeklyGeneralEngagementJobV2
from jobs.agg_weekly_livestream_job_v2 import AggWeeklyLivestreamJobV2
from jobs.agg_weekly_vod_job_v2 import AggWeeklyVodJobV2
from jobs.cancellation_request_lookbacks_job import CancellationRequestLookbacksJob
from jobs.daily_at_risk_balance_job import DailyAtRiskBalanceJob
from jobs.daily_median_platform_viewership_job import DailyMedianPlatformViewershipJob
from jobs.daily_signups_job import DailySignupsJob
from jobs.daily_subscription_pauses_job import DailySubscriptionPausesJob
from jobs.first_two_weeks_viewership_job import FirstTwoWeeksViewershipJob
# from jobs.ftp_viewership_lookback_job import FtpViewershipLookBackJob
from jobs.new_viewers_job import NewViewersJob
from jobs.new_viewers_viewership_job import NewViewersViewershipJob
from jobs.new_viewer_retention_job import NewViewerRetentionJob
from jobs.pending_cancel_balance_job import PendingCancelBalanceJob
from jobs.premium_attributions_job import PremiumAttributionsJob
from jobs.reactivated_viewers_job import ReactivatedViewersJob
from jobs.returning_attributions_job import ReturningAttributionsJob
from jobs.signup_attributions_job import SignupAttributionsJob
# from jobs.trial_attribution_v2_job import TrialAttributionV2Job
# from jobs.trial_behavior_v2_job import TrialBehaviorV2Job
from jobs.vod_viewership_job import VodViewershipJob
from jobs.weekly_median_platform_viewership_job import WeeklyMedianPlatformViewershipJob
from jobs.livestream_viewership_job import LivestreamViewershipJob
from utils.components.loggerv3 import Loggerv3


class EdsProcessHandler(ServerProcessHandler):

	def __init__(self, local_mode):
		if local_mode:
			self.file_location = ''
		else:
			self.file_location = '/home/ubuntu/processes/rt-data-hestia/'
		self.dater = Dater()
		config.file_location = self.file_location
		config.local_mode = local_mode
		config.process_handler = __name__
		self.connector = DatabaseConnector(config.file_location, config.dry_run)
		self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location)
		self.loggerv3.disable_alerting()


	def run_jobs(self):
		self.loggerv3.start('Starting EDS Process Handler')

		today = self.dater.format_date(self.dater.get_today())
		yesterday = self.dater.format_date(self.dater.find_previous_day(self.dater.get_today()))
		two_days_ago = self.dater.format_date(self.dater.find_x_days_ago(self.dater.get_today(), 2))
		seven_days_ago = self.dater.format_date(self.dater.find_x_days_ago(self.dater.get_today(), 7))
		ten_days_ago = self.dater.format_date(self.dater.find_x_days_ago(self.dater.get_today(), 10))

		cddcj = CoreDagDataCheckJob(db_connector=self.connector)
		cddcj.execute()

		vvj = VodViewershipJob(target_date = yesterday, db_connector = self.connector)
		vvj.execute()

		lvj = LivestreamViewershipJob(target_date = yesterday, db_connector = self.connector)
		lvj.execute()

		# Commenting out due to removal of public trials from site
		# taj = TrialAttributionV2Job(target_date = seven_days_ago, db_connector = self.connector)
		# taj.execute()

		sa = SignupAttributionsJob(target_date = two_days_ago, db_connector = self.connector)
		sa.execute()

		ra = ReturningAttributionsJob(target_date = two_days_ago, db_connector = self.connector)
		ra.execute()

		dmpv = DailyMedianPlatformViewershipJob(target_date = yesterday, db_connector = self.connector)
		dmpv.execute()

		wmpv = WeeklyMedianPlatformViewershipJob(target_date = yesterday, db_connector = self.connector)
		wmpv.execute()

		pcbj = PendingCancelBalanceJob(target_date = yesterday, db_connector = self.connector)
		pcbj.execute()

		crlbj = CancellationRequestLookbacksJob(target_date = yesterday, db_connector = self.connector)
		crlbj.execute()

		dspj = DailySubscriptionPausesJob(target_date = yesterday, db_connector = self.connector)
		dspj.execute()

		adlj = AggDailyLivestreamJobV2(target_date = yesterday, db_connector = self.connector)
		adlj.execute()

		awlj = AggWeeklyLivestreamJobV2(target_date = yesterday, db_connector = self.connector)
		awlj.execute()

		adgejv2 = AggDailyGeneralEngagementJobV2(target_date = yesterday, db_connector = self.connector)
		adgejv2.execute()

		awgejv2 = AggWeeklyGeneralEngagementJobV2(target_date = yesterday, db_connector = self.connector)
		awgejv2.execute()

		advjv2 = AggDailyVodJobV2(target_date = yesterday, db_connector = self.connector)
		advjv2.execute()

		awvjv2 = AggWeeklyVodJobV2(target_date = yesterday, db_connector = self.connector)
		awvjv2.execute()

		nvj = NewViewersJob(target_date = yesterday, db_connector = self.connector)
		nvj.execute()

		nvrj = NewViewerRetentionJob(target_date = today, db_connector = self.connector)
		nvrj.execute()

		darbj = DailyAtRiskBalanceJob(target_date = today, db_connector = self.connector)
		darbj.execute()

		ftwsvj = FirstTwoWeeksViewershipJob(target_date = today, db_connector = self.connector)
		ftwsvj.execute()

		# Commenting out due to removal of public trials from site
		# fvlj = FtpViewershipLookBackJob(target_date=yesterday, db_connector=self.connector)
		# fvlj.execute()

		nvvj = NewViewersViewershipJob(target_date=yesterday, db_connector=self.connector)
		nvvj.execute()

		rvj = ReactivatedViewersJob(target_date=yesterday, db_connector=self.connector)
		rvj.execute()

		daj = DailySignupsJob(target_date=two_days_ago, db_connector=self.connector)
		daj.execute()

		paj = PremiumAttributionsJob(target_date=two_days_ago, db_connector=self.connector)
		paj.execute()

		# Commenting out due to removal of public trials from site
		# tb = TrialBehaviorV2Job(target_date=ten_days_ago, db_connector=self.connector)
		# tb.execute()


		self.loggerv3.success("All Processing Complete!")
