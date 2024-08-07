import config
from base.server_process_handler import ServerProcessHandler
from jobs.premium_attributions_v3_job import PremiumAttributionsV3Job
from jobs.core_dag_data_check_job import CoreDagDataCheckJob
from jobs.agg_daily_membership_job import AggDailyMembershipJob
from jobs.agg_daily_membership_job_v2 import AggDailyMembershipJobV2
from jobs.agg_daily_rttv_viewership_job_v2 import AggDailyRttvViewershipJobV2
from jobs.comments_job import CommentsJob
from jobs.daily_combined_yt_rt_viewership_job import DailyCombinedYtRtViewershipJob
from jobs.daily_web_signups_job import DailyWebSignupsJob
from jobs.decile_reporting_job import DecileReportingJob
from jobs.deleted_comments_job import DeletedCommentsJob
from jobs.dim_episode_job import DimEpisodeJob
from jobs.dim_shopify_rt_job import DimShopifyRtJob
from jobs.dim_content_blocks_job import DimContentBlocksJob
from jobs.engagements_job import EngagementsJob
from jobs.giftcards_job import GiftcardsJob
from jobs.livestream_schedule_job_v2 import LivestreamScheduleJobV2
from jobs.subscription_count_job_v2 import SubscriptionCountJobV2
from jobs.staff_created_comments_job import StaffCreatedCommentsJob
from jobs.staff_created_posts_job import StaffCreatedPostsJob
from jobs.signup_flow_campaign_attributions_job import SignupFlowCampaignAttributionsJob
from jobs.youtube_rt_members_job import YouTubeRTMembersJob
from jobs.weekly_audience_fan_community_job import WeeklyAudienceFanCommunityJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class DspProcessHandler(ServerProcessHandler):

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
		self.loggerv3.start('Starting DSP Process Handler')

		yesterday = self.dater.format_date(self.dater.find_previous_day(self.dater.get_today()))
		four_days_ago = self.dater.format_date(self.dater.find_x_days_ago(self.dater.get_today(), 4))

		cddcj = CoreDagDataCheckJob(db_connector=self.connector)
		cddcj.execute()

		gcj = GiftcardsJob(target_date = yesterday, db_connector = self.connector)
		gcj.execute()

		dej = DimEpisodeJob(db_connector = self.connector)
		dej.execute()

		dsrj = DimShopifyRtJob(db_connector = self.connector)
		dsrj.execute()

		ej = EngagementsJob(target_date = yesterday, db_connector = self.connector)
		ej.execute()

		cj = CommentsJob(target_date = yesterday, db_connector = self.connector)
		cj.execute()

		admj = AggDailyMembershipJob(target_date = yesterday, db_connector = self.connector)
		admj.execute()

		dcj = DeletedCommentsJob(target_date = yesterday, db_connector = self.connector)
		dcj.execute()

		sccj = StaffCreatedCommentsJob(target_date = yesterday, db_connector = self.connector)
		sccj.execute()

		scpj = StaffCreatedPostsJob(target_date = yesterday, db_connector = self.connector)
		scpj.execute()

		wafcj = WeeklyAudienceFanCommunityJob(target_date=yesterday, db_connector=self.connector)
		wafcj.execute()

		scjv2 = SubscriptionCountJobV2(target_date=yesterday, db_connector=self.connector)
		scjv2.execute()

		admjv2 = AggDailyMembershipJobV2(target_date=yesterday, db_connector=self.connector)
		admjv2.execute()

		dcyrvj = DailyCombinedYtRtViewershipJob(target_date=yesterday, db_connector=self.connector)
		dcyrvj.execute()

		dwsj = DailyWebSignupsJob(target_date=yesterday, db_connector=self.connector)
		dwsj.execute()

		lsjv2 = LivestreamScheduleJobV2(target_date=yesterday, db_connector=self.connector)
		lsjv2.execute()

		adrvjv2 = AggDailyRttvViewershipJobV2(target_date=yesterday, db_connector=self.connector)
		adrvjv2.execute()

		dcbj = DimContentBlocksJob(db_connector = self.connector)
		dcbj.execute()

		tyrtmj = YouTubeRTMembersJob(db_connector=self.connector)
		tyrtmj.execute()

		sfcaj = SignupFlowCampaignAttributionsJob(db_connector=self.connector)
		sfcaj.execute()

		pajv3 = PremiumAttributionsV3Job(target_date=four_days_ago, db_connector=self.connector)
		pajv3.execute()

		drj = DecileReportingJob(target_date=yesterday, db_connector=self.connector)
		drj.execute()

		self.loggerv3.success("All Processing Complete!")
