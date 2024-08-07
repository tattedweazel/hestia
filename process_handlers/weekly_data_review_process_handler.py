import config
from base.server_process_handler import ServerProcessHandler
from datetime import datetime, timedelta
from jobs.agg_weekly_site_apps_viewership import AggWeeklySiteAppsViewership
from jobs.youtube_weekly_narratives_job import YouTubeWeeklyNarrativesJob
from jobs.audio_weekly_narratives_job import AudioWeeklyNarrativesJob
from jobs.site_weekly_narratives_job import SiteWeeklyNarrativesJob
from jobs.output_narratives_job import OutputNarrativesJob
from jobs.log_file_handler_job import LogFileHandlerJob
from tools.modules.yt_scraper.vid_scraper.yt_video_scraper import YTVideoScraper
from utils.components.dater import Dater
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class WeeklyDataReviewProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        """Ran manually on Wednesdays"""
        if local_mode:
            self.file_location = ''
        else:
            self.file_location = '/home/centos/processes/rt-data-hestia/'
        config.file_location = self.file_location
        config.local_mode = local_mode
        config.process_handler = __name__
        self.dater = Dater()
        self.connector = DatabaseConnector(config.file_location, config.dry_run)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.loggerv3.disable_alerting()

    def run_jobs(self):
        self.loggerv3.start('Starting Weekly Data Review Process Handler')

        start_period = '2024-02-11'
        end_period = (datetime.strptime(start_period, '%Y-%m-%d') + timedelta(7)).strftime('%Y-%m-%d')  # This is the last day of the week PLUS 1 Day
        run_date = (datetime.strptime(start_period, '%Y-%m-%d') + timedelta(10)).strftime('%Y-%m-%d')  # This is the Wednesday following the previous week
        backfill = False  # Always set this value!
        today = self.dater.format_date(self.dater.get_today())

        # Data Collection & Aggregation
        yt = YTVideoScraper(start_period=start_period, end_period=end_period, run_date=run_date, backfill=backfill)
        yt.execute()

        awsav = AggWeeklySiteAppsViewership(target_date=start_period, db_connector=self.connector)
        awsav.execute()

        # Narratives
        ytwnj = YouTubeWeeklyNarrativesJob(db_connector=self.connector)
        ytwnj.execute()

        awnj = AudioWeeklyNarrativesJob(db_connector=self.connector)
        awnj.execute()

        swnj = SiteWeeklyNarrativesJob(db_connector=self.connector)
        swnj.execute()

        onj = OutputNarrativesJob(target_date=today)
        onj.execute()


        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
