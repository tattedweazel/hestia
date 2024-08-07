import config
from base.server_process_handler import ServerProcessHandler
from archive.daily_tubular_metrics_job import DailyTubularMetricsJob
from archive.agg_daily_tubular_podcast_metrics_job import AggDailyTubularPodcastMetricsJob
from archive.agg_daily_megaphone_podcast_metrics_job import AggDailyMegaphonePodcastMetricsJob
from archive.agg_daily_platform_podcast_metrics_job import AggDailyPlatformPodcastMetricsJob
from jobs.log_file_handler_job import LogFileHandlerJob
from archive.podcast_averages_job import PodcastAveragesJob
from archive.podcast_forecasts_job import PodcastForecastsJob
from archive.podcast_targets_job import PodcastTargetsJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class PodcastReportingProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        """This is currently not a scheduled job. It is ran manually with user inputted dates"""
        if local_mode:
            self.file_location = ''
        else:
            self.file_location = '/home/ubuntu/processes/rt-data-hestia/'
        self.dater = Dater()
        config.file_location = self.file_location
        config.local_mode = local_mode
        config.process_handler = __name__
        self.connector = DatabaseConnector(config.file_location, config.dry_run)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.loggerv3.disable_alerting()


    def run_jobs(self):
        self.loggerv3.start('Starting Podcast Reporting Process Handler')

        target_date = '2023-07-10'  # This is the pull date of the tubular report

        dtmj = DailyTubularMetricsJob(target_date=target_date, db_connector=self.connector, file_location=self.file_location)
        dtmj.execute()

        # Start Daily Podcast Metrics
        adtpmj = AggDailyTubularPodcastMetricsJob(target_date=target_date, db_connector=self.connector)
        adtpmj.execute()

        admpmj = AggDailyMegaphonePodcastMetricsJob(target_date=target_date, db_connector=self.connector)
        admpmj.execute()

        adppmj = AggDailyPlatformPodcastMetricsJob(target_date=target_date, db_connector=self.connector)
        adppmj.execute()
        # End Daily Podcast Metrics

        pfj = PodcastForecastsJob(target_date=target_date, db_connector=self.connector)
        pfj.execute()

        ptj = PodcastTargetsJob(target_date=target_date, db_connector=self.connector, file_location=self.file_location)
        ptj.execute()

        paj = PodcastAveragesJob(target_date=target_date, db_connector=self.connector)
        paj.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
