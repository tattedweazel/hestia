import config
from base.server_process_handler import ServerProcessHandler
from jobs.site_channel_trajectory_job import SiteChannelTrajectoryJob
from jobs.site_series_trajectory_job import SiteSeriesTrajectoryJob
from jobs.site_series_catalog_trajectory_job import SiteSeriesCatalogTrajectoryJob
from jobs.youtube_channel_trajectory_job import YoutubeChannelTrajectoryJob
from jobs.audio_trajectory_job import AudioTrajectoryJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class ChannelTrajectoryProcessHandler(ServerProcessHandler):

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
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)


    def run_jobs(self):
        self.loggerv3.start('Starting Channel Trajectory Process Handler')

        yesterday = self.dater.format_date(self.dater.find_previous_day(self.dater.get_today()))
        four_days_ago = self.dater.format_date(self.dater.find_x_days_ago(self.dater.get_today(), 4))

        sctj = SiteChannelTrajectoryJob(yesterday, self.connector) #yesterday
        sctj.execute()

        ytctj = YoutubeChannelTrajectoryJob(four_days_ago, self.connector) #four_days_ago
        ytctj.execute()
        
        sctj = SiteSeriesTrajectoryJob(yesterday, self.connector) #yesterday
        sctj.execute()
       
        scctj = SiteSeriesCatalogTrajectoryJob(yesterday, self.connector)
        scctj.execute()

        atj = AudioTrajectoryJob(four_days_ago, self.connector)
        atj.execute()

        self.loggerv3.success("All Processing Complete!")
