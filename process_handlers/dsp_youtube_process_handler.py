import config
from base.server_process_handler import ServerProcessHandler
from jobs.log_file_handler_job import LogFileHandlerJob
from jobs.dim_youtube_video_job import DimYouTubeVideoJob
from jobs.youtube_download_reporting_job import YouTubeDownloadReportingJob
from jobs.youtube_write_reporting_job import YouTubeWriteReportingJob
# from jobs.youtube_title_matching_job import YouTubeTitleMatchingJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class DspYouTubeProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        self.local_mode = local_mode
        if local_mode:
            self.file_location = ''
        else:
            self.file_location = '/home/ubuntu/processes/rt-data-hestia/'
        self.dater = Dater()
        config.file_location = self.file_location
        config.local_mode = self.local_mode
        config.process_handler = __name__
        self.connector = DatabaseConnector(config.file_location, config.dry_run)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location)
        self.loggerv3.disable_alerting()


    def run_jobs(self):
        self.loggerv3.start('Starting DSP YouTube Process Handler')

        ytdrj = YouTubeDownloadReportingJob(db_connector=self.connector, file_location=self.file_location)
        ytdrj.execute()

        ytwrj = YouTubeWriteReportingJob(db_connector=self.connector, file_location=self.file_location)
        ytwrj.execute()

        dytvj = DimYouTubeVideoJob(db_connector=self.connector, file_location=self.file_location)
        dytvj.execute()

        # tytmj = YouTubeTitleMatchingJob(db_connector=self.connector)
        # tytmj.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
