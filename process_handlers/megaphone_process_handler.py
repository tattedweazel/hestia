import config
from base.server_process_handler import ServerProcessHandler
from jobs.log_file_handler_job import LogFileHandlerJob
from jobs.dim_megaphone_job import DimMegaphoneJob
from jobs.megaphone_impressions_job import MegaphoneImpressionsJob
from jobs.megaphone_metrics_job import MegaphoneMetricsJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class MegaphoneProcessHandler(ServerProcessHandler):

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
        self.loggerv3.start('Starting DSP Megaphone Process Handler')

        two_days_ago = self.dater.format_date(self.dater.find_x_days_ago(self.dater.get_today(), 2))

        dmj = DimMegaphoneJob(db_connector=self.connector, file_location=self.file_location)
        dmj.execute()

        mij = MegaphoneImpressionsJob(target_date=two_days_ago, db_connector=self.connector, file_location=self.file_location)
        mij.execute()

        mmj = MegaphoneMetricsJob(target_date=two_days_ago, db_connector=self.connector, file_location=self.file_location)
        mmj.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
