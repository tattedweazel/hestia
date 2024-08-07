import config
from base.server_process_handler import ServerProcessHandler
from jobs.log_file_handler_job import LogFileHandlerJob
from jobs.sv2_table_usage_job import Sv2TableUsageJob
from jobs.file_identifier_job import FileIdentifierJob
from jobs.anomaly_detection_job import AnomalyDetectionJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class DataMonitoringProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        if local_mode:
            self.file_location = ''
        else:
            self.file_location = '/home/ubuntu/processes/rt-data-hestia/'
        self.dater = Dater()
        config.file_location = self.file_location
        config.local_mode = local_mode
        config.process_handler = __name__
        self.connector = DatabaseConnector(config.file_location, dry_run=config.dry_run)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location)
        self.loggerv3.disable_alerting()


    def run_jobs(self):
        self.loggerv3.start('Starting Data Monitoring Process Handler')

        today = self.dater.format_date(self.dater.get_today())

        adj = AnomalyDetectionJob(target_date=today, db_connector=self.connector)
        adj.execute()

        fij = FileIdentifierJob(db_connector=self.connector)
        fij.execute()

        stuj = Sv2TableUsageJob(target_date=today, db_connector=self.connector)
        stuj.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
