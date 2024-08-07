import config
from base.server_process_handler import ServerProcessHandler
from jobs.daily_tubular_metrics_job_v2 import DailyTubularMetricsJobV2
from jobs.tubular_sales_metrics_job import TubularSalesMetricsJob
from jobs.megaphone_sales_metrics_job import MegaphoneSalesMetricsJob
from jobs.misc_sales_metrics_job import MiscSalesMetricsJob
from jobs.roosterteeth_sales_metrics_jobs import RoosterteethSalesMetricsJob
from jobs.log_file_handler_job import LogFileHandlerJob
from jobs.sales_metrics_episode_forecasts_job import SalesMetricsEpisodeForecastsJob
from jobs.sales_metrics_weekly_averages_job import SalesMetricsWeeklyAverageJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class SalesMetricsProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        """This is currently not a scheduled job. It is ran manually with user inputted date(s)"""
        if local_mode:
            self.file_location = ''
        else:
            self.file_location = '/home/ubuntu/processes/rt-data-hestia/'
        self.dater = Dater()
        self.target_date = self.dater.format_date(self.dater.get_today())
        config.file_location = self.file_location
        config.local_mode = local_mode
        config.process_handler = __name__
        self.connector = DatabaseConnector(config.file_location, config.dry_run)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.loggerv3.disable_alerting()


    def run_jobs(self):
        self.loggerv3.start('Starting Sales Metrics Process Handler')


        dtmj = DailyTubularMetricsJobV2(target_date=self.target_date, db_connector=self.connector, file_location=self.file_location)
        dtmj.execute()

        # Start Aggregating Sales Metrics
        tsmj = TubularSalesMetricsJob(target_date=self.target_date, db_connector=self.connector)
        tsmj.execute()

        msmj = MegaphoneSalesMetricsJob(target_date=self.target_date, db_connector=self.connector)
        msmj.execute()

        rsmj = RoosterteethSalesMetricsJob(target_date=self.target_date, db_connector=self.connector)
        rsmj.execute()

        msmj = MiscSalesMetricsJob(target_date=self.target_date, db_connector=self.connector)
        msmj.execute()
        # End Aggregating Sales Metrics

        smfj = SalesMetricsEpisodeForecastsJob(target_date=self.target_date, db_connector=self.connector)
        smfj.execute()

        smwaj = SalesMetricsWeeklyAverageJob(target_date=self.target_date, db_connector=self.connector)
        smwaj.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
