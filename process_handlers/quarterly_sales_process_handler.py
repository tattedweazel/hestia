import config
from base.server_process_handler import ServerProcessHandler
from jobs.quarterly_sales_actuals_job import QuarterlySalesActualsJob
from jobs.quarterly_sales_estimates_job import QuarterlySalesEstimatesJob
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class QuarterlySalesProcessHandler(ServerProcessHandler):

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
        self.loggerv3.start('Starting Quarterly Sales Process Handler')

        """
        Note: When running "Estimates" job, comment out "Actuals" job.
        Only run "Actuals" once they have been determined by Data and Sales team.
        """

        quarter_start = '2023-10-01'  # First day of the quarter
        quarter_end = '2024-01-01'  # Min value of (Last day of the quarter + 1, Run Date - 6)
        data_from_quarter = 'Q4 2023'
        for_quarter = 'Q1 2024'

        qsej = QuarterlySalesEstimatesJob(quarter_start=quarter_start, quarter_end=quarter_end, data_from_quarter=data_from_quarter, for_quarter=for_quarter, db_connector=self.connector)
        qsej.execute()


        # actuals_file_name = 'q1_actuals.csv'
        # for_quarter_date = '2024-01-01'
        # for_quarter_end_date = '2024-04-01'
        #
        # qsaj = QuarterlySalesActualsJob(file_name=actuals_file_name, for_quarter_date=for_quarter_date, for_quarter_end_date=for_quarter_end_date, db_connector=self.connector)
        # qsaj.execute()


        self.loggerv3.success("All Processing Complete!")
