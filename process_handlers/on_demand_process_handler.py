import config
from base.server_process_handler import ServerProcessHandler
from utils.connectors.database_connector import DatabaseConnector
from utils.components.dater import Dater
from utils.components.loggerv3 import Loggerv3


class OnDemandProcessHandler(ServerProcessHandler):

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
        self.loggerv3.disable_alerting()


    def run_jobs(self):
        self.loggerv3.start('Starting On Demand Process Handler')


        self.loggerv3.success("All Processing Complete!")
