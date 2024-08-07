import config
import sys
from abc import ABC, abstractmethod
from utils.components.loggerv2 import Loggerv2


class EtlJobV2(ABC):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, table_name = None, jobname=None):
        self.target_date = target_date
        self.db_connector = db_connector
        self.api_connector = api_connector
        self.table_name = table_name
        self.jobname = jobname
        self.file_location = config.file_location
        self.local_mode = config.local_mode
        self.loggerv2 = Loggerv2(name=self.jobname, file_location=self.file_location, local_mode=self.local_mode)
        sys.excepthook = self.loggerv2.handle_uncaught_exception
        super().__init__()


    @abstractmethod
    def execute(self):
        pass
