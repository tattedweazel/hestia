import os
import shutil
from datetime import datetime
from base.etl_jobv3 import EtlJobV3


class LogFileHandlerJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, jobname=None):
        super().__init__(jobname=__name__)

        self.LIFECYCLE_DAYS = 14
        self.log_file_dir = ''.join([self.file_location, 'logs'])
        self.log_files = None
        self.today = datetime.today()


    def get_log_files(self):
        self.log_files = os.listdir(self.log_file_dir)
        if '.DS_Store' in self.log_files:
            self.log_files.remove('.DS_Store')


    def delete_old_log_files(self):
        for file in self.log_files:
            try:
                log_date = datetime.strptime(file, '%Y-%m-%d')
                day_diff = (self.today - log_date).days
                if day_diff > self.LIFECYCLE_DAYS:
                    log_dir = '/'.join([self.log_file_dir, file])
                    self.loggerv3.info(f'Removing dir {log_dir}')
                    shutil.rmtree(log_dir)
            except ValueError:
                self.loggerv3.exception(f'Incorrect log file format: {file}')


    def execute(self):
        self.loggerv3.start('Starting Log File Handler Job')
        self.get_log_files()
        self.delete_old_log_files()
        self.loggerv3.success("All Processing Complete!")



