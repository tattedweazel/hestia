import pandas as pd
import config
import json
import json_log_formatter
import logging
import os
import traceback
from datetime import datetime
from time import sleep
from utils.components.dater import Dater
from utils.connectors.opsgenie_api_connector import OpsGenieAPIConnector
from utils.connectors.database_connector import DatabaseConnector


class Loggerv3(logging.Logger):

    def __init__(self, name, file_location, local_mode=False, level=logging.DEBUG):
        """
        Creates a logging object that prints to terminal, writes json-formatted logs to dir, writes logs to redshift, and calls Opsgenie API when errors arise.

        :param name: The name of the file using the logger class instance, formatted as <dir>.<filename> without the extension
        :param file_location: The path where the log files will be written to
        :param local_mode: Boolean. If enabled, it will disable alerting to Opsgenie
        :param level: logging level. Defaults to DEBUG

        """
        super(Loggerv3, self).__init__(name)
        self.dater = Dater()
        self.alerter = OpsGenieAPIConnector(file_location)
        self.today = self.dater.format_date(self.dater.get_today())
        self.name = name
        self.filename = os.path.basename(self.name)
        self.file_location = file_location
        self.local_mode = local_mode
        self.log_level = level
        self.alert = False if self.local_mode is True else True
        self.directory = ''.join([self.file_location, 'logs'])
        self.full_logger_path = None
        self.fire_uncaught_exception = True
        self.create_full_logger_path()
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.log_level)
        self.stream_format = '%(asctime)-15s %(levelname)-8s %(message)s'
        self.stream_date_format = '%Y-%m-%d %H:%M:%S'
        self.logStreamHandler = None
        self.logFileHandler = None
        self.assign_handlers()
        self.process_handler = config.process_handler
        self.MAX_DETAILS_LENGTH = 1000
        self.run_start_time = None
        self.run_end_time = None
        self.write_logs_to_table = True
        self.connector = DatabaseConnector(file_location)
        self.schema = 'data_monitoring_tools'
        self.table_name = 'run_logs'


    def create_full_logger_path(self):
        """If directory doesn't exist, create it"""
        logger_path = '/'.join([self.directory, self.today])
        if not os.path.isdir(logger_path):
            os.mkdir(logger_path)
        self.filename = ''.join([self.filename.split('.py')[0], '.json'])
        self.full_logger_path = '/'.join([logger_path, self.filename])


    def assign_handlers(self):
        if len(self.logger.handlers) == 0:
            self.create_stream_handler(self.logger)
            self.create_file_handler(self.full_logger_path, self.logger)


    def create_stream_handler(self, logger):
        self.logStreamHandler = logging.StreamHandler()
        formatter = logging.Formatter(fmt=self.stream_format, datefmt=self.stream_date_format)
        self.logStreamHandler.setFormatter(formatter)
        self.logStreamHandler.setLevel(self.log_level)
        logger.addHandler(self.logStreamHandler)


    def create_file_handler(self, path, logger):
        self.logFileHandler = logging.FileHandler(path, mode='a')
        json_formatter = json_log_formatter.JSONFormatter()
        self.logFileHandler.setFormatter(json_formatter)
        self.logFileHandler.setLevel(self.log_level)
        logger.addHandler(self.logFileHandler)


    def kickoff_logger(self):
        if self.run_start_time is None:
            self.run_start_time = datetime.now()
        self.assign_handlers()


    def start(self, msg):
        status = 'START'
        self.kickoff_logger()
        self.logger.info(msg, extra={'level': status, 'process_handler': self.process_handler})


    def success(self, msg):
        status = 'SUCCESS'
        sleep(5)
        self.write_logs_to_redshift(status)
        self.logger.info(msg, extra={'level': status, 'process_handler': self.process_handler})
        self.close()


    def debug(self, msg):
        self.kickoff_logger()
        self.logger.debug(msg, extra={'level': logging.getLevelName(logging.DEBUG), 'process_handler': self.process_handler})


    def info(self, msg):
        self.kickoff_logger()
        self.logger.info(msg, extra={'level': logging.getLevelName(logging.INFO), 'process_handler': self.process_handler})


    def warning(self, msg):
        self.kickoff_logger()
        self.logger.warning(msg, extra={'level': logging.getLevelName(logging.WARNING), 'process_handler': self.process_handler})


    def exception(self, msg, terminate=False):
        self.kickoff_logger()
        self.write_logs_to_redshift(logging.getLevelName(logging.ERROR))

        self.logger.exception(msg, extra={'level': logging.getLevelName(logging.ERROR), 'process_handler': self.process_handler})
        self.send_alert()
        if terminate is True:
            self.fire_uncaught_exception = False
            raise



    def error(self, msg, terminate=False):
        self.kickoff_logger()
        self.write_logs_to_redshift(logging.getLevelName(logging.ERROR))

        self.logger.error(msg, extra={'level': logging.getLevelName(logging.ERROR), 'process_handler': self.process_handler})
        self.send_alert()
        if terminate is True:
            self.fire_uncaught_exception = False
            raise


    def critical(self, msg, terminate=False):
        self.kickoff_logger()
        self.write_logs_to_redshift(logging.getLevelName(logging.CRITICAL))

        self.logger.critical(msg, extra={'level': logging.getLevelName(logging.CRITICAL), 'process_handler': self.process_handler})
        self.send_alert()
        if terminate is True:
            self.fire_uncaught_exception = False
            raise


    def inline_info(self, msg):
        self.kickoff_logger()
        inline_logger = logging.getLogger(f'{self.name} - Inline')
        inline_logger.setLevel(self.log_level)
        self.create_file_handler(self.full_logger_path, inline_logger)
        inline_logger.info(msg, extra={'level': logging.getLevelName(logging.INFO)})
        inline_logger.handlers = []


    def handle_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        self.kickoff_logger()
        self.write_logs_to_redshift(logging.getLevelName(logging.ERROR))

        if self.fire_uncaught_exception is True:
            self.logger.error('Uncaught exception',
                              extra={'level': logging.getLevelName(logging.ERROR),
                                     'exc_type': str(exc_type),
                                     'exc_traceback': traceback.format_tb(exc_traceback)[-1][:self.MAX_DETAILS_LENGTH],
                                     'exc_value': str(exc_value)[:self.MAX_DETAILS_LENGTH],
                                     'process_handler': self.process_handler
                                     }
                              )
            self.send_alert()


    def close(self):
        self.logger.handlers.clear()


    def disable_alerting(self):
        self.alert = False


    def disable_writing(self):
        self.write_logs_to_table = False


    def write_process_handler_helper(self):
        if self.process_handler is not None:
            return self.process_handler
        elif self.local_mode is True:
            return 'local'
        else:
            return 'repl'


    def write_logs_to_redshift(self, final_status):
        if self.write_logs_to_table is True:
            self.run_end_time = datetime.now()
            data = {
                'job_start': self.run_start_time,
                'filename': self.name,
                'final_status': final_status,
                'run_time_sec': round((self.run_end_time - self.run_start_time).total_seconds(), 0),
                'process_handler': self.write_process_handler_helper()
            }
            dataframe = pd.DataFrame([data])
            self.connector.write_to_sql(dataframe, self.table_name, self.connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
            self.connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


    def send_alert(self):
        if self.alert:
            data = [json.loads(line) for line in open(self.full_logger_path, 'r')]
            log_line = data[-1]
            self.alerter.alert(log_line, self.name)
        else:
            data = [json.loads(line) for line in open(self.full_logger_path, 'r')]
            log_line = data[-1]
            print(log_line)
