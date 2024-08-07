import config
import json
import json_log_formatter
import logging
import os
import traceback
from datetime import datetime
from utils.components.dater import Dater
from utils.connectors.opsgenie_api_connector import OpsGenieAPIConnector


class Loggerv2(logging.Logger):

    def __init__(self, name, file_location, local_mode, level=logging.DEBUG, alert=True):
        super(Loggerv2, self).__init__(name)
        self.dater = Dater()
        self.alerter = OpsGenieAPIConnector(file_location)
        self.today = self.dater.format_date(self.dater.get_today())
        self.name = name
        self.filename = os.path.basename(self.name)
        self.file_location = file_location
        self.local_mode = local_mode
        self.log_level = level
        self.alert = alert
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


    def create_full_logger_path(self):
        """If directory doesn't exist, create it"""
        logger_path = '/'.join([self.directory, self.today])
        if not os.path.isdir(logger_path):
            os.mkdir(logger_path)
        self.filename = ''.join([self.filename.split('.py')[0], '.json'])
        self.full_logger_path = '/'.join([logger_path, self.filename])


    def assign_handlers(self):
        if self.local_mode:
            self.create_stream_handler(self.logger)
            self.create_file_handler(self.full_logger_path, self.logger)
        else:
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


    def success(self, msg):
        self.logger.info(msg, extra={'level': 'SUCCESS', 'process_handler': self.process_handler})
        self.close()


    def debug(self, msg):
        self.logger.debug(msg, extra={'level': logging.getLevelName(logging.DEBUG), 'process_handler': self.process_handler})


    def info(self, msg):
        self.logger.info(msg, extra={'level': logging.getLevelName(logging.INFO), 'process_handler': self.process_handler})


    def warning(self, msg):
        self.logger.warning(msg, extra={'level': logging.getLevelName(logging.WARNING), 'process_handler': self.process_handler})


    def exception(self, msg, terminate=False):
        self.logger.exception(msg, extra={'level': logging.getLevelName(logging.ERROR), 'process_handler': self.process_handler})
        self.send_alert()
        if terminate is True:
            self.fire_uncaught_exception = False
            raise


    def error(self, msg, terminate=False):
        self.logger.error(msg, extra={'level': logging.getLevelName(logging.ERROR), 'process_handler': self.process_handler})
        self.send_alert()
        if terminate is True:
            self.fire_uncaught_exception = False
            raise


    def critical(self, msg, terminate=False):
        self.logger.critical(msg, extra={'level': logging.getLevelName(logging.CRITICAL), 'process_handler': self.process_handler})
        self.send_alert()
        if terminate is True:
            self.fire_uncaught_exception = False
            raise


    def inline_info(self, msg):
        inline_logger = logging.getLogger(f'{self.name} - Inline')
        inline_logger.setLevel(self.log_level)
        self.create_file_handler(self.full_logger_path, inline_logger)
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {logging.getLevelName(logging.INFO)}{" " * 5}{msg}{" " * 10}', end="\r")
        inline_logger.info(msg, extra={'level': logging.getLevelName(logging.INFO)})
        inline_logger.handlers = []


    def handle_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        if self.fire_uncaught_exception is True:
            if self.local_mode:
                print(exc_value)
                print(traceback.format_tb(exc_traceback)[-1])
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


    def send_alert(self):
        if self.alert:
            data = [json.loads(line) for line in open(self.full_logger_path, 'r')]
            log_line = data[-1]
            self.alerter.alert(log_line, self.name)
        else:
            data = [json.loads(line) for line in open(self.full_logger_path, 'r')]
            log_line = data[-1]
            print(log_line)
