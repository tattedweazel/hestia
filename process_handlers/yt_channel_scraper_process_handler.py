import config
from base.server_process_handler import ServerProcessHandler
from jobs.log_file_handler_job import LogFileHandlerJob
from tools.modules.yt_scraper.channel_scraper.yt_weekly_channel_scraper import YTWeeklyChannelScraper
from utils.components.loggerv3 import Loggerv3


class YTChannelScraperProcessHandler(ServerProcessHandler):

    def __init__(self, local_mode):
        if local_mode:
            self.file_location = ''
        else:
            self.file_location = '/home/centos/processes/rt-data-hestia/'
        config.file_location = self.file_location
        config.local_mode = local_mode
        config.process_handler = __name__
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.loggerv3.disable_alerting()

    def run_jobs(self):
        self.loggerv3.start('Starting YT Channel Scraper Process Handler')

        # start_dates = backfill_by_week(latest_date='2023-01-23', earliest_date='2022-01-03', start_dow=0)
        start_dates = ['2024-02-11']
        backfill = False
        yt = YTWeeklyChannelScraper(start_dates=start_dates,  backfill=backfill)
        yt.execute()

        lfhj = LogFileHandlerJob()
        lfhj.execute()

        self.loggerv3.success("All Processing Complete!")
