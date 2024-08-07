import config
import pandas as pd
import random
from time import sleep
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from tools.modules.yt_scraper.vid_scraper.advanced_metrics_content_scraper import AdvancedMetricsContentScraper
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3
from utils.components.dater import Dater


class YTVideoScraper:

    def __init__(self, run_date):
        """
        Scrapes Day 1 & Day 7 for a given number of videos for a single channel.
        Note this can be edited.
        """
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.dater = Dater()
        self.run_date = run_date
        self.VIDEOS_LOOKBACK = 120  # Days
        self.MIN_SLEEP = 6
        self.playwright = None
        self.browser = None
        self.page = None
        self.videos = []
        self.dim_videos = []
        self.url = 'https://www.youtube.com/'

        self.content_dataframe = None
        self.final_dataframe = None

        self.loggerv3.disable_alerting()


    def load_video_ids(self):
        self.loggerv3.info('Getting video IDs by channel')
        query = f"""
        SELECT
            channel_title,
            video_id,
            video_title,
            published_at,
            duration_in_seconds
        FROM warehouse.dim_yt_video_v2
        WHERE channel_title = 'Funhaus'
          AND published_at >= '{self.run_date}' - {self.VIDEOS_LOOKBACK} - 7
          AND published_at < '{self.run_date}' - 7
          AND duration_in_seconds > 90
          AND duration_in_seconds < 1800
          AND video_title not like '%%Members Only%%'
        GROUP BY 1, 2, 3, 4, 5;
    	"""
        results = self.db_connector.read_redshift(query)
        for result in results:
            data = {
                'channel_title': result[0],
                'video_id': result[1],
                'video_title': result[2],
                'published_at': result[3],
                'duration_in_seconds': result[4]
            }
            self.videos.append(data)
            self.dim_videos.append(data)


    def start_browser(self):
        self.loggerv3.info('Starting browser')
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.page = self.browser.new_page()
        self.page.goto(self.url)


    def manually_sign_in(self):
        self.loggerv3.info('Manually fill in email, password, and 2-auth')
        while True:
            confirm = input("Did you sign in (y/n), 2-factor auth, and choose a channel (any channel)? ")
            if confirm != 'y':
                print("Need to enter a 'y' to confirm")
                continue
            else:
                break


    def set_time_period(self, start_period, days):
        start_period_str = str(start_period)[:10]

        unix_start = self.dater.convert_to_unix_timestamp(start_period_str)
        end_period_dt = datetime.strptime(start_period_str, '%Y-%m-%d') + timedelta(days)
        end_period = datetime.strftime(end_period_dt, '%Y-%m-%d')
        unix_end = self.dater.convert_to_unix_timestamp(end_period)
        return {
            'time_period': f'period-{unix_start},{unix_end}',
            'advanced_time_period': f'{unix_start},{unix_end}'
        }


    def get_video_metrics(self):
        self.loggerv3.info('Getting video metrics')

        for video_data in self.videos:
            print(f" {video_data['video_id']}")

            for day in (1, 7):
                yt_periods = self.set_time_period(video_data['published_at'], day)
                time_period = yt_periods['time_period']
                advanced_time_period = yt_periods['advanced_time_period']

                # Content Scraper
                content_scraper = AdvancedMetricsContentScraper(
                    page=self.page,
                    video_id=video_data['video_id'],
                    video_duration=video_data['duration_in_seconds'],
                    time_period=time_period,
                    advanced_time_period=advanced_time_period
                )
                content_scraper.execute()
                content_scraper.dataframe['day'] = day
                if self.content_dataframe is None:
                    self.content_dataframe = content_scraper.dataframe
                else:
                    self.content_dataframe = pd.concat([self.content_dataframe, content_scraper.dataframe])

                sleep(self.MIN_SLEEP)


    def build_dataframes(self):
        self.loggerv3.info('Building final dataframes')
        # Join Content -> Dim
        self.final_dataframe = self.content_dataframe.merge(pd.DataFrame(self.dim_videos), how='left', on='video_id')
        self.final_dataframe['weekday'] = self.final_dataframe['published_at'].dt.dayofweek


    def write_results_to_csv(self):
        self.loggerv3.info("Writing studio weekly metrics to Redshift")
        self.final_dataframe.to_csv('funhaus_output2.csv', index=False)


    def close_browser(self):
        self.loggerv3.info('Closing browser')
        self.browser.close()


    def execute(self):
        self.loggerv3.start(f"Running YT Custom Vid Scraper 1 for {self.run_date}")
        self.load_video_ids()
        self.start_browser()
        self.manually_sign_in()
        self.get_video_metrics()
        self.build_dataframes()
        self.write_results_to_csv()
        self.close_browser()
        self.loggerv3.success("All Processing Complete!")
