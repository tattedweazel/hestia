import config
import pandas as pd
import random
import utils.components.yt_variables as sv
import tools.modules.yt_scraper.base.yt_scraper_helpers as sh
from time import sleep
from datetime import datetime
from playwright.sync_api import sync_playwright
from tools.modules.yt_scraper.vid_scraper.advanced_metrics_content_scraper import AdvancedMetricsContentScraper
from tools.modules.yt_scraper.vid_scraper.overview_scraper import OverviewScraper
from tools.modules.yt_scraper.vid_scraper.details_scraper import DetailsScraper
from tools.modules.yt_scraper.vid_scraper.content_type_scraper import ContentTypeScraper
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3
from utils.components.dater import Dater


class YTVideoScraper:

    def __init__(self, start_period, end_period, run_date, backfill):
        # See `write_results_to_redshift` method for tables and schemas
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.table_name = 'yt_studio_weekly_metrics'
        self.schema = 'warehouse'
        self.dater = Dater()
        self.start_period = start_period
        self.end_period = end_period
        self.run_date = run_date
        self.backfill = backfill  # True or False
        self.time_period = 'period-4_weeks'
        self.advanced_time_period = '4_weeks'
        self.MIN_SLEEP = 6
        self.MAX_SLEEP = 8
        self.playwright = None
        self.browser = None
        self.page = None
        self.channels = {}
        self.channel_videos = {}
        self.dim_videos = []
        self.url = 'https://www.youtube.com/'
        self.channel_positions = sv.channel_positions

        self.content_dataframe = None
        self.overview_dataframe = None
        self.details_dataframe = None
        self.content_type_dataframe = None
        self.final_dataframe = None


    def load_channel_ids(self):
        self.loggerv3.info('Getting channels')
        query = f"""
        SELECT
            distinct channel_title, channel_id
    	FROM warehouse.dim_yt_video_v2
    	WHERE channel_title in {sv.channels}
    	  AND published_at >= '{self.start_period}' and published_at < '{self.end_period}';
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.channels[result[0]] = result[1]


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
    	WHERE channel_title in {sv.channels}
    	  AND published_at >= '{self.start_period}' and published_at < '{self.end_period}'
    	GROUP BY 1, 2, 3, 4, 5;
    	"""
        results = self.db_connector.read_redshift(query)
        for result in results:
            data = {
                'channel_title': result[0],
                'video_id': result[1],
                'video_title': result[2],
                'published_at': result[3],
                'duration_in_seconds': result[4],
            }
            if result[0] not in self.channel_videos:
                self.channel_videos[result[0]] = [data]
            else:
                self.channel_videos[result[0]].append(data)

            self.dim_videos.append(data)


    def start_browser(self):
        self.loggerv3.info('Starting browser')
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.page = self.browser.new_page()
        self.page.goto(self.url)


    def manually_sign_in(self):
        self.loggerv3.info('Manually fill in email, password, and 2-auth')
        sh.sign_in_confirm_helper()


    def set_time_period(self):
        self.loggerv3.info('Setting time period and unix times')
        if self.backfill is True:
            unix_start = self.dater.convert_to_unix_timestamp(self.start_period)
            unix_end = self.dater.convert_to_unix_timestamp(self.run_date)
            self.time_period = 'period-default'
            self.advanced_time_period = f'{unix_start},{unix_end}'


    def get_video_metrics(self):
        self.loggerv3.info('Getting video metrics')
        # Navigate to Creator Studio
        self.page.locator('#avatar-btn > yt-img-shadow').click()
        self.page.get_by_text('YouTube Studio').click()
        for channel_title, channel_id in self.channels.items():
            print(channel_title)
            # Navigate to Creator Studio Channel
            sh.change_channel_helper(self.page, self.channel_positions, channel_title)

            videos = self.channel_videos[channel_title]
            sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))

            for video_data in videos:
                print(f" {video_data['video_id']}")

                # Content Scraper
                content_scraper = AdvancedMetricsContentScraper(
                    page=self.page,
                    video_id=video_data['video_id'],
                    video_duration=video_data['duration_in_seconds'],
                    time_period=self.time_period,
                    advanced_time_period=self.advanced_time_period
                )
                content_scraper.execute()
                if content_scraper.dataframe is None:
                    self.loggerv3.info(f"Video does not exist anymore. Moving on...")
                    continue
                elif self.content_dataframe is None:
                    self.content_dataframe = content_scraper.dataframe
                else:
                    self.content_dataframe = pd.concat([self.content_dataframe, content_scraper.dataframe])

                # Overview Scraper
                overview_scraper = OverviewScraper(
                    page=self.page,
                    video_id=video_data['video_id'],
                    time_period=self.time_period
                )
                overview_scraper.execute()
                if self.overview_dataframe is None:
                    self.overview_dataframe = overview_scraper.dataframe
                else:
                    self.overview_dataframe = pd.concat([self.overview_dataframe, overview_scraper.dataframe])

                # Details Scraper
                details_scraper = DetailsScraper(
                    page=self.page,
                    video_id=video_data['video_id']
                )
                details_scraper.execute()
                if self.details_dataframe is None:
                    self.details_dataframe = details_scraper.dataframe
                else:
                    self.details_dataframe = pd.concat([self.details_dataframe, details_scraper.dataframe])

                # Content Type Scraper
                content_type_scraper = ContentTypeScraper(
                    page=self.page,
                    video_id=video_data['video_id']
                )
                content_type_scraper.execute()
                if self.content_type_dataframe is None:
                    self.content_type_dataframe = content_type_scraper.dataframe
                else:
                    self.content_type_dataframe = pd.concat([self.content_type_dataframe, content_type_scraper.dataframe])


                # Navigate Back to Creator Studio Home
                self.page.goto(f'https://studio.youtube.com/channel/{channel_id}')
                sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))


    def build_dataframes(self):
        self.loggerv3.info('Building final dataframes')
        # Join Content -> Overview
        self.final_dataframe = self.content_dataframe.merge(self.overview_dataframe, how='left', on='video_id')

        # Join Content -> Details
        self.final_dataframe = self.final_dataframe.merge(self.details_dataframe, how='left', on='video_id')

        # Join Content -> Dim
        self.final_dataframe = self.final_dataframe.merge(pd.DataFrame(self.dim_videos), how='left', on='video_id')

        # Join Content -> Content Type
        self.final_dataframe = self.final_dataframe.merge(pd.DataFrame(self.content_type_dataframe), how='left', on='video_id')

        # Set Final Values
        self.final_dataframe['run_date'] = datetime.strptime(self.run_date, '%Y-%m-%d')
        self.final_dataframe['start_week'] = self.start_period


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing studio weekly metrics to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def close_browser(self):
        self.loggerv3.info('Closing browser')
        self.browser.close()


    def execute(self):
        self.loggerv3.start(f"Running YT Video Metrics Scraper starting {self.start_period}")
        self.load_channel_ids()
        self.load_video_ids()
        self.start_browser()
        self.manually_sign_in()
        self.set_time_period()
        self.get_video_metrics()
        self.build_dataframes()
        self.write_results_to_redshift()
        self.close_browser()
        self.loggerv3.success("All Processing Complete!")
