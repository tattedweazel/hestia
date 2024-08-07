import config
import os
import pandas as pd
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from tools.modules.yt_scraper.vid_scraper.advanced_metrics_traffic_source_scraper import AdvancedMetricsTrafficSourceScraper
from tools.modules.yt_scraper.vid_scraper.advanced_metrics_content_scraper import AdvancedMetricsContentScraper
from tools.modules.yt_scraper.vid_scraper.overview_scraper import OverviewScraper
from tools.modules.yt_scraper.vid_scraper.details_scraper import DetailsScraper
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3
from utils.components.dater import Dater


class YTVideoScraperBackfill:

    def __init__(self, start_dates):
        # See `write_results_to_redshift` method for tables and schemas
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.dater = Dater()
        self.start_dates = start_dates
        self.playwright = None
        self.browser = None
        self.page = None
        self.channel_videos = {}
        self.dim_videos = []
        self.url = 'https://www.youtube.com/'

        self.content_dataframe = None
        self.overview_dataframe = None
        self.details_dataframe = None
        self.traffic_source_dataframe = None
        self.final_dataframe = None


    def period_helper(self, start_period):
        end_period = (datetime.strptime(start_period, '%Y-%m-%d') + timedelta(7)).strftime('%Y-%m-%d')  # This is the last day of the week PLUS 1 Day
        run_date = (datetime.strptime(start_period, '%Y-%m-%d') + timedelta(10)).strftime('%Y-%m-%d')  # This is the Wednesday following the previous week
        return {
            'start_period': start_period,
            'end_period': end_period,
            'run_date': run_date
        }


    def get_video_ids(self):
        self.loggerv3.info('Getting video IDs by channel')
        for start_period in self.start_dates:
            date_period_dict = self.period_helper(start_period)
            query = f"""
            SELECT 
                channel_title,
                video_id,
                video_title,
                published_at,
                duration_in_seconds
            FROM warehouse.dim_yt_video_v2
            WHERE channel_title in
            ('Achievement Hunter', 'Rooster Teeth', 'Rooster Teeth Animation', 'Funhaus',
    	     'LetsPlay', 'DEATH BATTLE!', 'Red Web', 'Face Jam', 'F**KFACE', 'Inside Gaming', 
    	     'Annual Pass', 'Tales from the Stinky Dragon')
              AND published_at >= '{date_period_dict["start_period"]}' and published_at < '{date_period_dict["end_period"]}';
            """
            results = self.db_connector.read_redshift(query)
            for result in results:
                data = {
                    'channel_title': result[0],
                    'video_id': result[1],
                    'video_title': result[2],
                    'published_at': result[3],
                    'duration_in_seconds': result[4],
                    'start_period': date_period_dict['start_period'],
                    'end_period': date_period_dict['end_period'],
                    'run_date': date_period_dict['run_date']
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
        while True:
            confirm = input("Did you sign in (y/n)? ")
            if confirm != 'y':
                print("Need to enter a 'y' to confirm")
                continue
            else:
                break


    def set_time_period(self, start_period, run_date):
        unix_start = self.dater.convert_to_unix_timestamp(start_period)
        unix_end = self.dater.convert_to_unix_timestamp(run_date)

        return {
            'time_period': 'period-default',
            'advanced_time_period': f'{unix_start},{unix_end}'
        }


    def get_video_metrics(self):
        self.loggerv3.info('Getting video metrics')
        for channel, videos in self.channel_videos.items():

            while True:
                os.system(f"say Action Required. Please navigate to the {channel} channel")
                confirm = input(f'Are you in the {channel} channel within Creator Studio? ')
                if confirm != 'y':
                    print("Need to enter a 'y' to confirm")
                    continue
                else:
                    break

            for video_data in videos:

                start_period = video_data['start_period']
                run_date = video_data['run_date']
                time_period_dict = self.set_time_period(start_period, run_date)
                time_period = time_period_dict['time_period']
                advanced_time_period = time_period_dict['advanced_time_period']

                self.loggerv3.info(f'Video ID {video_data["video_id"]} for {start_period}')

                # Content Scraper
                content_scraper = AdvancedMetricsContentScraper(
                    page=self.page,
                    video_id=video_data['video_id'],
                    video_duration=video_data['duration_in_seconds'],
                    time_period=time_period,
                    advanced_time_period=advanced_time_period
                )
                content_scraper.execute()
                content_scraper.dataframe['start_week'] = start_period
                content_scraper.dataframe['run_date'] = datetime.strptime(run_date, '%Y-%m-%d')
                if self.content_dataframe is None:
                    self.content_dataframe = content_scraper.dataframe
                else:
                    self.content_dataframe = pd.concat([self.content_dataframe, content_scraper.dataframe])

                # Overview Scraper
                overview_scraper = OverviewScraper(
                    page=self.page,
                    video_id=video_data['video_id'],
                    time_period=time_period
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

                # # Traffic Source Metrics Scraper
                # traffic_source_scraper = AdvancedMetricsTrafficSourceScraper(
                #     page=self.page,
                #     video_id=video_data['video_id'],
                #     video_duration=video_data['duration_in_seconds'],
                #     time_period=time_period,
                #     advanced_time_period=advanced_time_period
                # )
                # traffic_source_scraper.execute()
                # if traffic_source_scraper.dataframe is not None:
                #     traffic_source_scraper.dataframe['start_week'] = start_period
                #     traffic_source_scraper.dataframe['run_date'] = datetime.strptime(run_date, '%Y-%m-%d')
                #     if self.traffic_source_dataframe is None:
                #         self.traffic_source_dataframe = traffic_source_scraper.dataframe
                #     else:
                #         self.traffic_source_dataframe = pd.concat([self.traffic_source_dataframe, traffic_source_scraper.dataframe])



    def build_dataframes(self):
        self.loggerv3.info('Building final dataframes')
        # Join Content -> Overview
        self.final_dataframe = self.content_dataframe.merge(self.overview_dataframe, how='left', on='video_id')

        # Join Content -> Details
        self.final_dataframe = self.final_dataframe.merge(self.details_dataframe, how='left', on='video_id')

        # Join Content -> Dim
        self.final_dataframe = self.final_dataframe.drop(columns=['start_period', 'end_period', 'run_date'])
        self.final_dataframe = self.final_dataframe.merge(pd.DataFrame(self.dim_videos), how='left', on='video_id')


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing studio weekly metrics to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, 'yt_studio_weekly_metrics', self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions('yt_studio_weekly_metrics', 'warehouse')

        # self.loggerv3.info("Writing traffic source metrics to Redshift")
        # self.db_connector.write_to_sql(self.traffic_source_dataframe, 'yt_studio_traffic_source_metrics', self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        # self.db_connector.update_redshift_table_permissions('yt_studio_traffic_source_metrics', 'warehouse')


    def close_browser(self):
        self.loggerv3.info('Closing browser')
        self.browser.close()


    def execute(self):
        self.loggerv3.start(f"Running YT Video Metrics Scraper Backfill")
        self.get_video_ids()
        self.start_browser()
        self.manually_sign_in()
        self.get_video_metrics()
        self.build_dataframes()
        self.write_results_to_redshift()
        self.close_browser()
        self.loggerv3.success("All Processing Complete!")
