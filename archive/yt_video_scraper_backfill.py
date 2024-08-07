import config
import os
import pandas as pd
import random
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from time import sleep
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3
from utils.components.dater import Dater


class YTVideoScraperBackfill:

    def __init__(self, start_dates, columns_to_update):
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.dater = Dater()
        self.table_name = 'yt_studio_weekly_metrics'
        self.prod_schema = 'warehouse'
        self.staging_schema = 'staging'
        self.start_dates = start_dates
        self.columns_to_update = columns_to_update
        self.playwright = None
        self.browser = None
        self.page = None
        self.channel_videos = {}
        self.dim_videos = []
        self.video_metrics = []
        self.cleaned_metrics = []
        self.final_dataframe = None
        self.url = 'https://www.youtube.com/'
        self.MIN_SLEEP = 4
        self.MAX_SLEEP = 7
        self.MAX_RETRIES = 4


    def period_helper(self, start_period):
        end_period = (datetime.strptime(start_period, '%Y-%m-%d') + timedelta(7)).strftime('%Y-%m-%d')  # This is the last day of the week PLUS 1 Day
        run_date = (datetime.strptime(start_period, '%Y-%m-%d') + timedelta(10)).strftime('%Y-%m-%d')  # This is the Wednesday following the previous week
        return {
            'start_period': start_period,
            'end_period': end_period,
            'run_date': run_date
        }


    def get_video_ids(self):
        self.loggerv3.info('Getting video IDs by channel and time periods')
        for start_period in self.start_dates:
            date_period_dict = self.period_helper(start_period)
            query = f"""
            SELECT
                dim.channel_title,
                dim.video_id,
                dim.video_title,
                dim.published_at,
                dim.duration_in_seconds
            FROM warehouse.dim_yt_video_v2 dim
            WHERE dim.channel_title in
                ('Achievement Hunter', 'Funhaus', 'Rooster Teeth', 'Rooster Teeth Animation', 'LetsPlay',
                  'DEATH BATTLE!', 'Red Web', 'Face Jam', 'F**KFACE', 'Squad Team Force', 'Inside Gaming',
                  'Annual Pass', 'Funhaus Too', 'Black Box Down', 'Tales from the Stinky Dragon')
                AND dim.published_at >= '{date_period_dict["start_period"]}' and dim.published_at < '{date_period_dict["end_period"]}';
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


    def request_url_helper(self, channel, video, url_sub_dir, locator, time_period, retries):
        response = self.page.goto(f'https://studio.youtube.com/video/{video}/analytics/{url_sub_dir}/{time_period}')
        if response.status == 200:
            sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
            if len(self.page.locator(locator).all_inner_texts()) > 0:
                return True
            else:
                if retries == self.MAX_RETRIES:
                    self.loggerv3.info(f'Video {video} for {channel} returned a {response.status}')
                    return False
                else:
                    return self.request_url_helper(channel, video, url_sub_dir, locator, time_period, retries + 1)

        else:
            if retries == self.MAX_RETRIES:
                self.loggerv3.info(f'Video {video} for {channel} returned a {response.status}')
                return False
            else:
                return self.request_url_helper(channel, video, url_sub_dir, locator, time_period, retries+1)


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
                sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
                # Set Video Dim Metrics
                metrics = video_data

                # Set Time Period
                unix_start = self.dater.convert_to_unix_timestamp(video_data['start_period'])
                unix_end = self.dater.convert_to_unix_timestamp(video_data['run_date'])
                time_period = f'period-{unix_start},{unix_end}'

                # Go to Audience Tab
                audience_results = self.request_url_helper(channel, video_data['video_id'], 'tab-build_audience', '#metric-total', time_period, retries=1)
                if audience_results is True:
                    audience = self.page.locator('#metric-total').all_inner_texts()
                    metrics['returning_viewers'] = audience[0]
                    metrics['unique_viewers'] = audience[1]
                    metrics['subscribers'] = audience[2]
                else:
                    continue

                # Go to Engagement Tab
                engagement_results = self.request_url_helper(channel, video_data['video_id'], 'tab-interest_viewers', '#metric-total', time_period, retries=1)
                if engagement_results is True:
                    engagements = self.page.locator('#metric-total').all_inner_texts()
                    metrics['avg_watch_time'] = engagements[1]

                # Go to Reach Tab
                engagement_results = self.request_url_helper(channel, video_data['video_id'], 'tab-reach_viewers', '#impressions-value', time_period, retries=1)
                if engagement_results is True:
                    impressions = self.page.locator('#impressions-value').inner_text()
                    metrics['impressions'] = impressions
                    ctr = self.page.locator('#ctr-title').inner_text()
                    metrics['ctr'] = ctr
                    watch_time_from_imps = self.page.locator('#wt-value').inner_text()
                    metrics['watch_time'] = watch_time_from_imps
                else:
                    continue

                # Go to Overview Tab
                overview_response = self.page.goto(f'https://studio.youtube.com/video/{video_data["video_id"]}/analytics/tab-overview/{time_period}')
                sleep(self.MAX_SLEEP)
                if overview_response.status == 200:
                    concurrents = self.page.locator('#display-value').all_inner_texts()
                    if len(concurrents) == 3:
                        metrics['chat_messages'] = concurrents[0]
                        metrics['peak_concurrents'] = concurrents[1]
                        metrics['avg_concurrents'] = concurrents[2]
                    elif len(concurrents) == 2:
                        metrics['chat_messages'] = "0"
                        metrics['peak_concurrents'] = concurrents[0]
                        metrics['avg_concurrents'] = concurrents[1]
                    else:
                        metrics['chat_messages'] = "0"
                        metrics['peak_concurrents'] = "0"
                        metrics['avg_concurrents'] = "0"
                else:
                    continue

                # Go to Details Tab
                details_response = self.page.goto(f'https://studio.youtube.com/video/{video_data["video_id"]}/edit')
                sleep(self.MAX_SLEEP)
                if details_response.status == 200:
                    visibility_tag = self.page.locator('#visibility-text').inner_text()
                    metrics['visibility_tag'] = visibility_tag
                else:
                    continue

                # Append scraped metrics
                self.video_metrics.append(metrics)


    def convert_metrics_helper(self, metric):
        if '—' == metric:
            return 0.0
        elif 'K' in metric:
            return float(metric.replace('K', '').replace(',', '')) * 1000
        elif 'M' in metric:
            return float(metric.replace('M', '').replace(',', '')) * 1000000
        else:
            return float(metric.replace(',', ''))


    def convert_time_helper(self, metric):
        if metric == '—':
            return 0
        else:
            minutes_in_seconds = int(metric.split(':')[0]) * 60
            seconds = int(metric.split(':')[1])
            return minutes_in_seconds + seconds


    def convert_subscribers_helper(self, metric):
        if '—' == metric:
            return 0
        elif 'K' in metric:
            if '—' in metric:
                return -1 * int(float(metric.replace('—', '').replace('K', '').replace(',', '')) * 1000)
            else:
                return int(float(metric.replace('+', '').replace('K', '').replace(',', '')) * 1000)
        else:
            if '—' in metric:
                return -1 * int(metric.replace('—', '').replace(',', ''))
            else:
                return int(metric.replace('+', '').replace(',', ''))


    def clean_raw_data(self):
        self.loggerv3.info('Cleaning raw data')
        for video in self.video_metrics:
            d = {
                'channel_title': video['channel_title'],
                'video_title': video['video_title'],
                'video_id': video['video_id'],
                'published_at': video['published_at'],
                'duration_in_seconds': video['duration_in_seconds'],
                'start_week': video['start_period'],
                'run_date': video['run_date'],
                'returning_viewers': self.convert_metrics_helper(video['returning_viewers']),
                'unique_viewers': self.convert_metrics_helper(video['unique_viewers']),
                'subscribers': self.convert_subscribers_helper(video['subscribers']),
                'impressions': self.convert_metrics_helper(video['impressions']),
                'ctr': float(video['ctr'].replace('% click-through rate', '')) / 100,
                'watch_time_from_imps_hr': self.convert_metrics_helper(video['watch_time']),
                'avg_watch_time_sec': self.convert_time_helper(video['avg_watch_time']),
                'peak_concurrents': self.convert_metrics_helper(video['peak_concurrents']),
                'avg_concurrents': self.convert_metrics_helper(video['avg_concurrents']),
                'chat_messages': self.convert_metrics_helper(video['chat_messages']),
                'visibility_tag': video['visibility_tag'],
                'avg_completion_rate': self.convert_time_helper(video['avg_watch_time']) / video['duration_in_seconds']
            }

            if d['unique_viewers'] > 0:
                d['new_viewer_pct'] = (d['unique_viewers'] - d['returning_viewers']) / d['unique_viewers']
            else:
                d['new_viewer_pct'] = 0.0

            self.cleaned_metrics.append(d)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.cleaned_metrics)

        self.final_dataframe['score'] = ((self.final_dataframe['unique_viewers'] * self.final_dataframe['avg_completion_rate']) / 100).round(2)

        self.final_dataframe = self.final_dataframe.round(4)

        convert = {'returning_viewers': int, 'unique_viewers': int, 'subscribers': int, 'impressions': int,
                   'avg_watch_time_sec': int, 'peak_concurrents': int, 'avg_concurrents': int, 'chat_messages': int}

        self.final_dataframe = self.final_dataframe.astype(convert)

        self.final_dataframe['run_date'] = pd.to_datetime(self.final_dataframe['run_date'], format='%Y-%m-%d')

        self.final_dataframe = self.final_dataframe[
            ['channel_title', 'video_title', 'video_id', 'published_at', 'duration_in_seconds',
             'unique_viewers', 'returning_viewers', 'new_viewer_pct', 'impressions', 'ctr', 'subscribers',
             'watch_time_from_imps_hr', 'avg_watch_time_sec', 'avg_completion_rate', 'score', 'run_date',
             'peak_concurrents', 'avg_concurrents', 'start_week', 'chat_messages', 'visibility_tag']]


    def set_query(self):
        query = ""
        for i in range(len(self.columns_to_update)):
            query += f" {self.columns_to_update[i]} = staging.{self.columns_to_update[i]}"
            if i < len(self.columns_to_update) - 1:
                query += ","
        return query


    def delete_where_query(self):
        query = f"prod.video_id = stage_{self.table_name}.video_id"
        for i in range(len(self.columns_to_update)):
            query += f" AND prod.{self.columns_to_update[i]} = stage_{self.table_name}.{self.columns_to_update[i]}"
        return query


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(self.final_dataframe, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
               BEGIN TRANSACTION;

                   UPDATE {self.prod_schema}.{self.table_name}
                   SET 
                       {self.set_query()}
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.video_id = prod.video_id;

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                       {self.delete_where_query()};

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)


    def close_browser(self):
        self.loggerv3.info('Closing browser')
        self.browser.close()


    def execute(self):
        self.loggerv3.start(f"Running YT Video Metrics Scraper Backfill")
        self.get_video_ids()
        self.start_browser()
        self.manually_sign_in()
        self.get_video_metrics()
        self.clean_raw_data()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.close_browser()
        self.loggerv3.success("All Processing Complete!")
