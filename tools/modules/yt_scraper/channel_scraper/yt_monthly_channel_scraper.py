import pandas as pd
import config
import random
import utils.components.yt_variables as sv
import tools.modules.yt_scraper.base.yt_scraper_helpers as sh
from datetime import datetime
from dateutil import relativedelta
from playwright.sync_api import sync_playwright
from time import sleep
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3
from utils.components.dater import Dater


class YTMonthlyChannelScraper:

    def __init__(self, start_dates):
        """
        :param start_dates: An array of string dates in format of YYYY-MM-DD
        """
        self.table_name = 'yt_studio_monthly_channel_metrics'
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.dater = Dater()
        self.start_dates = start_dates
        self.schema = 'warehouse'
        self.channels = {}
        self.playwright = None
        self.browser = None
        self.page = None
        self.channel_metrics = []
        self.cleaned_metrics = []
        self.final_dataframe = None
        self.url = 'https://www.youtube.com/'
        self.MIN_SLEEP = 5
        self.MAX_SLEEP = 6
        self.channel_positions = sv.channel_positions


    def load_channel_ids(self):
        self.loggerv3.info('Loading channel IDs')
        query = f"""
        SELECT distinct channel_title, channel_id
        FROM warehouse.dim_yt_video_v2
        WHERE channel_title in {sv.channels};
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.channels[result[0]] = result[1]


    def start_browser(self):
        self.loggerv3.info('Starting browser')
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.page = self.browser.new_page()
        self.page.goto(self.url)


    def manually_sign_in(self):
        self.loggerv3.info('Manually fill in email, password, and 2-auth')
        sh.sign_in_confirm_helper()


    def get_channel_metrics(self):
        self.loggerv3.info('Getting channel metrics')
        # Navigate to Creator Studio
        self.page.locator('#avatar-btn > yt-img-shadow').click()
        self.page.get_by_text('YouTube Studio').click()
        for channel_title, channel_id in self.channels.items():

            # Navigate to Creator Studio Channel
            sh.change_channel_helper(self.page, self.channel_positions, channel_title)

            for start_date in self.start_dates:
                sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))

                # Get Monthly Unix Times
                unix_start = self.dater.convert_to_unix_timestamp(start_date)
                end_date_dt = datetime.strptime(start_date, '%Y-%m-%d') + relativedelta.relativedelta(months=1)
                end_date = end_date_dt.strftime('%Y-%m-%d')
                unix_end = self.dater.convert_to_unix_timestamp(end_date)

                print(f'Month {start_date}')
                metrics = {
                    'channel_id': channel_id,
                    'channel_title': channel_title,
                    'month_start': start_date
                }


                # Go to Audience Tab
                response = self.page.goto(f'https://studio.youtube.com/channel/{channel_id}/analytics/tab-build_audience/period-{unix_start},{unix_end}')
                if response.status == 200:
                    sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
                    audience = self.page.locator('#metric-total').all_inner_texts()
                    if len(audience) > 0:
                        metrics['returning_viewers'] = audience[0]
                        metrics['unique_viewers'] = audience[1]
                    else:
                        metrics['returning_viewers'] = None
                        metrics['unique_viewers'] = None
                else:
                    metrics['returning_viewers'] = None
                    metrics['unique_viewers'] = None


                # Append scraped metrics
                self.channel_metrics.append(metrics)
                sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))



    def convert_float_metrics_helper(self, metric):
        if metric is None or metric == "0" or metric == '—':
            return None
        elif 'K' in metric:
            return float(metric.replace('K', '').replace(',', '').replace('$', '')) * 1000
        elif 'M' in metric:
            return float(metric.replace('M', '').replace(',', '').replace('$', '')) * 1000000
        else:
            return float(metric.replace('$', '').replace(',', ''))


    def convert_int_metrics_helper(self, metric):
        if metric is None or metric == "0" or metric == '—':
            return None
        elif 'K' in metric:
            return int(float(metric.replace('K', '').replace(',', '').replace('$', '')) * 1000)
        elif 'M' in metric:
            return int(float(metric.replace('M', '').replace(',', '').replace('$', '')) * 1000000)
        else:
            return int(float(metric.replace('$', '').replace(',', '')))


    def clean_raw_data(self):
        self.loggerv3.info('Cleaning raw data')
        for channel in self.channel_metrics:
            d = {
                'month_start': channel['month_start'],
                'channel_id': channel['channel_id'],
                'channel_title': channel['channel_title'],
                'unique_viewers': self.convert_int_metrics_helper(channel['unique_viewers']),
                'returning_viewers': self.convert_int_metrics_helper(channel['returning_viewers'])
            }

            self.cleaned_metrics.append(d)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.cleaned_metrics)


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def close_browser(self):
        self.loggerv3.info('Closing browser')
        self.browser.close()


    def execute(self):
        self.loggerv3.start(f"Running YT Monthly Channel Scraper")
        self.load_channel_ids()
        self.start_browser()
        self.manually_sign_in()
        self.get_channel_metrics()
        self.clean_raw_data()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.close_browser()
        self.loggerv3.success("All Processing Complete!")
