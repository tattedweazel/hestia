import pandas as pd
import config
import random
import utils.components.yt_variables as sv
import tools.modules.yt_scraper.base.yt_scraper_helpers as sh
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from time import sleep
from tools.modules.yt_scraper.base.yt_scraper_helpers import convert_int_metrics_helper, convert_float_metrics_helper, convert_percentages_helper
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3
from utils.components.dater import Dater


class YTWeeklyChannelScraper:

    def __init__(self, start_dates, backfill):
        """
        :param start_dates: An array of string dates in format of YYYY-MM-DD
        Note duration is an integer representing the number of days the time range will last. The last day is exclusive. [start_date, start_date + duration)
        """
        self.table_name = 'yt_studio_weekly_channel_metrics'
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.dater = Dater()
        self.start_dates = start_dates
        self.backfill = backfill
        self.staging_schema = 'staging'
        self.prod_schema = 'warehouse'
        self.duration = 7
        self.channels = {}
        self.playwright = None
        self.browser = None
        self.page = None
        self.channel_metrics = []
        self.cleaned_metrics = []
        self.final_dataframe = None
        self.url = 'https://www.youtube.com/'
        self.membership_channels = ['Funhaus', 'DEATH BATTLE!']
        self.set_clause = None
        self.MIN_SLEEP = 6
        self.MAX_SLEEP = 8
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


    def revenue_metrics_helper(self, channel_id, unix_start, unix_end):
        response = self.page.goto(
            f'https://studio.youtube.com/channel/{channel_id}/analytics/tab-earn_revenue/period-{unix_start},{unix_end}')
        sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
        if response.status == 200:
            monetization = self.page.locator('#metric-total').all_inner_texts()
            if len(monetization) > 0:
                return monetization[0]
            else:
                print('Retrying revenue...')
                return self.revenue_metrics_helper(channel_id, unix_start, unix_end)
        else:
            print('Retrying revenue...')
            return self.revenue_metrics_helper(channel_id, unix_start, unix_end)


    def audience_metrics_helper(self, channel_id, unix_start, unix_end):
        response = self.page.goto(
            f'https://studio.youtube.com/channel/{channel_id}/analytics/tab-build_audience/period-{unix_start},{unix_end}')
        sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
        if response.status == 200:
            audience = self.page.locator('#metric-total').all_inner_texts()
            if len(audience) > 0:
                return {
                    'returning_viewers': audience[0],
                    'unique_viewers': audience[1]
                }
            else:
                print('Retrying audience...')
                return self.audience_metrics_helper(channel_id, unix_start, unix_end)
        else:
            print('Retrying audience...')
            return self.audience_metrics_helper(channel_id, unix_start, unix_end)


    def uvs_by_content_type_helper(self, channel_id, unix_start, unix_end):
        response = self.page.goto(
            f'https://studio.youtube.com/channel/{channel_id}/analytics/tab-content/period-default/explore?entity_type=CHANNEL&entity_id={channel_id}&time_period={unix_start},{unix_end}&explore_type=TABLE_AND_CHART&metric=NEW_VIEWERS&granularity=DAY&t_metrics=RETURNING_VIEWERS&t_metrics=NEW_VIEWERS&t_metrics=ESTIMATED_UNIQUE_VIEWERS&dimension=CREATOR_CONTENT_TYPE&o_column=NEW_VIEWERS&o_direction=ANALYTICS_ORDER_DIRECTION_DESC'
        )
        sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
        if response.status == 200:
            content_types = self.page.locator('#entity-title-value').all_inner_texts()
            returning_viewers = self.page.locator('#row-container > div:nth-child(3)').all_inner_texts()
            new_viewers = self.page.locator('#row-container > div:nth-child(4)').all_inner_texts()
            if len(content_types) > 0 and len(returning_viewers) > 0 and len(new_viewers) > 0:
                output = {}
                for idx, ct in enumerate(content_types):
                    if ct.lower() == 'videos':
                        output['returning_vods_uvs'] = returning_viewers[idx]
                        output['new_vods_uvs'] = new_viewers[idx]
                    elif ct.lower() == 'shorts':
                        output['returning_shorts_uvs'] = returning_viewers[idx]
                        output['new_shorts_uvs'] = new_viewers[idx]
                    elif ct.lower() == 'live stream' or ct.lower() == 'livestream' or ct.lower() == 'live streams' or ct.lower() == 'livestreams':
                        output['returning_live_uvs'] = returning_viewers[idx]
                        output['new_live_uvs'] = new_viewers[idx]

                return output
            else:
                print('Retrying UVs by Content Type...')
                return self.uvs_by_content_type_helper(channel_id, unix_start, unix_end)
        else:
            print('Retrying UVs by Content Type...')
            return self.uvs_by_content_type_helper(channel_id, unix_start, unix_end)


    def viewers_across_format_helper(self, channel_id, unix_start, unix_end):
        self.page.goto(
            f'https://studio.youtube.com/channel/{channel_id}/analytics/tab-content/period-{unix_start},{unix_end}'
        )
        sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
        if self.page.locator("role=button[name='All']").count() > 0:
            self.page.click("role=button[name='All']")
            sleep(self.MIN_SLEEP)
            viewers_across_format = self.page.locator('.legendListContainer').all_inner_texts()
            if len(viewers_across_format) > 0 and 'Shorts' in viewers_across_format[0] and 'Videos' in viewers_across_format[0]:
                vaf_data = viewers_across_format[0].split('\n')
                return {
                    f"{vaf_data[0].lower().replace(' ', '_')}": vaf_data[1],
                    f"{vaf_data[2].lower().replace(' ', '_')}": vaf_data[3],
                    f"{vaf_data[4].lower().replace(' ', '_')}": vaf_data[5],
                }
            else:
                return {
                    'shorts_only': None,
                    'watching_both': None,
                    'videos_only': None
                }
        else:
            return {
                'shorts_only': None,
                'watching_both': None,
                'videos_only': None
            }


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

                # Get Unix Times
                unix_start = self.dater.convert_to_unix_timestamp(start_date)
                end_date_dt = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(self.duration)
                end_date = end_date_dt.strftime('%Y-%m-%d')
                unix_end = self.dater.convert_to_unix_timestamp(end_date)
                week_end = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(self.duration - 1)).strftime('%Y-%m-%d')

                print(f'{channel_title} - Week ending {week_end}')
                metrics = {
                    'channel_id': channel_id,
                    'channel_title': channel_title,
                    'week_start': start_date,
                    'week_ending': week_end
                }

                # Go to Revenue Tab
                metrics['revenue'] = self.revenue_metrics_helper(channel_id, unix_start, unix_end)

                # Go to Audience Tab
                metrics['returning_viewers'] = self.audience_metrics_helper(channel_id, unix_start, unix_end)['returning_viewers']
                metrics['unique_viewers'] = self.audience_metrics_helper(channel_id, unix_start, unix_end)['unique_viewers']


                # Go to Viewers Across Format
                while True:
                    try:
                        viewers_across_format = self.viewers_across_format_helper(channel_id, unix_start, unix_end)
                        for key, val in viewers_across_format.items():
                            if key in ('shorts_only', 'watching_both', 'videos_only'):
                                metrics[key] = val
                    except:
                        input('Re-authenticate')
                        continue
                    break


                # UVs by Content Type
                uvs_by_content_type = self.uvs_by_content_type_helper(channel_id, unix_start, unix_end)
                metrics.update(uvs_by_content_type)


                # Go to Earn Tab
                if not self.backfill and channel_title in self.membership_channels:
                    response = self.page.goto(f'https://studio.youtube.com/channel/{channel_id}/monetization/memberships')
                    if response.status == 200:
                        sleep(self.MAX_SLEEP)
                        memberships = self.page.locator('#total-sponsors-card > p').all_inner_texts()
                        metrics['memberships'] = memberships[0]
                    else:
                        self.loggerv3.info(f'Channel {channel_title} for start_date {start_date} returned a {response.status}')
                        continue
                else:
                    metrics['memberships'] = None


                # Go to Advanced Metrics
                # Video
                response = self.page.goto(
                    f"""https://studio.youtube.com/channel/{channel_id}/analytics/tab-overview/period-default/explore?entity_type=CHANNEL&entity_id={channel_id}&ur_dimensions=CREATOR_CONTENT_TYPE&ur_values='VIDEO_ON_DEMAND'&ur_inclusive_starts=&ur_exclusive_ends=&time_period={unix_start},{unix_end}&explore_type=TABLE_AND_CHART&metric=TOTAL_ESTIMATED_EARNINGS&granularity=DAY&t_metrics=TOTAL_ESTIMATED_EARNINGS&t_metrics=EPM&t_metrics=VIEWS&dimension=VIDEO&o_column=TOTAL_ESTIMATED_EARNINGS&o_direction=ANALYTICS_ORDER_DIRECTION_DESC
                    """
                )
                if response.status == 200:
                    sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
                    rev_list = self.page.locator('#row-container > div:nth-child(3)').all_inner_texts()
                    if len(rev_list) > 0:
                        metrics['video_rev'] = rev_list[0]
                    else:
                        metrics['video_rev'] = None

                    rpm_list = self.page.locator('#row-container > div:nth-child(4)').all_inner_texts()
                    if len(rpm_list) > 0:
                        metrics['video_rpm'] = rpm_list[0]
                    else:
                        metrics['video_rpm'] = None

                    views_list = self.page.locator('#row-container > div:nth-child(5)').all_inner_texts()
                    if len(views_list) > 0:
                        metrics['video_views'] = views_list[0]
                    else:
                        metrics['video_views'] = None
                else:
                    metrics['video_rev'] = None
                    metrics['video_rpm'] = None
                    metrics['video_views'] = None

                # Shorts
                response = self.page.goto(
                    f"""https://studio.youtube.com/channel/{channel_id}/analytics/tab-overview/period-default/explore?entity_type=CHANNEL&entity_id={channel_id}&ur_dimensions=CREATOR_CONTENT_TYPE&ur_values='SHORTS'&ur_inclusive_starts=&ur_exclusive_ends=&time_period={unix_start},{unix_end}&explore_type=TABLE_AND_CHART&metric=TOTAL_ESTIMATED_EARNINGS&granularity=DAY&t_metrics=TOTAL_ESTIMATED_EARNINGS&t_metrics=EPM&t_metrics=VIEWS&dimension=VIDEO&o_column=TOTAL_ESTIMATED_EARNINGS&o_direction=ANALYTICS_ORDER_DIRECTION_DESC
                    """
                )
                if response.status == 200:
                    sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
                    rev_list = self.page.locator('#row-container > div:nth-child(3)').all_inner_texts()
                    if len(rev_list) > 0:
                        metrics['shorts_rev'] = rev_list[0]
                    else:
                        metrics['shorts_rev'] = None

                    rpm_list = self.page.locator('#row-container > div:nth-child(4)').all_inner_texts()
                    if len(rpm_list) > 0:
                        metrics['shorts_rpm'] = rpm_list[0]
                    else:
                        metrics['shorts_rpm'] = None

                    views_list = self.page.locator('#row-container > div:nth-child(5)').all_inner_texts()
                    if len(views_list) > 0:
                        metrics['shorts_views'] = views_list[0]
                    else:
                        metrics['shorts_views'] = None
                else:
                    metrics['shorts_rev'] = None
                    metrics['shorts_rpm'] = None
                    metrics['shorts_views'] = None

                # Livestreams
                response = self.page.goto(
                    f"""https://studio.youtube.com/channel/{channel_id}/analytics/tab-overview/period-default/explore?entity_type=CHANNEL&entity_id={channel_id}&ur_dimensions=CREATOR_CONTENT_TYPE&ur_values='LIVE_STREAM'&ur_inclusive_starts=&ur_exclusive_ends=&time_period={unix_start},{unix_end}&explore_type=TABLE_AND_CHART&metric=TOTAL_ESTIMATED_EARNINGS&granularity=DAY&t_metrics=TOTAL_ESTIMATED_EARNINGS&t_metrics=EPM&t_metrics=VIEWS&dimension=VIDEO&o_column=TOTAL_ESTIMATED_EARNINGS&o_direction=ANALYTICS_ORDER_DIRECTION_DESC
                    """
                )
                if response.status == 200:
                    sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))
                    rev_list = self.page.locator('#row-container > div:nth-child(3)').all_inner_texts()
                    if len(rev_list) > 0:
                        metrics['livestream_rev'] = rev_list[0]
                    else:
                        metrics['livestream_rev'] = None

                    rpm_list = self.page.locator('#row-container > div:nth-child(4)').all_inner_texts()
                    if len(rpm_list) > 0:
                        metrics['livestream_rpm'] = rpm_list[0]
                    else:
                        metrics['livestream_rpm'] = None

                    views_list = self.page.locator('#row-container > div:nth-child(5)').all_inner_texts()
                    if len(views_list) > 0:
                        metrics['livestream_views'] = views_list[0]
                    else:
                        metrics['livestream_views'] = None
                else:
                    metrics['livestream_rev'] = None
                    metrics['livestream_rpm'] = None
                    metrics['livestream_views'] = None

                # Append scraped metrics
                self.channel_metrics.append(metrics)

                # Navigate Back to Creator Studio Home
                self.page.goto(f'https://studio.youtube.com/channel/{channel_id}')
                sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))


    def clean_raw_data(self):
        self.loggerv3.info('Cleaning raw data')
        for channel in self.channel_metrics:
            d = {
                'week_start': channel['week_start'],
                'week_ending': channel['week_ending'],
                'channel_id': channel['channel_id'],
                'channel_title': channel['channel_title'],
                'revenue': convert_float_metrics_helper(channel['revenue']),
                'rpm': None,
                'unique_viewers': convert_int_metrics_helper(channel['unique_viewers']),
                'returning_viewers': convert_int_metrics_helper(channel['returning_viewers']),
                'memberships': convert_int_metrics_helper(channel['memberships']),
                'video_rev': convert_float_metrics_helper(channel['video_rev']),
                'video_rpm': convert_float_metrics_helper(channel['video_rpm']),
                'video_views': convert_int_metrics_helper(channel['video_views']),
                'shorts_rev': convert_float_metrics_helper(channel['shorts_rev']),
                'shorts_rpm': convert_float_metrics_helper(channel['shorts_rpm']),
                'shorts_views': convert_int_metrics_helper(channel['shorts_views']),
                'livestream_rev': convert_float_metrics_helper(channel['livestream_rev']),
                'livestream_rpm': convert_float_metrics_helper(channel['livestream_rpm']),
                'livestream_views': convert_int_metrics_helper(channel['livestream_views']),
                'shorts_only': convert_percentages_helper(channel['shorts_only']),
                'watching_both': convert_percentages_helper(channel['watching_both']),
                'videos_only': convert_percentages_helper(channel['videos_only']),
                'returning_vods_uvs': convert_int_metrics_helper(channel['returning_vods_uvs']) if 'returning_vods_uvs' in channel else None,
                'new_vods_uvs': convert_int_metrics_helper(channel['new_vods_uvs']) if 'new_vods_uvs' in channel else None,
                'returning_shorts_uvs': convert_int_metrics_helper(channel['returning_shorts_uvs']) if 'returning_shorts_uvs' in channel else None,
                'new_shorts_uvs': convert_int_metrics_helper(channel['new_shorts_uvs']) if 'new_shorts_uvs' in channel else None,
                'returning_live_uvs': convert_int_metrics_helper(channel['returning_live_uvs']) if 'returning_live_uvs' in channel else None,
                'new_live_uvs': convert_int_metrics_helper(channel['new_live_uvs']) if 'new_live_uvs' in channel else None
            }

            self.cleaned_metrics.append(d)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.cleaned_metrics)
        self.set_clause = f"""
            revenue =  staging.revenue, 
            unique_viewers =  staging.unique_viewers,
            returning_viewers = staging.returning_viewers,
            video_rev = staging.video_rev,
            video_rpm = staging.video_rpm,
            video_views = staging.video_views,
            shorts_rev = staging.shorts_rev,
            shorts_rpm = staging.shorts_rpm,
            shorts_views = staging.shorts_views,
            livestream_rev = staging.livestream_rev,
            livestream_rpm = staging.livestream_rpm,
            livestream_views = staging.livestream_views,
            returning_vods_uvs = staging.returning_vods_uvs,
            new_vods_uvs = staging.new_vods_uvs,
            returning_shorts_uvs = staging.returning_shorts_uvs,
            new_shorts_uvs = staging.new_shorts_uvs,
            returning_live_uvs = staging.returning_live_uvs,
            new_live_uvs = staging.new_live_uvs
        """
        if not self.backfill:
            self.set_clause += ", memberships = staging.memberships"


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
                        {self.set_clause}
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.channel_id = prod.channel_id
                       AND staging.week_ending = prod.week_ending;

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                        prod.channel_id = stage_{self.table_name}.channel_id
                        AND prod.week_ending = stage_{self.table_name}.week_ending;

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
        self.loggerv3.start(f"Running YT Weekly Channel Scraper")
        self.load_channel_ids()
        self.start_browser()
        self.manually_sign_in()
        self.get_channel_metrics()
        self.clean_raw_data()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.close_browser()
        self.loggerv3.success("All Processing Complete!")
