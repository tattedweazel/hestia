import pandas as pd
from time import sleep
from tools.modules.yt_scraper.base.yt_scraper_helpers import convert_metrics_helper, convert_time_helper, \
    convert_percentages_helper, convert_subscribers_helper, retry_goto_helper


class AdvancedMetricsContentScraper:


    def __init__(self, page, video_id, video_duration, time_period, advanced_time_period):
        self.page = page
        self.video_id = video_id
        self.video_duration = video_duration
        self.time_period = time_period
        self.advanced_time_period = advanced_time_period
        self.MAX_SLEEP = 8
        self.RETRY_COUNTER = 1
        self.MAX_RETRIES = 10
        self.metrics = {}
        self.dataframe = None


    def goto_page(self):
        link = f"""https://studio.youtube.com/video/{self.video_id}/analytics/tab-reach_viewers/{self.time_period}/explore?entity_type=VIDEO&entity_id={self.video_id}&time_period={self.advanced_time_period}&explore_type=TABLE_AND_CHART&metric=AVERAGE_WATCH_TIME&granularity=DAY&t_metrics=AVERAGE_WATCH_TIME&t_metrics=SUBSCRIBERS_NET_CHANGE&t_metrics=ESTIMATED_UNIQUE_VIEWERS&t_metrics=RETURNING_VIEWERS&t_metrics=WATCH_TIME&t_metrics=VIDEO_THUMBNAIL_IMPRESSIONS_VTR&t_metrics=VIDEO_THUMBNAIL_IMPRESSIONS&t_metrics=SHORTS_FEED_IMPRESSIONS_VTR&t_metrics=SHORTS_FEED_IMPRESSIONS&t_metrics=VIEWS&t_metrics=COMMENTS&t_metrics=SHARINGS&t_metrics=RATINGS_LIKES&dimension=VIDEO&o_column=AVERAGE_WATCH_TIME&o_direction=ANALYTICS_ORDER_DIRECTION_DESC"""
        retry_goto_helper(self.page, link, self.MAX_RETRIES)
        sleep(self.MAX_SLEEP)

        # Check if video exists
        if self.page.get_by_text('Oops, something went wrong').count() == 1 or self.page.get_by_text('Data is still processing. Please check back later.').count() == 1:
            return False
        return True


    def extract_data(self):
        avg_watch_time_list = self.page.locator('#row-container > div:nth-child(3)').all_inner_texts()
        if len(avg_watch_time_list) > 0:
            self.metrics['avg_watch_time'] = avg_watch_time_list[0]
        else:
            self.metrics['avg_watch_time'] = None

        subscribers_list = self.page.locator('#row-container > div:nth-child(4)').all_inner_texts()
        if len(subscribers_list) > 0:
            self.metrics['subscribers'] = subscribers_list[0]
        else:
            self.metrics['subscribers'] = None

        unique_viewers_list = self.page.locator('#row-container > div:nth-child(5)').all_inner_texts()
        if len(unique_viewers_list) > 0:
            self.metrics['unique_viewers'] = unique_viewers_list[0]
        else:
            self.metrics['unique_viewers'] = None

        returning_viewers_list = self.page.locator('#row-container > div:nth-child(6)').all_inner_texts()
        if len(returning_viewers_list) > 0:
            self.metrics['returning_viewers'] = returning_viewers_list[0]
        else:
            self.metrics['returning_viewers'] = None

        watch_time_list = self.page.locator('#row-container > div:nth-child(7)').all_inner_texts()
        if len(watch_time_list) > 0:
            self.metrics['watch_time'] = watch_time_list[0]
        else:
            self.metrics['watch_time'] = None

        ctr_list = self.page.locator('#row-container > div:nth-child(8)').all_inner_texts()
        if len(ctr_list) > 0:
            self.metrics['ctr'] = ctr_list[0]
        else:
            self.metrics['ctr'] = None

        impressions_list = self.page.locator('#row-container > div:nth-child(9)').all_inner_texts()
        if len(impressions_list) > 0:
            self.metrics['impressions'] = impressions_list[0]
        else:
            self.metrics['impressions'] = None

        viewed_vs_swiped_list = self.page.locator('#row-container > div:nth-child(10)').all_inner_texts()
        if len(viewed_vs_swiped_list) > 0:
            self.metrics['viewed_vs_swiped'] = viewed_vs_swiped_list[0]
        else:
            self.metrics['viewed_vs_swiped'] = "0"

        shown_in_feed_list = self.page.locator('#row-container > div:nth-child(11)').all_inner_texts()
        if len(shown_in_feed_list) > 0:
            self.metrics['shown_in_feed'] = shown_in_feed_list[0]
        else:
            self.metrics['shown_in_feed'] = "0"

        views_list = self.page.locator('#row-container > div:nth-child(12)').all_inner_texts()
        if len(views_list) > 0:
            self.metrics['views'] = views_list[0]
        else:
            self.metrics['views'] = "0"

        comments_list = self.page.locator('#row-container > div:nth-child(13)').all_inner_texts()
        if len(comments_list) > 0:
            self.metrics['comments'] = comments_list[0]
        else:
            self.metrics['comments'] = "0"

        shares_list = self.page.locator('#row-container > div:nth-child(14)').all_inner_texts()
        if len(shares_list) > 0:
            self.metrics['shares'] = shares_list[0]
        else:
            self.metrics['shares'] = "0"

        likes_list = self.page.locator('#row-container > div:nth-child(15)').all_inner_texts()
        if len(likes_list) > 0:
            self.metrics['likes'] = likes_list[0]
        else:
            self.metrics['likes'] = "0"


        if self.metrics['unique_viewers'] is None or self.metrics['unique_viewers'] == 0:
            if self.RETRY_COUNTER < self.MAX_RETRIES:
                print('Retrying Advanced Metrics Content Scraper...')
                # Update retry counter
                self.RETRY_COUNTER += 1
                # Reset output dict
                self.metrics = {}
                # Re-run methods
                self.goto_page()
                self.extract_data()
            else:
                print('Max retries for Advanced Metrics Content Scraper...')



    def clean_dataframe(self):
        self.dataframe = pd.DataFrame([self.metrics])
        self.dataframe['video_id'] = self.video_id

        self.dataframe['returning_viewers'] = self.dataframe['returning_viewers'].apply(convert_metrics_helper)
        self.dataframe['unique_viewers'] = self.dataframe['unique_viewers'].apply(convert_metrics_helper)
        self.dataframe['subscribers'] = self.dataframe['subscribers'].apply(convert_subscribers_helper)
        self.dataframe['impressions'] = self.dataframe['impressions'].apply(convert_metrics_helper)
        self.dataframe['ctr'] = self.dataframe['ctr'].apply(convert_percentages_helper)
        self.dataframe['watch_time_from_imps_hr'] = self.dataframe['watch_time'].apply(convert_metrics_helper)
        self.dataframe['avg_watch_time_sec'] = self.dataframe['avg_watch_time'].apply(convert_time_helper)
        self.dataframe['viewed_vs_swiped'] = self.dataframe['viewed_vs_swiped'].apply(convert_percentages_helper)
        self.dataframe['shown_in_feed'] = self.dataframe['shown_in_feed'].apply(convert_metrics_helper)
        self.dataframe['views'] = self.dataframe['views'].apply(convert_metrics_helper)
        self.dataframe['comments'] = self.dataframe['comments'].apply(convert_metrics_helper)
        self.dataframe['shares'] = self.dataframe['shares'].apply(convert_metrics_helper)
        self.dataframe['likes'] = self.dataframe['likes'].apply(convert_metrics_helper)
        self.dataframe['avg_completion_rate'] = self.dataframe['avg_watch_time'].apply(convert_time_helper) / self.video_duration

        self.dataframe['new_viewer_pct'] = 0.0
        self.dataframe.loc[self.dataframe['unique_viewers'] > 0, 'new_viewer_pct'] = (self.dataframe['unique_viewers'] - self.dataframe['returning_viewers']) / self.dataframe['unique_viewers']

        self.dataframe['score'] = ((self.dataframe['unique_viewers'] * self.dataframe['avg_completion_rate']) / 100).round(2)

        self.dataframe = self.dataframe.round(4)

        convert = {'returning_viewers': int, 'unique_viewers': int, 'subscribers': int, 'impressions': int, 'avg_watch_time_sec': int,  'views': int,
                   'comments': int, 'shares': int, 'likes': int}
        self.dataframe = self.dataframe.astype(convert)

        self.dataframe = self.dataframe[
            ['video_id', 'unique_viewers', 'returning_viewers', 'new_viewer_pct', 'impressions', 'ctr', 'subscribers',
             'watch_time_from_imps_hr', 'avg_watch_time_sec', 'avg_completion_rate', 'score',  'viewed_vs_swiped',
             'shown_in_feed', 'views', 'comments', 'shares', 'likes']
        ]



    def execute(self):
        if self.goto_page() is True:
            self.extract_data()
            self.clean_dataframe()
