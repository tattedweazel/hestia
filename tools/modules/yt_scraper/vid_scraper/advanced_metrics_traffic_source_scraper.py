import pandas as pd
from time import sleep
from tools.modules.yt_scraper.base.yt_scraper_helpers import convert_metrics_helper, convert_time_helper, \
    convert_percentages_helper, retry_goto_helper


class AdvancedMetricsTrafficSourceScraper:

    def __init__(self, page, video_id, video_duration, time_period, advanced_time_period):
        self.page = page
        self.video_id = video_id
        self.video_duration = video_duration
        self.time_period = time_period
        self.advanced_time_period = advanced_time_period
        self.raw_data = None
        self.data_list = []
        self.chunked_list = []
        self.items_per_chunk = 6
        self.dataframe = None
        self.MAX_SLEEP = 8
        self.MAX_RETRIES = 10


    def goto_page(self):
        link = f"""https://studio.youtube.com/video/{self.video_id}/analytics/tab-overview/period-default/explore?entity_type=VIDEO&entity_id={self.video_id}&time_period={self.advanced_time_period}&explore_type=TABLE_AND_CHART&metric=VIEWS&granularity=DAY&t_metrics=VIEWS&t_metrics=AVERAGE_WATCH_TIME&t_metrics=VIDEO_THUMBNAIL_IMPRESSIONS&t_metrics=VIDEO_THUMBNAIL_IMPRESSIONS_VTR&dimension=TRAFFIC_SOURCE_TYPE&o_column=AVERAGE_WATCH_TIME&o_direction=ANALYTICS_ORDER_DIRECTION_DESC"""
        retry_goto_helper(self.page, link, self.MAX_RETRIES)
        sleep(self.MAX_SLEEP)


    def extract_data(self):
        while True:
            try:
                self.raw_data = self.page.locator('#debug-explore-table > div > iron-collapse').all_inner_texts()
            except:
                sleep(self.MAX_SLEEP)
                continue
            break


    def structure_data(self):
        self.data_list = self.raw_data[0].split('\n')
        len_data_list = len(self.data_list)
        for i in range(0, len_data_list, self.items_per_chunk):
            x = i
            self.chunked_list.append(self.data_list[x:x + self.items_per_chunk])


    def clean_dataframe(self):
        self.dataframe = pd.DataFrame(self.chunked_list)
        self.dataframe.rename(columns={0: 'traffic_source', 1: 'views', 2: 'pct_of_total_views', 3: 'avg_watch_time', 4: 'impressions', 5: 'ctr'}, inplace=True)
        if 'pct_of_total_views' in self.dataframe.columns:
            self.dataframe = self.dataframe.drop(columns=['pct_of_total_views'], axis=1)

        self.dataframe['views'] = self.dataframe['views'].apply(convert_metrics_helper)
        self.dataframe['avg_watch_time'] = self.dataframe['avg_watch_time'].apply(convert_time_helper)
        self.dataframe['impressions'] = self.dataframe['impressions'].apply(convert_metrics_helper)
        self.dataframe['ctr'] = self.dataframe['ctr'].apply(convert_percentages_helper)

        convert = {'views': int, 'avg_watch_time': int, 'impressions': int}
        self.dataframe = self.dataframe.astype(convert)

        self.dataframe['video_id'] = self.video_id
        self.dataframe['avg_completion_rate'] = self.dataframe['avg_watch_time'] / self.video_duration

        self.dataframe = self.dataframe.round(4)

        self.dataframe = self.dataframe[['video_id', 'traffic_source', 'impressions', 'ctr', 'views', 'avg_completion_rate']]


    def execute(self):
        self.goto_page()
        self.extract_data()
        if len(self.raw_data[0]) > 0:
            self.structure_data()
            self.clean_dataframe()
