import pandas as pd
from time import sleep
from tools.modules.yt_scraper.base.yt_scraper_helpers import convert_metrics_helper, retry_goto_helper


class OverviewScraper:

    def __init__(self, page, video_id, time_period):
        self.page = page
        self.video_id = video_id
        self.time_period = time_period
        self.MAX_SLEEP = 8
        self.MAX_RETRIES = 10
        self.metrics = {}
        self.dataframe = None


    def goto_page(self):
        link = f'https://studio.youtube.com/video/{self.video_id}/analytics/tab-overview/{self.time_period}'
        retry_goto_helper(self.page, link, self.MAX_RETRIES)
        sleep(self.MAX_SLEEP)


    def extract_data(self):
        concurrents = None
        while True:
            try:
                concurrents = self.page.locator('#display-value').all_inner_texts()
            except:
                sleep(self.MAX_SLEEP)
                continue
            break

        if len(concurrents) == 3:
            self.metrics['chat_messages'] = concurrents[0]
            self.metrics['peak_concurrents'] = concurrents[1]
            self.metrics['avg_concurrents'] = concurrents[2]
        elif len(concurrents) == 2:
            self.metrics['chat_messages'] = "0"
            self.metrics['peak_concurrents'] = concurrents[0]
            self.metrics['avg_concurrents'] = concurrents[1]
        else:
            self.metrics['chat_messages'] = "0"
            self.metrics['peak_concurrents'] = "0"
            self.metrics['avg_concurrents'] = "0"


    def clean_dataframe(self):
        self.dataframe = pd.DataFrame([self.metrics])
        self.dataframe['video_id'] = self.video_id

        self.dataframe['peak_concurrents'] = self.dataframe['peak_concurrents'].apply(convert_metrics_helper)
        self.dataframe['avg_concurrents'] = self.dataframe['avg_concurrents'].apply(convert_metrics_helper)
        self.dataframe['chat_messages'] = self.dataframe['chat_messages'].apply(convert_metrics_helper)

        convert = {'peak_concurrents': int, 'avg_concurrents': int, 'chat_messages': int}
        self.dataframe = self.dataframe.astype(convert)


    def execute(self):
        self.goto_page()
        self.extract_data()
        self.clean_dataframe()
