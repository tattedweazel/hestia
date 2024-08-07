import pandas as pd
from time import sleep
from tools.modules.yt_scraper.base.yt_scraper_helpers import retry_goto_helper


class ContentTypeScraper:

    def __init__(self, page, video_id):
        self.page = page
        self.video_id = video_id
        self.MAX_SLEEP = 8
        self.MAX_RETRIES = 10
        self.metrics = {}
        self.dataframe = None


    def goto_page(self):
        link = f'https://studio.youtube.com/video/{self.video_id}/analytics/tab-reach_viewers/period-default'
        retry_goto_helper(self.page, link, self.MAX_RETRIES)
        sleep(self.MAX_SLEEP)


    def extract_data(self):
        titles = self.page.locator('#title').all_inner_texts()
        titles_str = ','.join(titles)

        if 'Short' in titles_str:
            self.metrics['content_type'] = 'short'
        elif 'live stream' in titles_str:
            self.metrics['content_type'] = 'livestream'
        else:
            self.metrics['content_type'] = 'vod'


    def clean_dataframe(self):
        self.dataframe = pd.DataFrame([self.metrics])
        self.dataframe['video_id'] = self.video_id


    def execute(self):
        self.goto_page()
        self.extract_data()
        self.clean_dataframe()
