import pandas as pd
from time import sleep
from tools.modules.yt_scraper.base.yt_scraper_helpers import retry_goto_helper


class DetailsScraper:

    def __init__(self, page, video_id):
        self.page = page
        self.video_id = video_id
        self.MAX_SLEEP = 8
        self.MAX_RETRIES = 3
        self.metrics = {}
        self.dataframe = None


    def goto_page(self):
        link = f'https://studio.youtube.com/video/{self.video_id}/edit'
        retry_goto_helper(self.page, link, self.MAX_RETRIES)
        sleep(self.MAX_SLEEP)


    def extract_data(self):
        try:
            visibility_tag = self.page.locator('#visibility-text').inner_text()
        except:
            visibility_tag = 'Public'
        self.metrics['visibility_tag'] = visibility_tag


    def clean_dataframe(self):
        self.dataframe = pd.DataFrame([self.metrics])
        self.dataframe['video_id'] = self.video_id


    def execute(self):
        self.goto_page()
        self.extract_data()
        self.clean_dataframe()
