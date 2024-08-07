import config
import json
import os
import random
from time import sleep
import utils.components.yt_variables as sv
from playwright.sync_api import sync_playwright
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3
from utils.components.dater import Dater
from utils.connectors.s3_api_connector import S3ApiConnector
from utils.components.sql_helper import SqlHelper


class YTVidCommentScraper:

    def __init__(self, video_ids):
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=True)
        self.dater = Dater()
        self.sql_helper = SqlHelper()
        self.video_ids = video_ids
        self.channel_videos = {}
        self.video_comments = {}
        self.rt_bucket = 'rt-data-youtube'
        self.s3_api_connector = S3ApiConnector(file_location=config.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.SLEEP = 8
        self.output_directory = config.file_location + 'tools/output/yt_scraper'
        self.mod_check_in = 1000
        self.SLEEP_FOR_LOCAL_WRITE = 2
        self.MIN_SLEEP = 6
        self.MAX_SLEEP = 8
        self.playwright = None
        self.browser = None
        self.page = None
        self.url = 'https://www.youtube.com/'
        self.channel_positions = sv.channel_positions
        self.final_dataframe = None


    def get_channel_ids(self):
        self.loggerv3.info('Getting Channel IDs')
        videos = self.sql_helper.array_to_sql_list(self.video_ids)
        query = f"""
            	SELECT 
            	    channel_title,
            		video_id
            	FROM warehouse.dim_yt_video_v2
            	WHERE video_id in ({videos})
            	GROUP BY 1, 2;
            	"""
        results = self.db_connector.read_redshift(query)
        for result in results:
            if result[0] not in self.channel_videos:
                self.channel_videos[result[0]] = [result[1]]
            else:
                self.channel_videos[result[0]].append(result[1])


    def start_browser(self):
        self.loggerv3.info('Starting browser')
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.page = self.browser.new_page()
        self.page.goto(self.url)


    def manually_sign_in(self):
        self.loggerv3.info('Manually fill in email, password, and 2-auth')
        while True:
            confirm = input("Did you sign in (y/n), 2-factor auth, a choose the channel, and navigate to creator studio? ")
            if confirm != 'y':
                print("Need to enter a 'y' to confirm")
                continue
            else:
                break


    def get_comments(self):
        self.loggerv3.info('Getting comments')
        # Navigate to Creator Studio
        self.page.locator('#avatar-btn > yt-img-shadow').click()
        self.page.get_by_text('YouTube Studio').click()
        for channel_title, videos in self.channel_videos.items():
            print(channel_title)
            # Navigate to Creator Studio Channel
            self.page.locator('#avatar-btn > yt-img-shadow').click()
            self.page.get_by_text('Switch Account').click()
            channel_position = self.channel_positions[channel_title]
            self.page.locator(f'#contents > ytd-account-item-renderer:nth-child({channel_position})').click()
            videos = self.channel_videos[channel_title]
            sleep(random.randrange(self.MIN_SLEEP, self.MAX_SLEEP))

            for video_id in videos:
                print(f" {video_id}")

                # Navigate to Creator Studio
                self.page.goto(f'https://studio.youtube.com/video/{video_id}/comments/inbox?filter=%5B%5D')
                sleep(random.randrange(self.SLEEP))
                self.page.locator('#main').click()
                self.video_comments[video_id] = []

                counter = 0
                while True:
                    scraped = self.page.locator('#content-text').all_inner_texts()
                    for comment in scraped:
                        cleaned_comment = {'comment': comment}
                        if cleaned_comment not in self.video_comments[video_id]:
                            self.video_comments[video_id].append(cleaned_comment)
                    self.page.keyboard.press("PageDown")
                    if counter % self.mod_check_in == 0:
                        response = input('Continue? ')
                        if response.lower() in ('n', 'no'):
                            break
                    counter += 1


    def upload_output_to_s3(self):
        self.loggerv3.info("Uploading to S3")

        for video_id, comments in self.video_comments.items():
            output_filename = f'{video_id}.json'
            full_path = '/'.join([self.output_directory, output_filename])
            key = f'video-comments/{output_filename}'

            with open(full_path, 'w') as f:
                json.dump(comments, f)

            sleep(self.SLEEP_FOR_LOCAL_WRITE)
            self.s3_api_connector.upload_file(full_path, key)


    def close_browser(self):
        self.loggerv3.info('Closing browser')
        self.browser.close()


    def cleanup(self):
        cmd = f"rm {self.output_directory}/*.*"
        os.system(cmd)


    def execute(self):
        self.loggerv3.start(f"Running YT Vid Comments Scraper")
        self.get_channel_ids()
        self.start_browser()
        self.manually_sign_in()
        self.get_comments()
        self.upload_output_to_s3()
        self.close_browser()
        self.cleanup()
        self.loggerv3.success("All Processing Complete!")
