import numpy as np
import pandas as pd
from base.etl_jobv3 import EtlJobV3
from time import sleep
from utils.components.batcher import Batcher
import utils.components.yt_variables as ytv
from utils.connectors.youtube_api_connector import YouTubeApiConnector


class DimYouTubeVideoJob(EtlJobV3):

    def __init__(self, backfill = False, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='dim_yt_video_v2')
        self.prod_schema = 'warehouse'
        self.staging_schema = 'staging'
        self.youtube_api_connector = YouTubeApiConnector(file_location=self.file_location, service='videos')
        self.channel_ids = ytv.channel_ids
        self.batcher = Batcher()
        self.BATCH_LIMIT = 50  # Max limit for youtube API
        self.SLEEP = 2
        self.videos_api = None
        self.video_ids = []
        self.untitled_videos = []
        self.batches = []
        self.videos = []
        self.quarantine_videos = []
        self.dataframe = None
        self.final_dataframe = None


    def instatiate_videos_api(self):
        self.loggerv3.info('Instatiating videos API')
        self.videos_api = self.youtube_api_connector.get_service_api()


    def identify_untitled_videos(self):
        self.loggerv3.info('Identifying untitled videos')
        query = f"""
            SELECT
                coca2.video_id,
                coca2.uploader_type
            FROM {self.prod_schema}.content_owner_combined_a2 coca2
            WHERE
                channel_id in {self.channel_ids}
                AND video_id not in (SELECT video_id FROM warehouse.dim_yt_video_v2)
            GROUP BY 1, 2;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.untitled_videos.append({
                'video_id': result[0],
                'uploader_type': result[1]
            })
            self.video_ids.append(result[0])


    def batch_videos(self):
        self.loggerv3.info('Batching videos')
        if len(self.video_ids) > 0:
            self.batches = self.batcher.list_to_list_batch(batch_limit=self.BATCH_LIMIT, iterator=self.video_ids)
        else:
            self.loggerv3.info('No videos to batch')


    def get_videos(self):
        self.loggerv3.info('Getting videos')
        if len(self.video_ids) > 0:
            counter = 0
            for batch in self.batches:
                results = self.videos_api.videos().list(
                    part="snippet, contentDetails, liveStreamingDetails",
                    id=','.join(batch)
                ).execute()
                counter += len(batch)
                for item in results['items']:
                    self.videos.append({
                        'video_id': item['id'],
                        'video_title': item['snippet']['title'],
                        'channel_id': item['snippet']['channelId'],
                        'channel_title': item['snippet']['channelTitle'],
                        'published_at': item['snippet']['publishedAt'],
                        'duration_in_seconds': pd.Timedelta(item['contentDetails']['duration']).total_seconds() - 1,
                        'livestream_start': self.livestream_start_helper(item)
                    })
                sleep(self.SLEEP)
        else:
            self.loggerv3.info('No videos to get from API')


    def livestream_start_helper(self, item):
        if 'liveStreamingDetails' in item:
            if 'actualStartTime' in item['liveStreamingDetails']:
                return item['liveStreamingDetails']['actualStartTime']
            elif 'scheduledStartTime' in item['liveStreamingDetails']:
                return item['liveStreamingDetails']['scheduledStartTime']
            else:
                return None
        else:
            return None


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        if len(self.videos) > 0:
            self.dataframe = pd.DataFrame(self.videos)
            untitled_videos_df = pd.DataFrame(self.untitled_videos)
            self.dataframe = self.dataframe.merge(untitled_videos_df, how='left', on='video_id')
            # Dedupe Final Dataframe
            self.final_dataframe = self.dataframe.groupby(['video_id', 'video_title', 'channel_id', 'channel_title']).agg(
                uploader_type=('uploader_type', np.min),
                published_at=('published_at', np.max),
                duration_in_seconds=('duration_in_seconds', np.max),
                livestream_start=('livestream_start', np.max)).reset_index()

            # If Published At < Livestream Start, then make Published At the same as Livestream Start
            self.final_dataframe.loc[self.final_dataframe['published_at'] < self.final_dataframe['livestream_start'], 'published_at'] = self.final_dataframe['livestream_start']

        else:
            self.loggerv3.info('Dataframe is empty')





    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to redshift staging')
        self.db_connector.write_to_sql(self.final_dataframe, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
               BEGIN TRANSACTION;
               
                   UPDATE {self.prod_schema}.{self.table_name}
                   SET 
                       livestream_start = staging.livestream_start
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.video_id = prod.video_id
                   WHERE
                       prod.livestream_start is NULL
                       AND staging.livestream_start is NOT NULL;

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                       prod.video_id = stage_{self.table_name}.video_id;

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        if self.final_dataframe is not None:
            self.db_connector.write_redshift(query)
        else:
            self.loggerv3.info('No write to redshift due to empty dataframe')


    def execute(self):
        self.loggerv3.start('Running Dim YouTube Video Job')
        self.instatiate_videos_api()
        self.identify_untitled_videos()
        self.batch_videos()
        self.get_videos()
        self.build_final_dataframe()
        if self.final_dataframe is not None:
            self.write_to_redshift_staging()
            self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
