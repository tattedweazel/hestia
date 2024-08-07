import os
import json
import pandas as pd
import random
from base.etl_jobv3 import EtlJobV3
from utils.components.batcher import Batcher
from utils.connectors.s3_api_connector import S3ApiConnector


class FreeUserOnboardingEpisodesJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='user_onboarding_episodes')
        self.schema = 'warehouse'
        self.batcher = Batcher()
        self.batches = None
        self.TOP_VIDEO_SEC_LENGTH = 1800
        self.EPISODE_LIMIT = 50
        self.target_date = target_date
        self.lookback_days = -28
        self.output_location = self.file_location + 'uploads'
        self.output_filename = 'featured_episodes_free.json'
        self.rt_bucket = 'rt-popularity'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.free_episodes = []
        self.premium_episodes = []
        self.live_episodes = []
        self.bonus_features = []
        self.final_episodes = []
        self.final_dataframe = None


    def get_free_user_episodes(self):
        self.loggerv3.info('Getting free user episodes')
        query = f"""
            WITH last_n_days as (
                SELECT 
                    user_key
                FROM warehouse.vod_viewership
                WHERE user_key is not null
                  AND user_tier in ('free')
                  AND start_timestamp > dateadd('days', {self.lookback_days}, '{self.target_date}')
                  AND start_timestamp < dateadd('days', 1, '{self.target_date}')
                  AND max_position > 0
                GROUP BY 1
            ), previous_viewers as (
                SELECT 
                    user_key
                FROM warehouse.vod_viewership
                WHERE user_key is not null
                  AND user_tier in ('free')
                  AND start_timestamp <= dateadd('days', {self.lookback_days}, '{self.target_date}')
                GROUP BY 1
            ), new_viewers as (
                SELECT 
                    user_key
                FROM last_n_days
                WHERE user_key not in (SELECT user_key FROM previous_viewers)
                GROUP BY 1
            ), user_viewership as (
                SELECT
                    vv.user_key,
                    dse.channel_title,
                    dse.series_title,
                    dse.season_title,
                    dse.episode_title,
                    dse.episode_key,
                    dse.episode_number,
                    dse.length_in_seconds,
                    (CASE
                        WHEN sum(vv.active_seconds) > dse.length_in_seconds THEN dse.length_in_seconds
                        ELSE sum(vv.active_seconds)
                    END) as active_seconds
                FROM warehouse.vod_viewership vv
                         INNER JOIN warehouse.dim_segment_episode dse on vv.episode_key = dse.episode_key
                WHERE vv.user_key in (SELECT user_key FROM new_viewers)
                  AND vv.user_tier in ('free')
                  AND vv.start_timestamp > dateadd('days', {self.lookback_days}, '{self.target_date}')
                  AND vv.start_timestamp < dateadd('days', 1, '{self.target_date}')
                  AND dse.length_in_seconds > 0
                  AND dse.channel_title NOT in ('Friends of RT', 'Kinda Funny', 'The Yogscast')
                GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
            ), vod_agg as (
                SELECT 
                    channel_title,
                    series_title,
                    season_title,
                    episode_title,
                    episode_key,
                    episode_number,
                    length_in_seconds,
                    sum(active_seconds) as vod_watched,
                    count(distinct user_key) as unique_viewers
                FROM user_viewership
                GROUP BY 1, 2, 3, 4, 5, 6, 7
            ), cmp as (
                SELECT 
                    channel_title,
                    series_title,
                    season_title,
                    episode_title,
                    episode_key,
                    episode_number,
                    length_in_seconds,
                    vod_watched,
                    unique_viewers,
                    round((1.0 * vod_watched) / (length_in_seconds * unique_viewers), 2) as completion_pct
                FROM vod_agg
            ), agg as (
                SELECT 
                    channel_title,
                    series_title,
                    season_title,
                    episode_title,
                    episode_key,
                    episode_number,
                    vod_watched,
                    unique_viewers,
                    completion_pct,
                    length_in_seconds,
                    round(completion_pct * unique_viewers, 1)                    as weight,
                    rank() over (partition by series_title order by weight desc, unique_viewers desc) as rnk
                FROM cmp
            )
            SELECT 
                channel_title,
                series_title,
                season_title,
                episode_title,
                episode_key,
                episode_number,
                vod_watched,
                unique_viewers,
                completion_pct,
                weight,
                length_in_seconds
            FROM agg
            WHERE rnk = 1
            ORDER BY weight desc, unique_viewers desc
            LIMIT {self.EPISODE_LIMIT};
        """

        results = self.db_connector.read_redshift(query)
        for result in results:
            self.free_episodes.append({
                'channel_title': result[0],
                'series_title': result[1],
                'season_title': result[2],
                'episode_title': result[3],
                'episode_key': result[4],
                'episode_number': result[5],
                'vod_watched': result[6],
                'unique_viewers': result[7],
                'completion_pct': float(result[8]),
                'weight': float(result[9]),
                'length_in_seconds': result[10]
            })


    def get_svod_be_premium_episodes(self):
        self.loggerv3.info('Getting svod-be premium episodes')
        query = """
            SELECT e.uuid
            FROM episodes e
            WHERE
                e.published_at < now() AND
                (
                    e.is_sponsors_only = 1 OR
                    e.public_golive_at > now()
                );
        """
        results = self.db_connector.query_svod_be_db_connection(query)
        for result in results:
            self.premium_episodes.append(result[0])


    def get_svod_be_live_episodes(self):
        self.loggerv3.info('Getting svod-be live episodes')
        uuids = [episode['episode_key'] for episode in self.free_episodes]
        self.batches = self.batcher.list_to_list_batch(batch_limit=500, iterator=uuids)
        for batch in self.batches:
            episodes = ','.join([f"'{x}'" for x in batch])
            query = f"""
            SELECT uuid
            FROM episodes
            WHERE
                published_at is not null
                AND published_at < now()
                AND uuid in ({episodes});
            """
            results = self.db_connector.query_svod_be_db_connection(query)
            for result in results:
                self.live_episodes.append(result[0])


    def get_bonus_features(self):
        self.loggerv3.info('Getting bonus features episodes')
        query = """
            SELECT uuid
            FROM bonus_features
            WHERE published_at < now();
        """
        results = self.db_connector.query_svod_be_db_connection(query)
        for result in results:
            self.bonus_features.append(result[0])


    def build_final_episodes(self):
        self.loggerv3.info('Building final episodes')
        rank = 1
        self.final_episodes = []
        for episode in self.free_episodes:
            if episode['episode_key'] not in self.premium_episodes and \
                    episode['episode_key'] not in self.bonus_features and \
                    episode['episode_key'] in self.live_episodes:
                episode['rank'] = rank
                self.final_episodes.append(episode)
                rank += 1

        if len(self.final_episodes) == 0:
            raise ValueError('Missing episode data')


    def reorganize_final_episodes(self):
        self.loggerv3.info('Reorganizing final episodes')
        """We want one video from each channel in the top 4"""
        top_channels = []
        popped_episodes = []
        for episode in self.final_episodes:
            if episode['channel_title'] not in top_channels and episode['length_in_seconds'] <= self.TOP_VIDEO_SEC_LENGTH:
                top_channels.append(episode['channel_title'])
                popped_episodes.append(episode)
                if len(popped_episodes) == 4:
                    break

        random.shuffle(popped_episodes)
        for popped_episode in popped_episodes:
            self.final_episodes.remove(popped_episode)

        self.final_episodes = popped_episodes + self.final_episodes
        # Re-ranking (keep ranking in self.build_final_episodes for debugging purposes)
        rank = 1
        for episode in self.final_episodes:
            episode['rank'] = rank
            rank += 1
            del episode['length_in_seconds']


    def write_results_to_json(self):
        self.loggerv3.info('Write results to JSON')
        with open(f"{self.output_location}/{self.output_filename}", 'w') as f:
            f.write(json.dumps(self.final_episodes))


    def upload_output_to_s3(self):
        self.loggerv3.info("Uploading to S3")
        self.s3_api_connector.upload_file(f"{self.output_location}/{self.output_filename}", self.output_filename)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.final_episodes)
        self.final_dataframe['run_date'] = self.target_date
        self.final_dataframe['tier'] = 'free'


    def write_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def cleanup(self):
        cmd = f"rm {self.output_location}/*.*"
        os.system(cmd)


    def execute(self):
        self.loggerv3.start(f"Running Free User Onboarding Episodes Job for {self.target_date}")
        self.get_free_user_episodes()
        self.get_svod_be_premium_episodes()
        self.get_svod_be_live_episodes()
        self.get_bonus_features()
        self.build_final_episodes()
        self.reorganize_final_episodes()
        self.write_results_to_json()
        self.upload_output_to_s3()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.cleanup()
        self.loggerv3.success("All Processing Complete!")
