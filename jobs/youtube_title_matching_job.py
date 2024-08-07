import pandas as pd
from base.etl_jobv3 import EtlJobV3
from rapidfuzz import fuzz, process, utils, string_metric



class YouTubeTitleMatchingJob(EtlJobV3):

    def __init__(self, backfill = False, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='yt_video_map')
        self.schema = 'warehouse'
        self.yt_videos = []
        self.rt_videos = []
        self.EXTRACT_LIMIT = 1
        self.MATCH_THRESHOLD = 90
        self.remainder_yt_videos = []
        self.remainder_rt_videos = []
        self.matched_records_df = None
        self.remainder_matched_records_df = None
        self.videos_df = None
        self.final_dataframe = None


    def get_youtube_videos(self):
        self.loggerv3.info('Getting youtube videos')
        query = """
            SELECT
                video_id,
                video_title,
                concat(concat(channel_title, ' '), video_title)
            FROM warehouse.dim_yt_video_v2
            WHERE
                video_id NOT IN (SELECT video_id FROM warehouse.yt_video_map GROUP BY 1)
                AND published_at >= '2021-01-01'
                AND channel_title in 
                    ('Rooster Teeth Animation', 'F**KFACE', 'Face Jam', 'Annual Pass',
                    'Achievement Hunter', 'Rooster Teeth', 'Red Web', 'Funhaus', 
                    'Squad Team Force', 'DEATH BATTLE!', 'Black Box Down', 'LetsPlay', 
                    'Rooster Teeth Trailers', 'Funhaus Too', 'All Good No Worries',
                    'Tales from the Stinky Dragon')
                AND video_title NOT IN ('Funhaus Live!', 'Funhaus GTA')
            GROUP BY 1, 2, 3;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.yt_videos.append({
                'video_id': result[0],
                'video_title': result[1],
                'full_video_title': result[2]
            })


    def get_rt_videos(self):
        self.loggerv3.info('Getting RT videos')
        query = """
            SELECT episode_key,
                   episode_title,
                   (CASE
                       WHEN channel_title = 'Achievement Hunter' and series_title like '%%Let''s Play%%' THEN concat(concat('Let''s Play', ' '), episode_title)
                       WHEN channel_title = 'Rooster Teeth' and series_title = 'F**KFACE' THEN concat(concat('F**KFACE', ' '), episode_title)
                       WHEN channel_title = 'Rooster Teeth' and series_title like '%%Face Jam%%' THEN concat(concat(series_title, ' '), episode_title)
                       WHEN channel_title = 'Achievement Hunter' and series_title = 'Annual Pass' THEN concat(concat('Annual Pass', ' '), episode_title)
                       WHEN channel_title = 'Achievement Hunter' and series_title like '%%Red Web%%' THEN concat(concat(series_title, ' '), episode_title)
                       WHEN channel_title = 'Squad Team Force' and series_title = 'Black Box Down' THEN concat(concat('Black Box Down', ' '), episode_title)
                       ELSE concat(concat(channel_title, ' '), episode_title)
                    END) as title
            FROM warehouse.dim_segment_episode
            WHERE
                public_date >= '2021-01-01'
                AND air_date <= current_date
                AND channel_title NOT IN ('Friends of RT', 'Kinda Funny', 'The Yogscast')
                AND episode_key NOT IN (SELECT episode_key FROM warehouse.yt_video_map GROUP BY 1)
                AND series_title NOT IN ('RTX', 'Red vs. Blue Complete', 'Rooster Teeth Backstage')
                AND episode_title NOT IN ('The Last Episode', 'Achievement Hunter', 'Season 2 - Trailer', 'FOR POWER Pt.2')
                AND regexp_count(episode_title, ' ') != 0
            GROUP BY 1, 2, 3;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.rt_videos.append({
                'episode_key': result[0],
                'episode_title': result[1],
                'full_episode_title': result[2]
            })


    def full_title_match(self):
        self.loggerv3.info('Fuzzy mapping RT titles to YouTube titles')
        # Get list of full RT video episode titles
        rt_full_titles = [video['full_episode_title'] for video in self.rt_videos]

        # Use RapidFuzz on full titles: Get most accurate match with accuracy that meets or exceeds threshold
        yt_videos_df = pd.DataFrame(self.yt_videos)
        yt_videos_df['matches1'] = yt_videos_df['full_video_title'].apply(lambda x: process.extract(x, rt_full_titles, limit=self.EXTRACT_LIMIT, score_cutoff=self.MATCH_THRESHOLD, processor=utils.default_process))
        yt_videos_df['matches2'] = yt_videos_df['full_video_title'].apply(lambda x: process.extract(x, rt_full_titles, limit=self.EXTRACT_LIMIT, scorer=fuzz.token_set_ratio, score_cutoff=self.MATCH_THRESHOLD, processor=utils.default_process))

        # Identify the match with the highest accuracy
        yt_records = yt_videos_df.to_dict('records')
        for video in yt_records:
            video['all_matches'] = video['matches1'] + video['matches2']
            final_match = ''
            final_accuracy = 0
            for match in video['all_matches']:
                if match[1] > final_accuracy:
                    final_accuracy = match[1]
                    final_match = match[0]
            video['final_match'] = final_match
            video['final_accuracy'] = final_accuracy

        # Join with RT videos to get episode keys
        yt_records_df = pd.DataFrame(yt_records)
        rt_records_df = pd.DataFrame(self.rt_videos)
        joined_records_df = yt_records_df.merge(rt_records_df, left_on='final_match', right_on='full_episode_title', how='left')

        # Filter for matches
        self.matched_records_df = joined_records_df[(joined_records_df['final_accuracy'] > 0)]

        matched_records = self.matched_records_df.to_dict('records')
        matched_video_ids = [record['video_id'] for record in matched_records]
        matched_episode_keys = [record['episode_key'] for record in matched_records]

        # Do title matching again but just with remainders (i.e. records that didnt initially match)

        # Filter YT videos to just have remainders
        for video in self.yt_videos:
            if video['video_id'] not in matched_video_ids:
                self.remainder_yt_videos.append(video)

        # Filter RT videos to just have remainders
        for video in self.rt_videos:
            if video['episode_key'] not in matched_episode_keys:
                self.remainder_rt_videos.append(video)

        # Get list of remainder RT video episode titles
        remainder_rt_titles = [video['episode_title'] for video in self.remainder_rt_videos]

        # Use RapidFuzz on remainder titles: Get most accurate match with accuracy that meets or exceeds threshold
        remainder_yt_videos_df = pd.DataFrame(self.remainder_yt_videos)
        remainder_yt_videos_df['matches1'] = remainder_yt_videos_df['video_title'].apply(lambda x: process.extract(x, remainder_rt_titles, limit=self.EXTRACT_LIMIT, score_cutoff=self.MATCH_THRESHOLD, processor=utils.default_process))
        remainder_yt_videos_df['matches2'] = remainder_yt_videos_df['video_title'].apply(lambda x: process.extract(x, remainder_rt_titles, limit=self.EXTRACT_LIMIT, scorer=fuzz.token_set_ratio, score_cutoff=self.MATCH_THRESHOLD, processor=utils.default_process))

        # Identify the match with the highest accuracy
        remainder_yt_records = remainder_yt_videos_df.to_dict('records')
        for video in remainder_yt_records:
            video['all_matches'] = video['matches1'] + video['matches2']
            final_match = ''
            final_accuracy = 0
            for match in video['all_matches']:
                if match[1] > final_accuracy:
                    final_accuracy = match[1]
                    final_match = match[0]
            video['final_match'] = final_match
            video['final_accuracy'] = final_accuracy

        # Join with RT videos to get episode keys
        remainder_yt_records_df = pd.DataFrame(remainder_yt_records)
        remainder_rt_records_df = pd.DataFrame(self.remainder_rt_videos)
        remainder_joined_records_df = remainder_yt_records_df.merge(remainder_rt_records_df, left_on='final_match', right_on='episode_title', how='left')

        # Filter for remainder matches
        self.remainder_matched_records_df = remainder_joined_records_df[(remainder_joined_records_df['final_accuracy'] > 0)]


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        # Join initial matches and remainder matches
        self.videos_df = pd.concat([self.matched_records_df, self.remainder_matched_records_df])

        # Identify Duplicates
        self.videos_df['episode_freq'] = self.videos_df.groupby('episode_key')['episode_key'].transform('count')
        self.videos_df['video_freq'] = self.videos_df.groupby('video_id')['video_id'].transform('count')

        # Remove Duplicates
        self.final_dataframe = self.videos_df
        self.final_dataframe = self.final_dataframe[self.final_dataframe['episode_freq'] == 1]
        self.final_dataframe = self.final_dataframe[self.final_dataframe['video_freq'] == 1]

        # Set Final Columns
        self.final_dataframe = self.final_dataframe[['episode_key', 'video_id']]
        self.loggerv3.info(f'Adding {len(self.final_dataframe)} records to {self.table_name}')


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def execute(self):
        self.loggerv3.start('Running YouTube Title Matching Job')
        self.get_youtube_videos()
        self.get_rt_videos()
        self.full_title_match()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
