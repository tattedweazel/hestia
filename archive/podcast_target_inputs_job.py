import pandas as pd
import numpy as np
from base.etl_jobv3 import EtlJobV3


class PodcastTargetInputsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, local_mode=True)
        self.AIR_DATE_DAYS_THRESHOLD = 180
        self.megaphone_data = []
        self.vod_data = []
        self.megaphone_df = None
        self.megaphone_totals = None
        self.megaphone_pvt = None
        self.megaphone_final_dataframe = None
        self.vod_df = None
        self.vod_totals = None
        self.vod_pvt = None
        self.vod_final_dataframe = None
        self.megaphone_file_path = 'tools/output/NEW_megaphone_episode_downloads_since_publishing.csv'
        self.vod_file_path = 'tools/output/NEW_episode_views_since_publishing.csv'


    def get_megaphone_data(self):
        self.loggerv3.info("Getting Megaphone Data")
        query = f"""
        WITH cte as (
            SELECT
              DATEADD(h, 6, CAST(mm.created_at AS date)) as download_date,
              mm.podcast_id as podcast_id,
              mm.episode_id AS episode_id,
              me.podcast_title AS podcast,
              mp.author AS author,
              me.title AS episode_title,
              mp.clean_title as podcast_clean_title,
              mp.channel as channel,
              CAST(me.pub_date AS date) as episode_publish_date,
              me.duration AS ep_length,
              mm.seconds_downloaded AS watchtime_seconds,
              mm.normalized_user_agent AS platform,
              mm.city AS user_city,
              mm.country AS user_country,
              date_diff('days', cast(me.pub_date as date), cast(mm.created_at as date)) as days_since_pub
            FROM warehouse.megaphone_metrics mm
            LEFT JOIN warehouse.dim_megaphone_podcast mp ON mm.podcast_id = mp.id
            LEFT JOIN warehouse.dim_megaphone_episode me ON mm.episode_id = me.id
            WHERE
                mm.seconds_downloaded >= 30
                AND episode_publish_date >= '2022-09-15'
                AND mp.channel != 'rooster_teeth_premium'
                AND me.podcast_title NOT IN ('Big Mood', 'Big Question', 'Gen2Gen', 'Must Be Dice', 'We''re All Insane')
        ), agg as (
            SELECT episode_title,
                   podcast,
                   author,
                   episode_publish_date,
                   days_since_pub,
                   count(*) as downloads
            FROM cte
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT
            episode_title,
            podcast,
            episode_publish_date,
            (CASE
                WHEN days_since_pub <= 1 THEN 1
                ELSE days_since_pub
            END) as days_since_pub,
            sum(downloads) over (PARTITION BY episode_title order by days_since_pub rows unbounded preceding) as cumm_downloads
        FROM agg;
		"""
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.megaphone_data.append({
                'episode_title': result[0],
                'podcast': result[1],
                'episode_publish_date': result[2],
                'days_since_pub': result[3],
                'cumm_downloads': result[4]
            })


    def output_megaphone_data(self):
        self.loggerv3.info('Outputting megaphone data')
        index_columns = ['episode_title', 'podcast', 'episode_publish_date']

        self.megaphone_df = pd.DataFrame(self.megaphone_data)

        self.megaphone_totals = self.megaphone_df.groupby(index_columns).agg(Total=('cumm_downloads', np.max))

        self.megaphone_pvt = pd.pivot_table(self.megaphone_df.reset_index(),
                                  index=index_columns,
                                  columns = 'days_since_pub',
                                  values = 'cumm_downloads',
                                  aggfunc = 'sum').fillna(method='ffill', axis=1).reset_index()

        self.megaphone_pvt.fillna(0, inplace=True)

        self.megaphone_final_dataframe = self.megaphone_totals.merge(self.megaphone_pvt, how='left', on=index_columns)

        self.megaphone_final_dataframe.to_csv(self.megaphone_file_path, index=False)


    def get_vod_data(self):
        self.loggerv3.info('Getting VOD data')
        query = f"""
        WITH cte as (
            SELECT dse.episode_title,
                   dse.series_title,
                   dse.air_date,
                   dse.channel_title,
                   dse.season_number,
                   dse.episode_number,
                   date_diff('days', cast(dse.air_date as date), cast(vv.start_timestamp as date)) as days_since_pub
            FROM warehouse.vod_viewership vv
                     LEFT JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
            WHERE air_date >= '2022-09-15'
        ), agg as (
            SELECT
                episode_title,
                series_title,
                air_date,
                channel_title,
                season_number,
                episode_number,
                days_since_pub,
                count(*) as views
            FROM cte
            GROUP BY 1, 2, 3, 4, 5, 6, 7
        )
        SELECT
            episode_title,
            series_title,
            air_date,
            channel_title,
            season_number,
            episode_number,
            (CASE
                WHEN days_since_pub <= 1 THEN 1
                ELSE days_since_pub
            END) as days_since_pub,
            sum(views) over (PARTITION BY episode_title order by days_since_pub rows unbounded preceding) as cumm_views
        FROM agg;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.vod_data.append({
                'episode_title': result[0],
                'series_title': result[1],
                'air_date': result[2],
                'channel_title': result[3],
                'season_number': result[4],
                'episode_number': result[5],
                'days_since_pub': result[6],
                'cumm_views': result[7]
            })


    def output_vod_data(self):
        self.loggerv3.info('Outputting VOD data')
        index_columns = ['episode_title', 'series_title', 'air_date', 'channel_title', 'season_number', 'episode_number']

        self.vod_df = pd.DataFrame(self.vod_data)

        self.vod_totals = self.vod_df.groupby(index_columns).agg(Total=('cumm_views', np.max))

        self.vod_pvt = pd.pivot_table(self.vod_df.reset_index(),
                                  index=index_columns,
                                  columns = 'days_since_pub',
                                  values = 'cumm_views',
                                  aggfunc = 'sum').fillna(method='ffill', axis=1).reset_index()
        self.vod_pvt.fillna(0, inplace=True)

        self.vod_final_dataframe = self.vod_totals.merge(self.vod_pvt, how='left', on=index_columns)

        self.vod_final_dataframe.to_csv(self.vod_file_path, index=False)


    def execute(self):
        self.loggerv3.start(f"Running Podcast Target Inputs Job")
        self.get_megaphone_data()
        self.output_megaphone_data()
        # self.get_vod_data()
        # self.output_vod_data()
        self.loggerv3.success("All Processing Complete!")
