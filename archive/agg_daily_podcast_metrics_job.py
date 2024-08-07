from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd


class AggDailyPodcastMetricsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_podcast_metrics')
        self.target_date = target_date
        self.min_date = None
        self.prod_schema = 'warehouse'
        self.staging_schema = 'staging'
        self.tubular_podcasts = []
        self.on_platform_podcasts = []
        self.megaphone_podcasts = []
        self.supporting_cast_podcasts = []
        self.final_dataframe = None


    def series_mapper(self, series):
        mapper = {
            '- Off Topic -': 'off topic',
            'RT Podcast': 'rooster teeth podcast',
            'Ship Hits The Fan': 'ship hits the fan podcast',
            'ANMA': 'anma podcast'
        }
        if series in mapper:
            return mapper[series]
        return series.lower()


    def early_access_identifier(self, series):
        if '(FIRST Member Early Access)' in series:
            return True
        return False


    def get_tubular_data(self):
        self.loggerv3.info('Getting tubular data')
        query = f"""
        WITH cte as (
            SELECT
                (CASE
                    WHEN lower(dim.video_title) like '%%- off topic -%%' THEN 'off topic'
                    WHEN lower(dim.video_title) like '%%rt podcast%%' THEN 'rooster teeth podcast'
                    WHEN lower(dim.video_title) like '%%funhaus podcast%%' THEN 'funhaus podcast'
                    WHEN (lower(dim.upload_creator) = 'f**kface' AND dim.video_title like '%%//%%' AND lower(dim.video_title) not like '%%f**kface breaks shit%%') THEN 'f**kface'
                    WHEN (lower(dim.upload_creator) = 'black box down' AND lower(dim.video_title) like '%%black box down podcast%%') THEN 'black box down'
                    WHEN (lower(dim.upload_creator) = 'face jam' AND lower(dim.video_title) not like '%%face jam shorts%%' AND lower(dim.video_title) not like '%%face jam live shopping event%%') THEN 'face jam'
                    WHEN (lower(dim.upload_creator) = 'red web' AND lower(dim.video_title) like '%%|%%' AND lower(dim.video_title not like '%%red web case files%%')) THEN 'red web'
                    WHEN lower(dim.video_title) like '%%death battle cast%%' THEN 'death battle cast'
                    WHEN (lower(dim.upload_creator) = 'annual pass' AND lower(dim.video_title) like '%%annual pass podcast%%') THEN 'annual pass'
                    WHEN (lower(dim.video_title) like '%%paradise path rpg%%' and lower(dim.video_title) not like '%%community haus party%%') THEN 'must be dice'
                    WHEN lower(dim.video_title) like '%%d&d, but%%' THEN 'd&d, but...'
                    ELSE 'none'
                END) as series_title,
                video_title as episode_title,
                dim.duration_seconds,
                cast(date_add('days', m.date_value, cast(dim.date_of_upload as date)) as varchar(10)) as viewership_date,
                cast(dim.date_of_upload as varchar(10)) as air_date,
                m.views
            FROM warehouse.daily_tubular_metrics m
            LEFT JOIN warehouse.dim_tubular_videos dim on dim.video_id = m.video_id
            WHERE 
                 m.run_date = '{self.target_date}'
                 AND m.agg_type = 'daily'
                 AND dim.video_title not like '%%!PODCAST!%%'
        )
        SELECT
            series_title,
            episode_title,
            viewership_date,
            air_date,
            sum(views) as views
        FROM cte
        WHERE
            series_title != 'none'
            AND episode_title not in (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes) 
        GROUP BY 1, 2, 3, 4;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.tubular_podcasts.append({
                'series_title': self.series_mapper(result[0]),
                'series_clean': self.series_mapper(result[0]),
                'early_access': False,
                'episode_title': result[1],
                'viewership_date': result[2],
                'air_date': result[3],
                'platform': 'youtube',
                'views': result[4]
            })


    def get_on_platform_data(self):
        self.loggerv3.info('Getting on platform data')
        query = f"""
        SELECT
            dse.series_title,
            dse.episode_title,
            cast(start_timestamp as varchar(10)) as viewerhip_date,
            cast(dse.air_date as varchar(10)) as air_date,
            count(*) as views
        FROM warehouse.vod_viewership vv
        LEFT JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
        WHERE 
            dse.season_title is NOT NULL
            AND lower(dse.series_title) in (
                'off topic', 'rooster teeth podcast', 'funhaus podcast', 'f**kface', 'black box down', 
                'face jam', 'red web', 'death battle cast', 'annual pass', 'tales from the stinky dragon',
                 'ot3 podcast', 'ship hits the fan podcast', 'must be dice', '30 morbid minutes', 'anma podcast',
                 'd&d, but...'
                 )
            AND episode_title NOT IN (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes)    
        GROUP BY 1, 2, 3, 4;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.on_platform_podcasts.append({
                'series_title': self.series_mapper(result[0]),
                'series_clean': self.series_mapper(result[0]),
                'early_access': False,
                'episode_title': result[1],
                'viewership_date': result[2],
                'air_date': result[3],
                'platform': 'rooster_teeth',
                'views': result[4]
            })


    def get_megaphone_data(self):
        self.loggerv3.info('Getting megaphone data')
        query = f"""
        WITH cte as (
           SELECT
                me.podcast_title as series_title,
                mp.clean_title as series_clean,
                me.title as episode_title,
                convert_timezone('CST', min(cast(me.pub_date as timestamp))) as air_date
            FROM warehouse.megaphone_metrics mm
            LEFT JOIN warehouse.dim_megaphone_podcast mp ON mm.podcast_id = mp.id
            LEFT JOIN warehouse.dim_megaphone_episode me ON mm.episode_id = me.id
            WHERE lower(mp.clean_title) IN (
                'off topic', 'rooster teeth podcast', 'funhaus podcast', 'f**kface', 'black box down',
                'face jam', 'red web', 'death battle cast', 'annual pass', 'tales from the stinky dragon',
                 'ot3 podcast', 'ship hits the fan', 'must be dice', '30 morbid minutes', 'anma',
                 'd&d, but...'
                 )
            GROUP BY 1, 2, 3
        )
        SELECT
            me.podcast_title as series_title,
            mp.clean_title as series_clean,
            me.title as episode_title,
            (CASE
                WHEN mm.created_at < cte.air_date THEN cast(cte.air_date as varchar(10))
                ELSE cast(mm.created_at as varchar(10))
            END) as viewership_date,
            cast(cte.air_date as varchar(10)) as air_date,
            count(*) as views
        FROM warehouse.megaphone_metrics mm
        LEFT JOIN warehouse.dim_megaphone_podcast mp ON mm.podcast_id = mp.id
        LEFT JOIN warehouse.dim_megaphone_episode me ON mm.episode_id = me.id
        LEFT JOIN cte ON cte.episode_title = me.title AND cte.series_title = me.podcast_title
        WHERE
            mm.seconds_downloaded >= 30
            AND cte.air_date IS NOT NULL
            AND me.podcast_title IS NOT NULL
            AND episode_title NOT IN (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes)    
        GROUP BY 1, 2, 3, 4, 5;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.megaphone_podcasts.append({
                'series_title': self.series_mapper(result[0]),
                'series_clean': self.series_mapper(result[1]),
                'early_access': self.early_access_identifier(result[0]),
                'episode_title': result[2],
                'viewership_date': result[3],
                'air_date': result[4],
                'platform': 'megaphone',
                'views': result[5]
            })


    # Leave commented out until we turn on Supporting Cast
    # def get_supporting_cast_data(self):
    #     self.loggerv3.info('Getting supporting cast data')
    #     query = f"""
    #     WITH cte as (
    #         SELECT
    #             podcast as series_title,
    #             podcast_clean as series_clean,
    #             episode_title,
    #             convert_timezone('CST', min(cast(air_date as timestamp))) as air_date
    #         FROM warehouse.supporting_cast_episodes
    #         GROUP BY 1, 2, 3
    #     )
    #     SELECT
    #         sce.podcast as series_title,
    #         sce.podcast_clean as series_clean,
    #         sce.episode_title,
    #         cast(sce.download_datetime as varchar(10)) as viewership_date,
    #         cast(cte.air_date as date) as air_date,
    #         count(*) as views
    #     FROM warehouse.supporting_cast_episodes sce
    #     LEFT JOIN cte ON cte.episode_title = sce.episode_title AND cte.series_title = sce.podcast
    #     WHERE
    #         cte.air_date >= '{self.min_date}'
    #         AND cte.air_date < '{self.target_date}'
    #         AND viewership_date < '{self.target_date}'
    #         AND lower(series_title) in (
    #             'off topic', 'rooster teeth podcast', 'funhaus podcast', 'f**kface', 'black box down',
    #             'face jam', 'red web', 'death battle cast', 'annual pass', 'tales from the stinky dragon',
    #              'ot3 podcast', 'ship hits the fan', 'must be dice', '30 morbid minutes', 'anma'
    #              )
    #         AND sce.episode_title not in (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes)
    #     GROUP BY 1, 2, 3, 4, 5;
    #     """
    #     results = self.db_connector.read_redshift(query)
    #     for result in results:
    #         self.supporting_cast_podcasts.append({
    #             'series_title': self.series_mapper(result[0]),
    #             'series_clean': self.series_mapper(result[1]),
    #             'early_access': self.early_access_identifier(result[0]),
    #             'episode_title': result[2],
    #             'viewership_date': result[3],
    #             'air_date': result[4],
    #             'platform': 'supporting_cast',
    #             'views': result[5]
    #         })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        tubular_podcasts_df = pd.DataFrame(self.tubular_podcasts)
        on_platform_podcasts_df = pd.DataFrame(self.on_platform_podcasts)
        megaphone_podcasts_df = pd.DataFrame(self.megaphone_podcasts)
        self.final_dataframe = pd.concat([tubular_podcasts_df, on_platform_podcasts_df, megaphone_podcasts_df])


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(self.final_dataframe, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
               BEGIN TRANSACTION;

                   UPDATE {self.prod_schema}.{self.table_name}
                   SET 
                       views = staging.views,
                       air_date = staging.air_date
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.series_title = prod.series_title
                       AND staging.episode_title = prod.episode_title
                       AND staging.viewership_date = prod.viewership_date
                       AND staging.platform = prod.platform
                       AND staging.views != prod.views;

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                       prod.series_title = stage_{self.table_name}.series_title
                       AND prod.series_clean = stage_{self.table_name}.series_clean
                       AND prod.early_access = stage_{self.table_name}.early_access
                       AND prod.episode_title = stage_{self.table_name}.episode_title
                       AND prod.viewership_date = stage_{self.table_name}.viewership_date
                       AND prod.platform = stage_{self.table_name}.platform
                       AND prod.views = stage_{self.table_name}.views;

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)


    def execute(self):
        self.loggerv3.start(f"Running Agg Daily Podcast Metrics Job for {self.target_date}")
        self.get_tubular_data()
        self.get_on_platform_data()
        self.get_megaphone_data()
        # self.get_supporting_cast_data()  # Leave commented out until we turn on Supporting Cast
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
