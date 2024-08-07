from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd


class AggDailyPlatformPodcastMetricsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_podcast_metrics')
        self.target_date = target_date
        self.prod_schema = 'warehouse'
        self.staging_schema = 'staging'
        self.platform_podcasts = []
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


    def get_platform_data(self):
        self.loggerv3.info('Getting platform data')
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
                 'd&d, but...', 'always open'
                 )
            AND episode_title NOT IN (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes)    
        GROUP BY 1, 2, 3, 4;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.platform_podcasts.append({
                'series_title': self.series_mapper(result[0]),
                'series_clean': self.series_mapper(result[0]),
                'early_access': False,
                'episode_title': result[1],
                'viewership_date': result[2],
                'air_date': result[3],
                'platform': 'rooster_teeth',
                'views': result[4]
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.platform_podcasts)


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
        self.loggerv3.start(f"Running Agg Daily Platform Podcast Metrics Job for {self.target_date}")
        self.get_platform_data()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
