from base.etl_jobv3 import EtlJobV3
from utils.components.sql_helper import SqlHelper
from pandas import pandas as pd


class AggDailyTubularPodcastMetricsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_podcast_metrics')
        self.target_date = target_date
        self.sql_helper = SqlHelper()
        self.prod_schema = 'warehouse'
        self.staging_schema = 'staging'
        self.tubular_podcasts = []
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


    def get_tubular_data(self):
        self.loggerv3.info('Getting tubular data')
        add_fh_pod_episodes = ["What''s Got Us So Pissed Off Now?! - Funhaus Fan Q & A", 'Tales From the Funhaus Pool Party!', 'Are You Ready for More Funhaus Fantasy Roleplaying Games?']
        add_fh_pod_episodes_str = self.sql_helper.array_to_sql_list(add_fh_pod_episodes)
        query = f"""
        WITH cte as (
            SELECT
                (CASE
                    WHEN lower(dim.video_title) like '%%- off topic -%%' THEN 'off topic'
                    WHEN lower(dim.video_title) like '%%rt podcast%%' THEN 'rooster teeth podcast'
                    WHEN (lower(dim.video_title) like '%%funhaus podcast%%' or dim.video_title in ({add_fh_pod_episodes_str})) THEN 'funhaus podcast'
                    WHEN (lower(dim.upload_creator) = 'f**kface' AND dim.video_title like '%%//%%' AND lower(dim.video_title) not like '%%f**kface breaks shit%%') THEN 'f**kface'
                    WHEN (lower(dim.upload_creator) = 'black box down' AND lower(dim.video_title) not like '%%black box down explained%%') THEN 'black box down'
                    WHEN (lower(dim.upload_creator) = 'face jam' AND lower(dim.video_title) not like '%%face jam shorts%%' AND lower(dim.video_title) not like '%%face jam live shopping event%%' AND lower(dim.video_title) not like '%%truck’d up!%%') THEN 'face jam'
                    WHEN (lower(dim.upload_creator) = 'red web' AND lower(dim.video_title) like '%%|%%' AND lower(dim.video_title) not like '%%red web case files%%') THEN 'red web'
                    WHEN lower(dim.video_title) like '%%death battle cast%%' THEN 'death battle cast'
                    WHEN (lower(dim.upload_creator) = 'annual pass' AND lower(dim.video_title) like '%%annual pass podcast%%') THEN 'annual pass'
                    WHEN ((lower(dim.video_title) like '%%paradise path rpg%%' or lower(dim.video_title) like '%%super princess rescue quest%%' or lower(dim.video_title) like '%%must be dice%%' or lower(dim.video_title) like '%%forgotten planet rpg%%') and lower(dim.video_title) not like '%%community haus party%%') THEN 'must be dice'
                    WHEN lower(dim.video_title) like '%%d&d, but%%' THEN 'd&d, but...'
                    WHEN lower(dim.upload_creator) = 'all good no worries' and lower(dim.video_title) like '%%always open%%' THEN 'always open'
                    WHEN lower(dim.video_title) like '%%ship hits the fan podcast%%' THEN 'ship hits the fan podcast'
                    WHEN lower(dim.upload_creator) = 'tales from the stinky dragon' and dim.video_title like '%%Ep%%' THEN 'tales from the stinky dragon'
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
                 AND dim.duration_seconds > 120
                 
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


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.tubular_podcasts)


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
        self.loggerv3.start(f"Running Agg Daily Tubular Podcast Metrics Job for {self.target_date}")
        self.get_tubular_data()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
