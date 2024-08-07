import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class RoosterteethSalesMetricsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_sales_metrics', local_mode=True)
        self.staging_schema = 'staging'
        self.prod_schema = 'warehouse'
        self.run_date = target_date
        self.LOOKBACK = 30
        self.platform_podcasts = []
        self.final_dataframe = None


    def get_platform_data(self):
        self.loggerv3.info('Getting platform data')
        query = f"""
        WITH cte as (
            SELECT
                (CASE
                    WHEN lower(dse.series_title) = 'hypothetical nonsense' THEN 'hypothetical nonsense'
                    WHEN lower(dse.series_title) = 'always open' THEN 'always open'
                    WHEN lower(dse.series_title) = 'please be nice to me' THEN 'please be nice to me'
                    WHEN lower(dse.series_title) = 'let''s play gmod' THEN 'ah gmod gameplay'
                    WHEN lower(dse.series_title) = 'let''s play' AND lower(episode_title) LIKE '%%regulation gameplay%%' THEN 'f**kface regulation gameplay'
                    WHEN lower(dse.series_title) = 'let''s play' AND date_part('dow', air_date) = 6 THEN 'ah fan favorites sundays'
                    WHEN lower(dse.series_title) = 'challenge accepted' THEN 'challenge accepted' 
                    WHEN lower(dse.series_title) = 'f**kface' THEN 'f**kface'
                    WHEN lower(dse.series_title) = 'does it do?' THEN 'does it do' 
                    WHEN lower(dse.series_title) = 'black box down' THEN 'black box down'
                    WHEN lower(dse.series_title) = 'let''s roll' THEN 'lets roll'
                    WHEN lower(dse.series_title) = 'red web' AND lower(dse.episode_title) NOT LIKE '%%case files%%' THEN 'red web'
                    WHEN lower(dse.series_title) = 'face jam' AND (lower(dse.episode_title) LIKE '%%spittin silly%%' OR dse.length_in_seconds >= 3000) THEN 'face jam'
                    WHEN lower(dse.series_title) = 'off topic' THEN 'off topic'
                    WHEN lower(dse.series_title) = 'annual pass' THEN 'annual pass'
                    WHEN lower(dse.series_title) = 'rooster teeth podcast' AND dse.length_in_seconds >= 3000 THEN 'rooster teeth podcast'
                    WHEN lower(dse.channel_title) = 'tales from the stinky dragon' and lower(dse.series_title) not in ('mini-series', 'stinky dragon adventures', 'stinky dragon shorts') AND lower(episode_title) NOT LIKE '%%[second wind]%%' THEN 'tales from the stinky dragon'
                    WHEN lower(dse.series_title) = 'anma podcast' THEN 'anma'
                    WHEN lower(dse.series_title) = 'so...alright' THEN 'so... alright'
                    WHEN lower(dse.channel_title) = 'funhaus' AND lower(dse.series_title) in ('gameplay', 'demo wheel', 'funhaus variety!') AND date_part('dow', air_date) in (0, 1) THEN 'funhaus mondays'
                    WHEN lower(dse.channel_title) = 'funhaus' AND lower(dse.series_title) in ('gameplay', 'demo wheel', 'funhaus variety!') AND date_part('dow', air_date) in (2, 3, 4, 5) THEN 'funhaus sundays or fridays' 
                    WHEN lower(dse.series_title) = 'funhaus podcast' THEN 'funhaus podcast'
                    WHEN lower(dse.series_title) = 'board as hell' THEN 'board as hell'
                    WHEN lower(dse.series_title) = 'ship hits the fan podcast' THEN 'ship hits the fan'
                    WHEN lower(dse.series_title) = '30 morbid minutes' AND dse.length_in_seconds > 1400 THEN '30 morbid minutes'
                    WHEN lower(dse.series_title) = 'must be dice' THEN 'must be dice'
                    WHEN lower(dse.series_title) = 'inside gaming roundup' THEN 'inside gaming roundup'  
                    WHEN lower(dse.series_title) = 'death battle cast' THEN 'death battle cast'
                    WHEN lower(dse.series_title) = 'death battle fight previews' THEN 'death battle preview'
                    WHEN lower(dse.series_title) = 'death battle!' THEN 'death battle'
                    WHEN lower(dse.series_title) = 'easy allies podcast' THEN 'easy allies'
                    WHEN lower(dse.series_title) = 'kinda funny in review' THEN 'kinda funny'
                    WHEN lower(dse.series_title) = 'kinda funny games daily' THEN 'kinda funny'
                    WHEN lower(dse.series_title) = 'kinda funny podcast' THEN 'kinda funny'
                    WHEN lower(dse.series_title) = 'kinda funny gamescast' THEN 'kinda funny'
                    WHEN lower(dse.series_title) = 'kinda funny xcast' THEN 'kinda funny'
                    WHEN lower(dse.series_title) = 'ps i love you xoxo' THEN 'kinda funny'
                    WHEN lower(dse.series_title) = 'recreyo' THEN 'recreyo'
                    ELSE 'none'
                END) as series_title,
                dse.episode_title,
                dse.episode_key as episode_id,
                cast(start_timestamp as varchar(10)) as viewerhip_date,
                cast(dse.air_date as varchar(10)) as air_date,
                count(*) as views
            FROM warehouse.vod_viewership vv
            LEFT JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
            WHERE 
                dse.season_title is NOT NULL
                AND episode_title NOT IN (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes)
                AND start_timestamp > current_date - {self.LOOKBACK}
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT
            episode_id,
            series_title,
            episode_title,
            viewerhip_date,
            air_date,
            views
        FROM cte
        WHERE series_title NOT IN ('none', 'death battle preview', 'death battle');
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            days_since_pub = (datetime.strptime(result[3], '%Y-%m-%d') - datetime.strptime(result[4], '%Y-%m-%d')).days
            self.platform_podcasts.append({
                'episode_id': result[0],
                'series_title': result[1],
                'episode_title': result[2],
                'viewership_date': result[3],
                'air_date': result[4],
                'platform': 'rooster_teeth',
                'days_since_pub': days_since_pub,
                'views': result[5]
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.platform_podcasts)
        self.final_dataframe['run_date'] = self.run_date


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
                       run_date = staging.run_date,
                       episode_title = staging.episode_title
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.episode_id = prod.episode_id
                       AND staging.days_since_pub = prod.days_since_pub
                    WHERE prod.platform = 'rooster_teeth';

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                       prod.episode_id = stage_{self.table_name}.episode_id
                       AND prod.days_since_pub = stage_{self.table_name}.days_since_pub
                       AND prod.platform = stage_{self.table_name}.platform;

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)


    def execute(self):
        self.loggerv3.start(f"Running Roosterteeth Sales Metrics Job on {self.run_date}")
        self.get_platform_data()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
