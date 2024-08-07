import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class AggDailyRoosterteethQuarterlySalesJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_quarterly_sales_metrics', local_mode=True)
        self.schema = 'warehouse'
        self.run_date = target_date
        self.platform_podcasts = []
        self.final_dataframe = None


    def get_platform_data(self):
        self.loggerv3.info('Getting platform data')
        query = f"""
        WITH cte as (
            SELECT
                (CASE
                    WHEN lower(dse.series_title) = 'always open' THEN 'always open'
                    WHEN lower(dse.series_title) = 'please be nice to me' THEN 'please be nice to me' 
                    WHEN lower(dse.series_title) = 'let''s play gmod' THEN 'ah gmod gameplay'
                    WHEN ((lower(dse.series_title) = 'let''s play' AND episode_title LIKE '%%VR%%') OR (lower(dse.series_title)= 'let''s play minecraft' AND episode_title LIKE '%%VR%%')) AND date_part('dow', dse.air_date) = 0 THEN 'ah vr mondays'
                    WHEN lower(dse.series_title) = 'let''s play' AND date_part('dow', air_date) = 6 THEN 'ah fan favorites sundays'
                    WHEN lower(dse.series_title) = 'challenge accepted' THEN 'challenge accepted' 
                    WHEN lower(dse.series_title) = 'f**kface' THEN 'f**kface'
                    WHEN lower(dse.series_title) = 'does it do?' THEN 'does it do' 
                    WHEN lower(dse.series_title) = 'let''s roll' THEN 'lets roll'
                    WHEN lower(dse.series_title) = 'red web' THEN 'red web'
                    WHEN lower(dse.series_title) = 'face jam' THEN 'face jam'
                    WHEN lower(dse.series_title) = 'off topic' THEN 'off topic'
                    WHEN lower(dse.series_title) = 'annual pass' THEN 'annual pass'
                    WHEN lower(dse.series_title) = 'rooster teeth podcast' THEN 'rooster teeth podcast'
                    WHEN lower(dse.series_title) = 'black box down' THEN 'black box down'
                    WHEN lower(dse.series_title) = 'tales from the stinky dragon' THEN 'tales from the stinky dragon'
                    WHEN lower(dse.series_title) = 'anma podcast' THEN 'anma'
                    WHEN lower(dse.channel_title) = 'funhaus' AND lower(dse.series_title) = 'gameplay' AND date_part('dow', air_date) = 0 THEN 'funhaus mondays'
                    WHEN lower(dse.channel_title) = 'funhaus' AND lower(dse.series_title) = 'gameplay' AND date_part('dow', air_date) in (6, 4) THEN 'funhaus sundays or fridays' 
                    WHEN lower(dse.series_title) = 'funhaus podcast' THEN 'funhaus podcast'
                    WHEN lower(dse.series_title) = 'ship hits the fan podcast' THEN 'ship hits the fan'
                    WHEN lower(dse.series_title) = '30 morbid minutes' THEN '30 morbid minutes'
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
                cast(start_timestamp as varchar(10)) as viewerhip_date,
                cast(dse.air_date as varchar(10)) as air_date,
                count(*) as views
            FROM warehouse.vod_viewership vv
            LEFT JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
            WHERE 
                dse.season_title is NOT NULL
                AND episode_title NOT IN (SELECT episode_title FROM warehouse.blacklisted_podcast_episodes)    
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            series_title,
            episode_title,
            viewerhip_date,
            air_date,
            views
        FROM cte
        WHERE series_title != 'none';
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            days_since_pub = (datetime.strptime(result[2], '%Y-%m-%d') - datetime.strptime(result[3], '%Y-%m-%d')).days
            if days_since_pub <= 45:
                self.platform_podcasts.append({
                    'series_title': result[0],
                    'episode_title': result[1],
                    'viewership_date': result[2],
                    'air_date': result[3],
                    'platform': 'roosterteeth',
                    'days_since_pub': days_since_pub,
                    'views': result[4]
                })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.platform_podcasts)
        self.final_dataframe['run_date'] = self.run_date


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def execute(self):
        self.loggerv3.start(f"Running Agg Daily Roosterteeth Quarterly Sales Job for Run Date {self.run_date}")
        self.get_platform_data()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
