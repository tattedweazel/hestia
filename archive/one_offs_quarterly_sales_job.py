import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class OneOffsQuarterlySalesJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_quarterly_sales_metrics', local_mode=True)
        self.schema = 'warehouse'
        self.run_date = target_date
        self.iilluminaughtii_tubular_podcasts = []
        self.iilluminaughtii_final_dataframe = None


    def iilluminaughtii(self):
        self.loggerv3.info('One off for iilluminaughtii')
        """Megaphone titles episodes correctly. YouTube does not"""
        query = f"""
        WITH megaphone_cte as (
            SELECT (CASE
                        WHEN lower(title) LIKE '%%dark dives%%' THEN 'iilluminaughtii dark dives'
                        WHEN lower(title) LIKE '%%corporate_casket%%' THEN 'iilluminaughtii corporate casket'
                        WHEN lower(title) LIKE '%%multi level mondays%%' OR lower(title) LIKE '%%multi-level-mondays%%' THEN 'iilluminaughtii multi level mondays'
                        ELSE 'none'
                END) as series_title,
                   (CASE
                        WHEN lower(title) LIKE '%%dark dives%%' THEN REPLACE(title, ' | Dark Dives', '')
                        WHEN lower(title) LIKE '%%corporate_casket%%' THEN REPLACE(title, ' | Corporate Casket', '')
                        WHEN lower(title) LIKE '%%multi level mondays%%' THEN REPLACE(REPLACE(title, ' | Multi Level Mondays', ''), ' | Multi-Level-Mondays', '')
                        ELSE title
                       END) as episode_title
            FROM warehouse.dim_megaphone_episode
            WHERE lower(podcast_title) = 'iilluminaughtii'
              AND series_title != 'none'
        ), tubular_cte as (
            SELECT
                cte.series_title,
                dim.video_title as episode_title,
                dim.duration_seconds,
                cast(date_add('days', m.date_value, cast(dim.date_of_upload as date)) as varchar(10)) as viewership_date,
                cast(dim.date_of_upload as varchar(10)) as air_date,
                m.views
            FROM warehouse.quarterly_sales_tubular_metrics m
                LEFT JOIN warehouse.dim_tubular_videos dim on dim.video_id = m.video_id
                LEFT JOIN megaphone_cte cte on cte.episode_title = dim.video_title
            WHERE m.agg_type = 'daily'
              AND dim.upload_creator = 'iilluminaughtii'
              AND dim.duration_seconds > 120
        )
        SELECT
            series_title,
            episode_title,
            viewership_date,
            air_date,
            sum(views) as views
        FROM tubular_cte
        WHERE series_title is not null
        GROUP BY 1, 2, 3, 4;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            days_since_pub = (datetime.strptime(result[2], '%Y-%m-%d') - datetime.strptime(result[3], '%Y-%m-%d')).days
            if days_since_pub <= 45:
                self.iilluminaughtii_tubular_podcasts.append({
                    'series_title': result[0],
                    'episode_title': result[1],
                    'viewership_date': result[2],
                    'air_date': result[3],
                    'platform': 'youtube',
                    'days_since_pub': days_since_pub,
                    'views': result[4]
                })


    def write_iilluminaughtii_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.iilluminaughtii_final_dataframe = pd.DataFrame(self.iilluminaughtii_tubular_podcasts)
        self.iilluminaughtii_final_dataframe['run_date'] = self.run_date
        self.db_connector.write_to_sql(self.iilluminaughtii_final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def execute(self):
        self.loggerv3.start(f"Running One Offs Quarterly Sales Job for Run Date {self.run_date}")
        self.iilluminaughtii()
        self.write_iilluminaughtii_to_redshift()
        self.loggerv3.success("All Processing Complete!")
