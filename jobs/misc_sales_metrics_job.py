import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class MiscSalesMetricsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_sales_metrics', local_mode=True)
        self.staging_schema = 'staging'
        self.prod_schema = 'warehouse'
        self.run_date = target_date
        self.metrics = []
        self.final_dataframe = None


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
                dim.video_id as episode_id,
                cte.series_title,
                dim.video_title as episode_title,
                dim.duration_seconds,
                cast(date_add('days', m.date_value, cast(dim.date_of_upload as date)) as varchar(10)) as viewership_date,
                cast(dim.date_of_upload as varchar(10)) as air_date,
                m.views
            FROM warehouse.daily_tubular_metrics_v2 m
                LEFT JOIN warehouse.dim_tubular_videos dim on dim.video_id = m.video_id
                LEFT JOIN megaphone_cte cte on cte.episode_title = dim.video_title
            WHERE m.agg_type = 'daily'
              AND dim.upload_creator = 'iilluminaughtii'
              AND dim.duration_seconds > 120
        )
        SELECT
            episode_id,
            series_title,
            episode_title,
            viewership_date,
            air_date,
            sum(views) as views
        FROM tubular_cte
        WHERE series_title is not null
        GROUP BY 1, 2, 3, 4, 5;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            days_since_pub = (datetime.strptime(result[3], '%Y-%m-%d') - datetime.strptime(result[4], '%Y-%m-%d')).days
            self.metrics.append({
                'episode_id': result[0],
                'series_title': result[1],
                'episode_title': result[2],
                'viewership_date': result[3],
                'air_date': result[4],
                'platform': 'youtube',
                'days_since_pub': days_since_pub,
                'views': result[5]
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.metrics)
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
                           AND staging.platform = prod.platform
                           AND staging.views != prod.views;

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
        self.loggerv3.start(f"Running Misc Sales Metrics Job on {self.run_date}")
        self.iilluminaughtii()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
