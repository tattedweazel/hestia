import pandas as pd
import config
import utils.components.yt_variables as sv
from datetime import datetime, timedelta
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3
from utils.components.dater import Dater


class WeeklyYTShortsViews:

    def __init__(self, run_date):
        """
        Run's the last 2 reporting weeks from the run_date.
        Reporting is from Sunday - Saturday inclusive.
        """
        self.table_name = 'weekly_yt_shorts_views'
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=config.local_mode)
        self.dater = Dater()
        self.run_date = run_date
        self.staging_schema = 'staging'
        self.prod_schema = 'warehouse'
        self.reporting_weeks = []
        self.shorts_views = []
        self.final_dataframe = None


    def set_reporting_weeks(self):
        self.loggerv3.info('Setting reporting weeks')
        # Get DOW index of run date
        run_date_dt = datetime.strptime(self.run_date, '%Y-%m-%d')
        # Identify last sunday from that run date
        last_sunday_dt = run_date_dt - timedelta(run_date_dt.weekday() + 1)
        # Previous Reporting Week 1
        self.reporting_weeks.append({
            'start_week': datetime.strftime(last_sunday_dt - timedelta(7), '%m/%d/%y'),
            'end_week': datetime.strftime(last_sunday_dt - timedelta(1), '%m/%d/%y'),
            'start_week_filter': datetime.strftime(last_sunday_dt - timedelta(7), '%Y%m%d'),
            'end_week_filter': datetime.strftime(last_sunday_dt - timedelta(0), '%Y%m%d')
        })
        # Previous Reporting Week 2
        self.reporting_weeks.append({
            'start_week': datetime.strftime(last_sunday_dt - timedelta(14), '%m/%d/%y'),
            'end_week': datetime.strftime(last_sunday_dt - timedelta(8), '%m/%d/%y'),
            'start_week_filter': datetime.strftime(last_sunday_dt - timedelta(14), '%Y%m%d'),
            'end_week_filter': datetime.strftime(last_sunday_dt - timedelta(7), '%Y%m%d')
        })


    def get_shorts_views(self):
        self.loggerv3.info('Getting shorts views')
        for reporting_week in self.reporting_weeks:
            query = f"""
            SELECT
                dim.channel_id,
                (CASE
                    WHEN dim.channel_title = 'F**KFACE' THEN 'F**kface'
                    ELSE dim.channel_title
                END),
                sum(coca2.views)
            FROM warehouse.content_owner_combined_a2 coca2
            LEFT JOIN warehouse.dim_yt_video_v2 dim on dim.video_id = coca2.video_id
            WHERE
                dim.duration_in_seconds <= 60
                AND coca2.start_date >= {reporting_week['start_week_filter']}
                AND coca2.start_date < {reporting_week['end_week_filter']}
                AND channel_title in {sv.channels}
            GROUP BY 1, 2;
            """
            results = self.db_connector.read_redshift(query)
            for result in results:
                self.shorts_views.append({
                    'channel_id': result[0],
                    'channel_title': result[1],
                    'start_week': reporting_week['start_week'],
                    'end_week': reporting_week['end_week'],
                    'views': result[2]
                })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.shorts_views)


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.db_connector.write_to_sql(self.final_dataframe, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
               BEGIN TRANSACTION;

                   UPDATE {self.prod_schema}.{self.table_name}
                   SET 
                       views =  staging.views
                   FROM {self.staging_schema}.stage_{self.table_name} staging
                   JOIN {self.prod_schema}.{self.table_name} prod
                       ON staging.channel_id = prod.channel_id
                       AND staging.end_week = prod.end_week;

                   DELETE FROM {self.staging_schema}.stage_{self.table_name}
                   USING {self.prod_schema}.{self.table_name} prod
                   WHERE 
                        prod.channel_id = stage_{self.table_name}.channel_id
                        AND prod.end_week = stage_{self.table_name}.end_week;

                   INSERT INTO {self.prod_schema}.{self.table_name}
                   SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                   TRUNCATE {self.staging_schema}.stage_{self.table_name};

                   COMMIT;

               END TRANSACTION;
           """
        self.db_connector.write_redshift(query)



    def execute(self):
        self.loggerv3.start(f"Running Weekly YT Shorts Views for run date {self.run_date}")
        self.set_reporting_weeks()
        self.get_shorts_views()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
