from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd


class SalesMetricsWeeklyAverageJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='sales_metrics_weekly_averages', local_mode=True)
        self.target_date = target_date
        self.WEEKS = 15  # Note that 15 doesn't include the final week, which gives us 16 weeks
        self.BENCHMARK_DAYS = 45
        self.metrics = []
        self.final_dataframe = None


    def get_weekly_averages(self):
        self.loggerv3.info('Getting weekly averages')
        query = f"""
        WITH weekly_cte as (
            SELECT
                series_title,
                date_part('week', cast(air_date as date)) as air_week,
                sum(views) as views
            FROM warehouse.agg_daily_sales_metrics
            WHERE air_date >= date_add('week', -{self.WEEKS}, current_date)
                AND days_since_pub <= {self.BENCHMARK_DAYS}
            GROUP BY 1, 2
        ), weekly_average_cte as (
            SELECT series_title,
                   avg(views) as weekly_average
            FROM weekly_cte
            GROUP BY 1
        ), totals_cte as (
            SELECT
                series_title,
                date_part('week', cast(viewership_date as date)) as air_week,
                sum(views) as views
            FROM warehouse.agg_daily_sales_metrics
            WHERE viewership_date >= date_add('week', -{self.WEEKS}, current_date)
            GROUP BY 1, 2
        ), totals_average_cte as (
            SELECT series_title,
                   avg(views) as total_average
            FROM totals_cte
            GROUP BY 1
        )
        SELECT coalesce(wac.series_title, tac.series_title),
           wac.weekly_average,
           tac.total_average
        FROM weekly_average_cte wac
        FULL OUTER JOIN totals_average_cte tac on wac.series_title = tac.series_title;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.metrics.append({
                'series_title': result[0],
                'weekly_average': result[1],
                'total_average': result[2]
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.metrics)


    def truncate_table(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE warehouse.{self.table_name}")


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Sales Metrics Weekly Average Job for {self.target_date}")
        self.get_weekly_averages()
        self.build_final_dataframe()
        self.truncate_table()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
