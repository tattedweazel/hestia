from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd


class HistoricalPodcastForecastsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='historical_podcast_forecasts')
        self.target_date = target_date
        self.schema = 'warehouse'
        self.BENCHMARK_DAYS = 4
        self.podcast_totals = []
        self.podcast_totals_df = None
        self.final_dataframe = None


    def get_forecasts(self):
        self.loggerv3.info('Getting forecasts')
        query = f"""
         WITH mx_air as (
              SELECT
                  platform,
                  series_title,
                  episode_title,
                  min(air_date) as air_date
              FROM warehouse.agg_daily_podcast_metrics
              GROUP BY 1, 2, 3
          ), cte as (
              SELECT
                  adpm.series_title,
                  cast(mx.air_date as timestamp) as air_date,
                  adpm.views
              FROM warehouse.agg_daily_podcast_metrics adpm
              INNER JOIN mx_air mx on mx.series_title = adpm.series_title and mx.episode_title = adpm.episode_title and mx.platform = adpm.platform
              WHERE adpm.early_access = 'false'
          ), cte2 as (
            SELECT
                series_title,
                cast(date_trunc('week', cast(air_date as date)) as varchar(10)) as air_week,
                sum(views) as views
            FROM cte
            GROUP BY 1, 2
          ), forecasts as(
              SELECT
                  series_title,
                  cast(date_trunc('week', cast(air_date as timestamp)) as varchar(10)) as air_week,
                  sum(forecast) as forecast
              FROM warehouse.podcast_forecasts
              WHERE air_week >= current_date - 60
              GROUP BY 1, 2
          )
          SELECT
            c.series_title,
            c.air_week,
            c.views as actual,
            f.forecast as forecast
          FROM cte2 c
          LEFT JOIN forecasts f on f.series_title = c.series_title and f.air_week = c.air_week
          WHERE f.forecast is NOT NULL
          ORDER BY series_title, air_week desc;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.podcast_totals.append({
                'run_date': self.target_date,
                'series_title': result[0],
                'air_date': result[1],
                'views': result[2],
                'forecast': result[3]
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.podcast_totals)


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, self.schema)


    def execute(self):
        self.loggerv3.start(f"Running Historical Podcast Forecasts Job for {self.target_date}")
        self.get_forecasts()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
