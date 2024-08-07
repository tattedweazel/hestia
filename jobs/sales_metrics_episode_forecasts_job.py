from base.etl_jobv3 import EtlJobV3
from time import sleep
from pandas import pandas as pd


class SalesMetricsEpisodeForecastsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='sales_metrics_episode_forecasts', local_mode=True)
        self.target_date = target_date
        self.prod_schema = 'warehouse'
        self.MIN_DAYS_FOR_FORECAST = 7  # Give 7 days for a podcast to bake
        self.BENCHMARK_DAYS = 45
        self.EPISODES_LOOKBACK = 20
        self.episodes_needing_forecast = []
        self.benchmark_medians = {}
        self.episodes_with_forecasts = []
        self.final_dataframe = None


    def get_total_views_to_date(self):
        self.loggerv3.info('Getting total views to date')
        query = f"""
        WITH cte as (
            SELECT
                series_title,
                episode_title,
                episode_id,
                platform,
                air_date,
                sum(views) as views,
                max(days_since_pub) as max_days_since_pub
            FROM warehouse.agg_daily_sales_metrics
            WHERE
                air_date >= date_add('days', -{self.BENCHMARK_DAYS}, '{self.target_date}')
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT
            series_title,
            episode_title,
            episode_id,
            platform,
            air_date,
            views,
            max_days_since_pub
        FROM cte
        WHERE max_days_since_pub > 1;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.episodes_needing_forecast.append({
                'series_title': result[0],
                'episode_title': result[1],
                'episode_id': result[2],
                'platform': result[3],
                'air_date': result[4],
                'views': result[5],
                'days_since_pub': result[6]
            })


    def get_rate_of_change_medians(self):
        self.loggerv3.info('Getting rate of change medians')
        query = f"""
           WITH rnk_cte  as (
               SELECT
                   series_title,
                   episode_title,
                   platform,
                   air_date,
                   ROW_NUMBER() over (partition by series_title, platform order by air_date desc) as rnk
               FROM warehouse.agg_daily_sales_metrics
               GROUP BY 1, 2, 3, 4
           ), last_x as (
               SELECT *
               FROM rnk_cte
               WHERE rnk <= {self.EPISODES_LOOKBACK}
           ), cumm_views_by_days_since_pub as (
               SELECT
                   series_title,
                   episode_title,
                   platform,
                   (CASE
                       WHEN days_since_pub < 1 THEN 1
                       ELSE days_since_pub
                   END) as days_since_pub,
                   views,
                   sum(views) over (partition by episode_title, platform order by days_since_pub rows unbounded preceding) as cumm_views
               FROM warehouse.agg_daily_sales_metrics
               WHERE days_since_pub <= {self.BENCHMARK_DAYS}
           ), lag_cte as (
               SELECT *,
                      lag(cumm_views, 1) over (order by series_title, episode_title, platform, days_since_pub) as prev_cumm_views
               FROM cumm_views_by_days_since_pub
           ), rate_of_change_cte as (
               SELECT *,
                      (views * 1.0) / prev_cumm_views as rate_of_change
               FROM lag_cte
               WHERE days_since_pub > 1
               ORDER BY series_title, episode_title, platform, days_since_pub
           ), median_by_days_since_pub as (
               SELECT
                   c.series_title,
                   c.platform,
                   c.days_since_pub,
                   median(rate_of_change) as med_rate_of_change
               FROM rate_of_change_cte c
               LEFT JOIN last_x l on l.series_title = c.series_title and l.episode_title = c.episode_title and l.platform = c.platform
               WHERE l.platform is NOT NULL
               GROUP BY 1, 2, 3
           )
           SELECT
               series_title,
               platform,
               days_since_pub,
               med_rate_of_change
           FROM median_by_days_since_pub
           ORDER BY series_title, platform, days_since_pub;
           """
        results = self.db_connector.read_redshift(query)
        for result in results:
            series_title = result[0]
            platform = result[1]
            days_since_pub = result[2]
            med_rate_of_change = result[3]

            if series_title not in self.benchmark_medians:
                self.benchmark_medians[series_title] = {platform: {days_since_pub: med_rate_of_change}}
            else:
                if platform not in self.benchmark_medians[series_title]:
                    self.benchmark_medians[series_title][platform] = {days_since_pub: med_rate_of_change}
                else:
                    self.benchmark_medians[series_title][platform][days_since_pub] = med_rate_of_change


    def calculate_forecasts(self):
        self.loggerv3.info('Calculating forecasts')
        for episode in self.episodes_needing_forecast:
            original_views = episode['views']
            original_days_since_pub = episode['days_since_pub']
            series_title = episode['series_title']
            platform = episode['platform']
            current_day_views = episode['views']
            current_days_since_pub = episode['days_since_pub']
            days_remaining = 45 - current_days_since_pub
            for i in range(1, days_remaining + 1):
                next_days_since_pub = current_days_since_pub + 1
                if next_days_since_pub in self.benchmark_medians[series_title][platform]:
                    rate_of_change = self.benchmark_medians[series_title][platform][next_days_since_pub]
                    daily_views = rate_of_change * current_day_views
                    current_day_views = daily_views + current_day_views
                    current_days_since_pub = next_days_since_pub
                else:
                    break

            self.episodes_with_forecasts.append({
                'series_title': series_title,
                'episode_id': episode['episode_id'],
                'episode_title': episode['episode_title'],
                'platform': platform,
                'air_date': episode['air_date'],
                'forecast': current_day_views - original_views
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.episodes_with_forecasts)
        self.final_dataframe = self.final_dataframe[self.final_dataframe['forecast'] > 0]
        convert = {'forecast': int}
        self.final_dataframe = self.final_dataframe.astype(convert)


    def truncate_table(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE warehouse.{self.table_name}")


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Sales Metrics Episode Forecasts Job for {self.target_date}")
        sleep(10)  # This job reads from tables that were updated by previous job
        self.get_total_views_to_date()
        self.get_rate_of_change_medians()
        self.calculate_forecasts()
        self.build_final_dataframe()
        self.truncate_table()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
