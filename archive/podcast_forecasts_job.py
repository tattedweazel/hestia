from base.etl_jobv3 import EtlJobV3
from time import sleep
from pandas import pandas as pd


class PodcastForecastsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='podcast_forecasts')
        self.target_date = target_date
        self.prod_schema = 'warehouse'
        self.MIN_DAYS_FOR_FORECAST = 7  # Give 7 days for a podcast to bake
        self.BENCHMARK_DAYS = 45
        self.podcast_totals = []
        self.expected_viewership = []
        self.podcast_totals_df = None
        self.expected_viewership_df = None
        self.joined_df = None
        self.final_dataframe = None


    def get_total_views_to_date(self):
        self.loggerv3.info('Getting total views to date')
        """Get all series episodes by platform that can have a projection, select their views to date and latest viewership days since pub"""
        query = f"""
        WITH cte as (
            SELECT
                series_title,
                episode_title,
                viewership_date,
                air_date,
                date_diff('days', cast(air_date as date), cast(viewership_date as date)) as days_since_pub,
                platform,
                views
            FROM warehouse.agg_daily_podcast_metrics
            WHERE
                early_access = 'false'
                AND air_date >= date_add('days', -{self.BENCHMARK_DAYS}, '{self.target_date}')
        )
        SELECT
            series_title,
            episode_title,
            platform,
            air_date,
            sum(views) as views_to_date,
            max(days_since_pub) as days_since_pub
        FROM cte
        GROUP BY 1, 2, 3, 4;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.podcast_totals.append({
                'series_title': result[0],
                'episode_title': result[1],
                'platform': result[2],
                'air_date': result[3],
                'views_to_date': result[4],
                'days_since_pub': result[5]
            })


    def get_expected_viewership_completion(self):
        self.loggerv3.info('Getting expected viewership completion')
        """
        Forecast = (X Day Projection) - (Views to Date)
        X Day Projection = (Views to Date) / % Completed
        % Completed = (Median, cumm views of last 10 episodes at viewership day X) /  (Median, cumm views of last 10 episodes at day last day 45)
        Why last 10? I dont know. This is arbitrary and can be changed
        """
        query = f"""
        WITH rnk_cte  as (
            SELECT
                series_title,
                episode_title,
                platform,
                air_date,
                rank() over (partition by series_title, platform order by air_date desc) as rnk
            FROM warehouse.agg_daily_podcast_metrics
            WHERE early_access = 'false'
            GROUP BY 1, 2, 3, 4
        ), last_10 as (
            SELECT *
            FROM rnk_cte
            WHERE rnk <= 10
        ), cumm_views_by_days_since_pub as (
            SELECT
                series_title,
                episode_title,
                platform,
                viewership_date,
                air_date,
                date_diff('days', cast(air_date as date), cast(viewership_date as date)) as days_since_pub,
                views,
                sum(views) over (partition by episode_title, platform order by viewership_date rows unbounded preceding) as cumm_views
            FROM warehouse.agg_daily_podcast_metrics
            WHERE
                early_access = 'false'
                and days_since_pub <= {self.BENCHMARK_DAYS}
            ORDER BY series_title, viewership_date
        ), median_by_viewership_day as (
            SELECT
                c.series_title,
                c.platform,
                c.days_since_pub,
                median(cumm_views) as med_cumm_views
            FROM cumm_views_by_days_since_pub c
            LEFT JOIN last_10 l on l.episode_title = c.episode_title and c.platform = l.platform
            WHERE l.platform is NOT NULL
            GROUP BY 1, 2, 3
        ), final_day_medians as (
            SELECT
                series_title,
                platform,
                med_cumm_views
            FROM median_by_viewership_day
            WHERE days_since_pub = {self.BENCHMARK_DAYS}
        )
        SELECT m.series_title,
               m.platform,
               m.days_since_pub,
               m.med_cumm_views as med_views,
               d.med_cumm_views as final_day_med_views,
               (CASE
                   WHEN  m.days_since_pub < {self.MIN_DAYS_FOR_FORECAST} OR m.days_since_pub > {self.BENCHMARK_DAYS} THEN 1.0
                   WHEN (m.med_cumm_views / d.med_cumm_views) > 1 THEN 1.0
                   ELSE round(m.med_cumm_views / d.med_cumm_views, 4)
                END) as pct_completed
        FROM median_by_viewership_day m
        LEFT JOIN final_day_medians d on d.series_title = m.series_title and d.platform = m.platform;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.expected_viewership.append({
                'series_title': result[0],
                'platform': result[1],
                'days_since_pub': result[2],
                'med_views': result[3],
                'final_day_med_views': result[4],
                'pct_completed':  result[5]
            })


    def calculate_forecasts(self):
        self.loggerv3.info('Calculating forecasts')
        self.podcast_totals_df = pd.DataFrame(self.podcast_totals)
        self.expected_viewership_df = pd.DataFrame(self.expected_viewership)

        self.joined_df = pd.merge(self.podcast_totals_df, self.expected_viewership_df, on=['series_title', 'platform', 'days_since_pub'], how='left')
        self.joined_df['projection'] = self.joined_df['views_to_date'] / self.joined_df['pct_completed']
        self.joined_df['forecast'] = self.joined_df['projection'] - self.joined_df['views_to_date']



    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = self.joined_df[['series_title', 'episode_title', 'platform', 'air_date', 'days_since_pub', 'forecast']]
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
        self.loggerv3.start(f"Running Podcast Forecasts Job for {self.target_date}")
        sleep(10)  # This job reads from tables that were updated by previous job
        self.get_total_views_to_date()
        self.get_expected_viewership_completion()
        self.calculate_forecasts()
        self.build_final_dataframe()
        self.truncate_table()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
