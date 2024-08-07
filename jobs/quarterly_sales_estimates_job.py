import numpy as np
import pandas as pd
from base.etl_jobv3 import EtlJobV3


class QuarterlySalesEstimatesJob(EtlJobV3):

    def __init__(self, quarter_start, quarter_end, data_from_quarter, for_quarter, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='quarterly_sales_estimates', local_mode=True)
        """This job also writes to quarterly_sales_forecasts"""
        self.staging_schema = 'staging'
        self.prod_schema = 'warehouse'
        self.forecasts_table_name = 'quarterly_sales_forecasts'
        self.data_from_quarter = data_from_quarter
        self.for_quarter = for_quarter
        self.quarter_start = quarter_start
        self.quarter_end = quarter_end
        self.target_date = target_date
        self.EPISODES_LOOKBACK = 20
        self.MIN_DAYS_FOR_FORECAST = 7  # Give 7 days for a podcast to bake
        self.BENCHMARK_DAYS = 45
        self.episodes_needing_forecast = []
        self.episodes_not_needing_forecast = []
        self.episodes_with_forecasts = []
        self.benchmark_medians = {}
        self.episodes_with_forecasts_df = None
        self.episodes_not_needing_forecast_df = None
        self.episodes_df = None
        self.by_episode_df = None
        self.by_week_df = None
        self.final_dataframe = None


    def get_total_views_to_date(self):
        self.loggerv3.info('Getting total views to date')
        query = f"""
        SELECT
            series_title,
            episode_title,
            episode_id,
            platform,
            air_date,
            sum(views) as views,
            max(days_since_pub) as days_since_pub
        FROM warehouse.agg_daily_sales_metrics
        WHERE
            air_date >= '{self.quarter_start}'
            AND air_date < '{self.quarter_end}'
            AND days_since_pub <= {self.BENCHMARK_DAYS}
        GROUP BY 1, 2, 3, 4, 5
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            if result[6] >= self.BENCHMARK_DAYS:
                self.episodes_not_needing_forecast.append({
                    'series_title': result[0],
                    'episode_title': result[1],
                    'platform': result[3],
                    'air_date': result[4],
                    'views': result[5]
                })
            elif self.MIN_DAYS_FOR_FORECAST < result[6] < self.BENCHMARK_DAYS:
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
            for i in range(1, days_remaining+1):
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
                'episode_title': episode['episode_title'],
                'episode_id': episode['episode_id'],
                'platform': platform,
                'air_date': episode['air_date'],
                'views': original_views,
                'days_since_pub': original_days_since_pub,
                'forecasted_views': current_day_views
            })


    def truncate_forecasts_tables(self):
        self.loggerv3.info('Truncating forecasts table')
        self.db_connector.write_redshift(f"TRUNCATE {self.prod_schema}.{self.forecasts_table_name};")


    def write_forecast_to_redshift(self):
        self.loggerv3.info('Writing forecasts to Redshift')
        self.episodes_with_forecasts_df = pd.DataFrame(self.episodes_with_forecasts)
        self.db_connector.write_to_sql(self.episodes_with_forecasts_df, self.forecasts_table_name, self.db_connector.sv2_engine(), schema=self.prod_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.forecasts_table_name, self.prod_schema)


    def concat_dataframes(self):
        self.loggerv3.info('Concat dataframes')
        """Concat episodes that need forecasting with those that dont"""
        self.episodes_with_forecasts_df = self.episodes_with_forecasts_df[['series_title', 'episode_title', 'platform', 'air_date', 'forecasted_views']]
        self.episodes_with_forecasts_df = self.episodes_with_forecasts_df.rename(columns={'forecasted_views': 'views'})

        self.episodes_not_needing_forecast_df = pd.DataFrame(self.episodes_not_needing_forecast)
        self.episodes_df = pd.concat([self.episodes_with_forecasts_df, self.episodes_not_needing_forecast_df])


    def calculate_averages(self):
        self.loggerv3.info('Calculating averages')
        # Calculate Averages by Episode (Everything except Kinda Funny)
        self.by_episode_df = self.episodes_df[self.episodes_df['series_title'] != 'kinda funny']

        self.by_episode_df = self.by_episode_df.groupby(['series_title', 'platform']).agg(estimate=('views', np.median)).reset_index()

        # Calculate Averages by Week (Just Kinda Funny)
        self.by_week_df = self.episodes_df[self.episodes_df['series_title'] == 'kinda funny']

        self.by_week_df['air_date'] = pd.to_datetime(self.by_week_df['air_date'])
        self.by_week_df['air_week'] = self.by_week_df['air_date'].dt.isocalendar().week

        self.by_week_df = self.by_week_df.groupby(['series_title', 'platform', 'air_week']).agg(sum_views=('views', np.sum)).reset_index()

        self.by_week_df = self.by_week_df.groupby(['series_title', 'platform']).agg(estimate=('sum_views', np.median)).reset_index()


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.concat([
            self.by_episode_df[['series_title', 'platform', 'estimate']],
            self.by_week_df[['series_title', 'platform', 'estimate']]
        ])

        convert = {'estimate': int}
        self.final_dataframe = self.final_dataframe.astype(convert)
        self.final_dataframe['data_from_quarter'] = self.data_from_quarter
        self.final_dataframe['for_quarter'] = self.for_quarter
        self.final_dataframe = self.final_dataframe[['series_title', 'platform', 'data_from_quarter', 'estimate', 'for_quarter']]
        # One off for "just kidding news off the record"
        self.final_dataframe.loc[self.final_dataframe['series_title'] == 'just kidding news off the record', 'estimate'] *= 2


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to redshift staging')
        self.db_connector.write_to_sql(self.final_dataframe, f'stage_{self.table_name}', self.db_connector.sv2_engine(), schema=self.staging_schema, chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(f'stage_{self.table_name}', self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
                  BEGIN TRANSACTION;

                      UPDATE {self.prod_schema}.{self.table_name}
                      SET 
                          estimate = staging.estimate
                      FROM {self.staging_schema}.stage_{self.table_name} staging
                      JOIN {self.prod_schema}.{self.table_name} prod
                          ON staging.series_title = prod.series_title
                          AND staging.platform = prod.platform
                          AND staging.data_from_quarter = prod.data_from_quarter
                          AND staging.for_quarter = prod.for_quarter
                          AND staging.estimate != prod.estimate;

                      DELETE FROM {self.staging_schema}.stage_{self.table_name}
                      USING {self.prod_schema}.{self.table_name} prod
                      WHERE 
                          prod.series_title = stage_{self.table_name}.series_title
                          AND prod.platform = stage_{self.table_name}.platform
                          AND prod.data_from_quarter = stage_{self.table_name}.data_from_quarter
                          AND prod.for_quarter = stage_{self.table_name}.for_quarter;

                      INSERT INTO {self.prod_schema}.{self.table_name}
                      SELECT * FROM {self.staging_schema}.stage_{self.table_name};

                      TRUNCATE {self.staging_schema}.stage_{self.table_name};

                      COMMIT;

                  END TRANSACTION;
              """
        self.db_connector.write_redshift(query)


    def execute(self):
        self.loggerv3.start(f"Running Quarterly Sales Estimates Job for {self.quarter_start} to {self.quarter_end}")
        self.get_total_views_to_date()
        self.get_rate_of_change_medians()
        self.calculate_forecasts()
        self.truncate_forecasts_tables()
        self.write_forecast_to_redshift()
        self.concat_dataframes()
        self.calculate_averages()
        self.build_final_dataframe()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
