from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd


class PodcastAveragesJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='podcast_averages')
        self.target_date = target_date
        self.prod_schema = 'warehouse'
        self.BENCHMARK_DAYS_90 = 90
        self.BENCHMARK_DAYS_45 = 45
        self.latest_podcast_avgs = []
        self.podcasts_avgs = []
        self.podcast_air_weeks = []
        self.averages_df = None
        self.podcast_air_weeks_df = None
        self.latest_podcast_avgs_df = None
        self.podcasts_avgs_df = None
        self.final_dataframe = None

        self.loggerv3.disable_alerting()

    # KEEP THIS COMMENTED OUT UNTIL WE WANT X DAY AVERAGES
    # def get_latest_podcast_averages(self):
    #     self.loggerv3.info('Getting latest podcast averages')
    #     query = f"""
    #     WITH cte as (
    #         SELECT
    #           series_title,
    #           episode_title,
    #           early_access,
    #           air_date,
    #           date_diff('days', cast(air_date as date), cast(viewership_date as date)) as days_since_pub,
    #           sum(views) as views
    #         FROM warehouse.agg_daily_podcast_metrics
    #         WHERE early_access = 'false'
    #         GROUP BY 1, 2, 3, 4, 5
    #     ), cte2 as (
    #         SELECT series_title,
    #                max(air_date) as max_air_date
    #         FROM cte
    #         WHERE date_diff('days', cast(air_date as date), cast('{self.target_date}' as date)) >= 1
    #         GROUP BY 1
    #     ), cte3 as (
    #         SELECT cte.series_title,
    #                cte.air_date,
    #                max(cte.days_since_pub) as max_days_since_pub
    #         FROM cte
    #                  INNER JOIN cte2 on cte2.series_title = cte.series_title and cte2.max_air_date = cte.air_date
    #         GROUP BY 1, 2
    #     ), cte4 as (
    #         SELECT cte.series_title,
    #                cte3.max_days_since_pub,
    #                cast(date_trunc('week', cast(cte.air_date as date)) as varchar(10)) as air_date,
    #                sum(views) as views
    #         FROM cte
    #                  INNER JOIN cte3 on cte.series_title = cte3.series_title and cte.days_since_pub <= cte3.max_days_since_pub
    #         WHERE
    #             cte3.max_days_since_pub <= 14
    #             AND cte.air_date >= '{self.target_date}' - ({self.BENCHMARK_DAYS_90} + 7)
    #             AND cte.air_date < '{self.target_date}' - 7
    #         GROUP BY 1, 2, 3
    #     )
    #     SELECT
    #         series_title,
    #         max_days_since_pub as avg_day_length,
    #         avg(views) as avg_views
    #     FROM cte4
    #     GROUP BY 1, 2;
    #     """
    #     results = self.db_connector.read_redshift(query)
    #     for result in results:
    #         self.latest_podcast_avgs.append({
    #             'series_title': result[0],
    #             'avg_day_length': result[1],
    #             'avg_views': result[2]
    #         })


    def get_podcast_averages(self):
        self.loggerv3.info('Getting expected viewership completion')
        query = f"""
        WITH cte_90 as (
            SELECT
                series_title,
                cast(date_trunc('week', cast(air_date as date)) as varchar(10)) as air_date,
                sum(views) as views
            FROM warehouse.agg_daily_podcast_metrics
            WHERE
                early_access = 'false'
                AND air_date >= '{self.target_date}' - ({self.BENCHMARK_DAYS_90} + 14)
                AND air_date < '{self.target_date}' - 14
            GROUP BY 1, 2
        ), cte_45 as (
            SELECT
                series_title,
                cast(date_trunc('week', cast(air_date as date)) as varchar(10)) as air_date,
                sum(views) as views
            FROM warehouse.agg_daily_podcast_metrics
            WHERE
                early_access = 'false'
                AND air_date >= '{self.target_date}' - ({self.BENCHMARK_DAYS_45} + 14)
                AND air_date < '{self.target_date}' - 14
            GROUP BY 1, 2
        )
        SELECT
            series_title,
            {self.BENCHMARK_DAYS_90} as avg_day_length,
            avg(views) as avg_views
        FROM cte_90
        GROUP BY 1, 2
        UNION
        SELECT
            series_title,
            {self.BENCHMARK_DAYS_45} as avg_day_length,
            avg(views) as avg_views
        FROM cte_45
        GROUP BY 1, 2
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.podcasts_avgs.append({
                'series_title': result[0],
                'avg_day_length': result[1],
                'avg_views': result[2]
            })


    def get_pub_weeks_per_series(self):
        self.loggerv3.info('Getting published weeks per series')
        query = f"""
        SELECT
            series_title,
            cast(date_trunc('week', cast(air_date as date)) as varchar(10)) as air_weeks
        FROM warehouse.agg_daily_podcast_metrics
        WHERE
            early_access = 'false'
            AND air_date >= '{self.target_date}' - {self.BENCHMARK_DAYS_90}
        GROUP BY 1, 2;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.podcast_air_weeks.append({
                'series_title': result[0],
                'air_week': result[1]
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.podcast_air_weeks_df = pd.DataFrame(self.podcast_air_weeks)
        # self.latest_podcast_avgs_df = pd.DataFrame(self.latest_podcast_avgs)
        self.podcasts_avgs_df = pd.DataFrame(self.podcasts_avgs)

        # df1 = self.podcast_air_weeks_df.merge(self.latest_podcast_avgs_df, how='left', on='series_title')
        self.final_dataframe = self.podcast_air_weeks_df.merge(self.podcasts_avgs_df, how='left', on='series_title')

        # self.final_dataframe = pd.concat([df1, df2])
        self.final_dataframe = self.final_dataframe[~self.final_dataframe['avg_views'].isna()]
        convert = {'avg_day_length': int, 'avg_views': int}
        self.final_dataframe = self.final_dataframe.astype(convert)


    def truncate_table(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE warehouse.{self.table_name}")


    def write_to_redshift(self):
        self.loggerv3.info('Writing to Redshift')
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', chunksize=5000, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Podcast Averages Job")
        # self.get_latest_podcast_averages()
        self.get_podcast_averages()
        self.get_pub_weeks_per_series()
        self.build_final_dataframe()
        self.truncate_table()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
