import pandas as pd
from base.etl_jobv3 import EtlJobV3


class AggWeeklySiteAppsViewership(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, file_location=''):
        super().__init__(jobname = __name__, db_connector = db_connector, table_name = 'agg_weekly_site_apps_viewership', local_mode=True)
        self.start_week = target_date
        self.schema = 'warehouse'
        self.viewership_days = 10
        self.air_date_days = 7
        self.episode_metrics = []
        self.final_dataframe = None


    def get_viewership_metrics(self):
        self.loggerv3.info('Get Viewership Metrics')
        query = f"""
        WITH base_viewership as (
            SELECT
                dse.channel_title,
                (CASE
                   WHEN vv.user_key is null then vv.anonymous_id
                   ELSE cast(vv.user_key as varchar)
                END) as user_key,
                vv.user_tier,
                dse.episode_key,
                dse.episode_title,
                dse.series_title,
                dse.air_date,
                dse.length_in_seconds  as episode_length,
                sum(vv.active_seconds) as seconds_watched,
                (CASE
                    WHEN sum(vv.active_seconds) >= dse.length_in_seconds THEN 1
                    ELSE sum(vv.active_seconds) / (1.0 * dse.length_in_seconds)
                END) as pct_consumed
            FROM warehouse.vod_viewership vv
            INNER JOIN warehouse.dim_segment_episode dse ON dse.episode_key = vv.episode_key
            WHERE
              dse.channel_title NOT IN ('Friends of RT', 'The Yogscast', 'Kinda Funny')
              AND vv.start_timestamp >= '{self.start_week}'
              AND vv.start_timestamp < date_add('days', {self.viewership_days}, '{self.start_week}')
              AND dse.air_date >= '{self.start_week}'
              AND dse.air_date < date_add('days', {self.air_date_days}, '{self.start_week}')
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ), completion as (
            SELECT
                user_key,
                user_tier,
                episode_key,
                episode_title,
                series_title,
                channel_title,
                air_date,
                (CASE
                    WHEN user_key IN (
                        SELECT
                            cast(user_key as varchar)
                        FROM warehouse.vod_viewership
                        WHERE start_timestamp < date_add('days', -{self.air_date_days}, '{self.start_week}')
                    ) THEN 0
                    ELSE 1
                END) as is_new,
                (CASE
                   WHEN user_tier = 'premium' THEN 1
                   ELSE 0
                END) as is_premium,
                pct_consumed
            FROM base_viewership
        ), agg as (
            SELECT
                c.channel_title,
                c.series_title,
                c.episode_title,
                c.episode_key,
                c.air_date,
                avg(c.pct_consumed) as avg_pct_consumed,
                count(distinct c.user_key) as viewers,
                sum(c.is_new) as new_viewers,
                sum(c.is_premium) as premium_viewers
            FROM completion c
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT
            channel_title,
            series_title,
            episode_title,
            episode_key,
            air_date,
            viewers,
            round(((new_viewers * 1.0) / viewers), 4) as pct_new_viewers,
            round(((premium_viewers * 1.0) / viewers), 4) as pct_premium_viewers,
            round(avg_pct_consumed,4) as avg_pct_consumed
        FROM agg;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.episode_metrics.append({
                'week_start': self.start_week,
                'channel_title': result[0],
                'series_title': result[1],
                'episode_title': result[2],
                'episode_key': result[3],
                'air_date': result[4],
                'viewers': result[5],
                'pct_new_viewers': result[6],
                'pct_premium_viewers': result[7],
                'avg_pct_consumed': result[8]
                })

        self.final_dataframe = pd.DataFrame(self.episode_metrics)


    def write_all_results_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


    def execute(self):
        self.loggerv3.start(f"Running Agg Weekly Site Apps Viewership Job for start week {self.start_week}")
        self.get_viewership_metrics()
        self.write_all_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
