import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime


class SiteSeriesTrajectoryJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
        super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'site_series_trajectory')

        self.target_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
        self.final_df = None


    def load_data(self):
        self.loggerv3.info("Running Query")
        query = f""" 
                WITH base_cte as (
                    SELECT
                        dse.channel_title,
                        dse.series_title,
                        vv.start_timestamp,
                        vv.user_key
                    FROM warehouse.vod_viewership vv
                    INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                    WHERE
                        vv.user_key is NOT NULL
                        AND vv.user_tier NOT in ('anon', 'grant')
                        AND vv.start_timestamp >= date_add('days', -121, '{self.target_date}')
                        AND dse.air_date >= date_add('days', -121, '{self.target_date}')
                ), last_week as (
                    SELECT
                    channel_title,
                    series_title,
                    count(distinct user_key) as views
                    FROM base_cte
                    WHERE start_timestamp >= date_add('days', -8, '{self.target_date}')
                    GROUP BY 1,2
                ), one_month_ago as (
                    SELECT
                    channel_title,
                    series_title,
                    count(distinct user_key) as views
                    FROM base_cte
                    WHERE start_timestamp >= date_add('days', -31, '{self.target_date}')
                    GROUP BY 1,2
                ), two_months_ago as (
                    SELECT
                    channel_title,
                    series_title,
                    count(distinct user_key) as views
                    FROM base_cte
                    WHERE start_timestamp >= date_add('days', -61, '{self.target_date}') and start_timestamp < date_add('days', -31, '{self.target_date}')
                    GROUP BY 1,2
                ), three_months_ago as (
                    SELECT
                    channel_title,
                    series_title,
                    count(distinct user_key) as views
                    FROM base_cte
                    where start_timestamp >= date_add('days', -91, '{self.target_date}') and start_timestamp < date_add('days', -61, '{self.target_date}')
                    GROUP BY 1,2
                ), four_months_ago as (
                    SELECT
                    channel_title,
                    series_title,
                    count(distinct user_key) as views
                    FROM base_cte
                    WHERE start_timestamp >= date_add('days', -121, '{self.target_date}') and start_timestamp < date_add('days', -91, '{self.target_date}')
                    GROUP BY 1,2
                )
                SELECT
                    lw.channel_title as "Channel Title",
                    lw.series_title as "Series Title",
                    lm.views as "UVs - Last Month",
                    round( ((lm.views * 1.0) / ((tma.views + thma.views + fma.views) / 3)) - 1, 2) as "90-day Trajectory"
                FROM last_week lw
                LEFT JOIN one_month_ago lm on lw.series_title = lm.series_title
                LEFT JOIN two_months_ago tma on lw.series_title = tma.series_title
                LEFT JOIN three_months_ago thma on lw.series_title = thma.series_title
                LEFT JOIN four_months_ago fma on lw.series_title = fma.series_title;
            """

        results = self.db_connector.read_redshift(query)
        records = []
        for result in results:
            records.append({
                "run_date": self.target_date,
                "channel_title": result[0],
                "series_title": result[1],
                "views_last_month": result[2],
                "trajectory": result[3]
            })
        self.final_df = pd.DataFrame(records)


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing to Redshift...")
        self.db_connector.write_to_sql(self.final_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.info(f"Running Site Series Trajectory Job for {self.target_date}")
        self.load_data()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
