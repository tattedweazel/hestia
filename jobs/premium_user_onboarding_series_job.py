import os
import json
import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.connectors.s3_api_connector import S3ApiConnector


class PremiumUserOnboardingSeriesJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='user_onboarding_series')
        self.schema = 'warehouse'
        self.EPISODE_LIMIT = 30
        self.target_date = target_date
        self.lookback_days = -28
        self.output_location = self.file_location + 'uploads'
        self.output_filename = 'featured_series_premium.json'
        self.rt_bucket = 'rt-popularity'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.premium_series = []
        self.final_series = []
        self.final_dataframe = None


    def get_premium_user_series(self):
        self.loggerv3.info('Getting premium user series')
        query = f"""
            WITH user_viewership as (
                SELECT
                    vv.user_key,
                    dse.channel_title,
                    dse.series_title,
                    dse.series_id,
                    dse.season_title,
                    dse.episode_title,
                    dse.episode_key,
                    dse.episode_number,
                    dse.length_in_seconds,
                    (CASE
                        WHEN sum(vv.active_seconds) > dse.length_in_seconds THEN dse.length_in_seconds
                        ELSE sum(vv.active_seconds)
                    END) as active_seconds
                FROM warehouse.vod_viewership vv
                         INNER JOIN warehouse.dim_segment_episode dse on vv.episode_key = dse.episode_key
                WHERE vv.user_tier in ('trial', 'premium')
                  AND vv.start_timestamp > dateadd('days', {self.lookback_days}, '{self.target_date}')
                  AND vv.start_timestamp < dateadd('days', 1, '{self.target_date}')
                  AND dse.length_in_seconds > 0
                  AND dse.channel_title NOT in ('Friends of RT', 'Kinda Funny', 'The Yogscast')
                GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
            ), vod_agg as (
                SELECT
                    channel_title,
                    series_title,
                    series_id,
                    season_title,
                    episode_title,
                    episode_key,
                    episode_number,
                    length_in_seconds,
                    sum(active_seconds) * 1.0 as vod_watched,
                    count(distinct user_key) as unique_viewers,
                    count(distinct user_key) * length_in_seconds as denom
                FROM user_viewership
                GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
            ), cmp1 as (
                SELECT
                    channel_title,
                    series_title,
                    series_id,
                    sum(vod_watched) as vod_watched,
                    sum(unique_viewers) as unique_viewers,
                    sum(denom) as denom
                FROM vod_agg
                GROUP BY 1, 2, 3
            ), cmp2 as (
                SELECT
                    channel_title,
                    series_title,
                    series_id,
                    vod_watched,
                    unique_viewers,
                    vod_watched / denom as completion_pct
                FROM cmp1
            ), agg as (
                SELECT
                    channel_title,
                    series_title,
                    series_id,
                    vod_watched,
                    unique_viewers,
                    completion_pct,
                    round(completion_pct * unique_viewers, 1)                    as weight,
                    rank() over (partition by series_title order by weight desc, unique_viewers desc) as rnk
                FROM cmp2
            )
            SELECT
                channel_title,
                series_title,
                series_id,
                vod_watched,
                unique_viewers,
                completion_pct,
                weight
            FROM agg
            WHERE rnk = 1
            ORDER BY weight desc, unique_viewers desc
            LIMIT {self.EPISODE_LIMIT};
        """

        results = self.db_connector.read_redshift(query)
        for result in results:
            self.premium_series.append({
                'channel_title': result[0],
                'series_title': result[1],
                'series_id': result[2],
                'vod_watched': int(result[3]),
                'unique_viewers': result[4],
                'completion_pct': float(result[5]),
                'weight': float(result[6])
            })


    def build_final_series(self):
        self.loggerv3.info('Building final series')
        rank = 1
        self.final_series = []
        for series in self.premium_series:
            series['rank'] = rank
            self.final_series.append(series)
            rank += 1

        if len(self.final_series) == 0:
            raise ValueError('Missing series data')


    def write_results_to_json(self):
        self.loggerv3.info('Write results to JSON')
        with open(f"{self.output_location}/{self.output_filename}", 'w') as f:
            f.write(json.dumps(self.final_series))


    def upload_output_to_s3(self):
        self.loggerv3.info("Uploading to S3")
        self.s3_api_connector.upload_file(f"{self.output_location}/{self.output_filename}", self.output_filename)


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        self.final_dataframe = pd.DataFrame(self.final_series)
        self.final_dataframe['run_date'] = self.target_date
        self.final_dataframe['tier'] = 'premium'


    def write_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def cleanup(self):
        cmd = f"rm {self.output_location}/*.*"
        os.system(cmd)


    def execute(self):
        self.loggerv3.start(f"Running Premium User Onboarding Series Job for {self.target_date}")
        self.get_premium_user_series()
        self.build_final_series()
        self.write_results_to_json()
        self.upload_output_to_s3()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.cleanup()
        self.loggerv3.success("All Processing Complete!")
