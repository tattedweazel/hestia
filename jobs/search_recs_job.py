import json
import os
from base.etl_jobv3 import EtlJobV3
from utils.connectors.s3_api_connector import S3ApiConnector


class SearchRecsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector)
        self.run_date = target_date
        self.rt_bucket = 'rt-popularity'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.output_location = self.file_location + 'uploads'
        self.LOOKBACK_DAYS = 30  # TODO: Determine this number
        self.RECORD_LIMIT = 40
        self.episode_records = []
        self.series_records = []
        self.premium_only_episodes = []
        self.premium_episodes = []
        self.free_episodes = []
        self.premium_series = []
        self.free_series = []


    def get_episode_searches(self):
        self.loggerv3.info("Getting episode searches")
        query = f"""
        SELECT
            (CASE
                WHEN se.user_tier in ('premium', 'trial') THEN 'premium'
                WHEN se.user_tier in ('free', 'anon') THEN 'free'
                ELSE 'other'
            END) as user_tier,
            dse.episode_title,
            se.target_id,
            count(se.*)
        FROM warehouse.search_event se
        INNER JOIN warehouse.dim_segment_episode dse on dse.episode_key = se.target_id
        WHERE
            timestamp < '{self.run_date}' + 1
            AND timestamp > '{self.run_date}' - {self.LOOKBACK_DAYS}
            AND target_type in ('episodes', 'episode')
            AND user_tier != 'other'
        GROUP BY 1, 2, 3
        ORDER BY 4 desc;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.episode_records.append({
                "run_date": self.run_date,
                "user_tier": result[0],
                "episode_title": result[1],
                "uuid": result[2],
                "count": result[3]
            })


    def get_series_searches(self):
        self.loggerv3.info("Getting series searches")
        query = f"""
        WITH series_cte as (
            SELECT series_id, series_title
            FROM warehouse.dim_segment_episode
            GROUP BY 1, 2
        )
        SELECT
            (CASE
                WHEN se.user_tier in ('premium', 'trial') THEN 'premium'
                WHEN se.user_tier in ('free', 'anon') THEN 'free'
                ELSE 'other'
            END) as user_tier,
            scte.series_title,
            se.target_id,
            count(se.*)
        FROM warehouse.search_event se
        INNER JOIN series_cte scte on scte.series_id = se.target_id
        WHERE
            timestamp < '{self.run_date}' + 1
            AND timestamp > '{self.run_date}'  - {self.LOOKBACK_DAYS}
            AND target_type in ('series', 'show')
            AND user_tier != 'other'
        GROUP BY 1, 2, 3
        ORDER BY 4 desc;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.series_records.append({
                "run_date": self.run_date,
                "user_tier": result[0],
                "series_title": result[1],
                "uuid": result[2],
                "count": result[3]
            })


    # def get_svod_be_premium_episodes(self):
    #     self.loggerv3.info('Getting svod-be premium episodes')
    #     query = """
    #         SELECT e.uuid
    #         FROM episodes e
    #         WHERE
    #             e.published_at < now() AND
    #             (
    #                 e.is_sponsors_only = 1 OR
    #                 e.public_golive_at > now()
    #             );
    #     """
    #     results = self.db_connector.query_svod_be_db_connection(query)
    #     for result in results:
    #         self.premium_only_episodes.append(result[0])


    def limit_filter_episodes(self):
        self.loggerv3.info('Limiting and filtering episodes')

        premium_episode_counter = 1
        free_episode_counter = 1
        for record in self.episode_records:
            if record['user_tier'] == 'premium':
                if premium_episode_counter <= self.RECORD_LIMIT:
                    record['rank'] = premium_episode_counter
                    del record['count']
                    self.premium_episodes.append(record)
                    premium_episode_counter += 1
            elif record['user_tier'] == 'free':
                if free_episode_counter <= self.RECORD_LIMIT:
                    record['rank'] = free_episode_counter
                    del record['count']
                    self.free_episodes.append(record)
                    free_episode_counter += 1


    def limit_filter_series(self):
        self.loggerv3.info('Limiting and filtering series')

        premium_series_counter = 1
        free_series_counter = 1
        for record in self.series_records:
            if record['user_tier'] == 'premium':
                if premium_series_counter <= self.RECORD_LIMIT:
                    record['rank'] = premium_series_counter
                    del record['count']
                    self.premium_series.append(record)
                    premium_series_counter += 1
            elif record['user_tier'] == 'free':
                if free_series_counter <= self.RECORD_LIMIT:
                    record['rank'] = free_series_counter
                    del record['count']
                    self.free_series.append(record)
                    free_series_counter += 1


    def write_all_results_to_json(self):
        self.loggerv3.info(f"Writing all results to json")
        # Writing Premium Episodes
        with open(f"{self.output_location}/premium_top_search_episodes.json", 'w') as f:
            f.write(json.dumps(self.premium_episodes))

        # Writing Free Episodes
        with open(f"{self.output_location}/free_top_search_episodes.json", 'w') as f:
            f.write(json.dumps(self.free_episodes))

        # Writing Premium Series
        with open(f"{self.output_location}/premium_top_search_series.json", 'w') as f:
            f.write(json.dumps(self.premium_series))

        # Writing Free Series
        with open(f"{self.output_location}/free_top_search_series.json", 'w') as f:
            f.write(json.dumps(self.free_series))


    def upload_output_to_s3(self):
        self.loggerv3.info("Uploading to S3")
        # Uploading Premium Episodes
        self.s3_api_connector.upload_file(f"{self.output_location}/premium_top_search_episodes.json", "premium_top_search_episodes.json")

        # Uploading Free Episodes
        self.s3_api_connector.upload_file(f"{self.output_location}/free_top_search_episodes.json", "free_top_search_episodes.json")

        # Uploading Premium Series
        self.s3_api_connector.upload_file(f"{self.output_location}/premium_top_search_series.json", "premium_top_search_series.json")

        # Uploading Free Series
        self.s3_api_connector.upload_file(f"{self.output_location}/free_top_search_series.json", "free_top_search_series.json")


    def cleanup(self):
        cmd = f"rm {self.output_location}/*.*"
        os.system(cmd)


    def execute(self):
        self.loggerv3.start(f"Running Search Recs Job for {self.run_date}")
        self.get_episode_searches()
        self.get_series_searches()
        self.limit_filter_episodes()
        self.limit_filter_series()
        self.write_all_results_to_json()
        self.upload_output_to_s3()
        self.cleanup()
        self.loggerv3.success("All Processing Complete!")
