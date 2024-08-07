import json
import os
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta
from slugify import slugify
from utils.connectors.s3_api_connector import S3ApiConnector


class PopularJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector)
        self.target_date = target_date
        self.run_date = None
        self.lookback_days = 90
        self.RECORD_LIMIT = 30
        self.rt_bucket = 'rt-popularity'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.output_location = self.file_location + 'uploads'
        self.output_filename = 'show_data.json'
        self.records = []
        self.sorted_records = []
        self.channel_records = {}


    def set_dates(self):
        self.loggerv3.info("Setting dates")
        if self.target_date is None:
            self.target_date = datetime.now()
        else:
            self.target_date = datetime.strptime(self.target_date, '%Y-%m-%d')
        self.run_date = self.target_date - timedelta(days=self.lookback_days)
        self.loggerv3.info(f'Dates set for {self.run_date} - {self.target_date}')


    def get_views(self):
        self.loggerv3.info("Loading view data")
        query = f"""
            WITH unique_viewers as (
                SELECT
                       dse.channel_title,
                       dse.series_title,
                       dse.series_id,
                       count(distinct (CASE
                            WHEN vv.user_key is NULL THEN vv.anonymous_id
                            ELSE CAST(vv.user_key as varchar)
                        END)) as univer_viewers,
                        count(distinct session_id) as views
                FROM warehouse.vod_viewership vv
                LEFT JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                WHERE
                    vv.start_timestamp > '{self.run_date}'
                    AND vv.start_timestamp < '{self.target_date}'
                    AND dse.length_in_seconds > 0
                    AND user_tier not in ('anon', 'grant', 'unknown')
                GROUP BY 1, 2, 3
            ), user_viewership as (
                SELECT
                    vv.user_key,
                    dse.channel_title,
                    dse.series_title,
                    dse.series_id,
                    dse.episode_title,
                    dse.episode_key,
                    dse.length_in_seconds,
                   (CASE
                        WHEN sum(vv.active_seconds) > dse.length_in_seconds THEN dse.length_in_seconds
                        ELSE sum(vv.active_seconds)
                       END) as active_seconds
                FROM warehouse.vod_viewership vv
                     LEFT JOIN warehouse.dim_segment_episode dse on dse.episode_key = vv.episode_key
                WHERE
                    vv.start_timestamp > '{self.run_date}'
                    AND vv.start_timestamp < '{self.target_date}'
                    AND user_tier not in ('anon', 'grant', 'unknown')
                    AND dse.length_in_seconds > 0
                GROUP BY 1, 2, 3, 4, 5, 6, 7
            ), cmp as (
                SELECT
                    channel_title,
                    series_title,
                    series_id,
                    episode_key,
                    length_in_seconds,
                    sum(active_seconds) * 1.0                    as vod_watched,
                    count(user_key) * length_in_seconds          as denominator
                FROM user_viewership
                GROUP BY 1, 2, 3, 4, 5
            ), cmp_agg as (
                SELECT
                       channel_title,
                       series_title,
                       series_id,
                       round(sum(vod_watched) / sum(denominator), 3) as avg_consumption
                FROM cmp
                GROUP BY 1, 2, 3
            )
            SELECT
                uv.channel_title,
                uv.series_title,
                uv.series_id,
                uv.univer_viewers,
                uv.views,
                c.avg_consumption
            FROM unique_viewers uv
            INNER JOIN cmp_agg c on c.series_id = uv.series_id
            ORDER BY 5 desc;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            viewers = result[3]
            avg_consumption = float(result[5])
            modifier = f"{len(str(viewers))}.{viewers}"
            weight = float(modifier) * (1 + avg_consumption)
            record = {
                "run_date": datetime.strftime(self.target_date, '%Y-%m-%d'),
                "channel": result[0],
                "series": result[1],
                "uuid": result[2],
                "viewers": viewers,
                "views": result[4],
                "avg_consumption": avg_consumption,
                "weight": float(weight)
            }
            self.records.append(record)


    def sort_records(self):
        self.loggerv3.info('Sort records')
        self.sorted_records = sorted(self.records, key=lambda i: i['weight'], reverse=True)
        for record in self.sorted_records:
            record['weight'] = "%.4f" % record['weight']


    def hydrate_channel_records(self):
        self.loggerv3.info('Hydrating channel records')
        for record in self.sorted_records:
            channel = record['channel']
            if channel not in self.channel_records:
                self.channel_records[channel] = [record]
            else:
                self.channel_records[channel].append(record)


    def limit_records(self):
        self.loggerv3.info('Limit records')
        self.sorted_records = self.sorted_records[:self.RECORD_LIMIT]

        for channel in self.channel_records:
            self.channel_records[channel] = self.channel_records[channel][:self.RECORD_LIMIT]


    def write_all_results_to_json(self):
        self.loggerv3.info(f"Creating output JSON: {self.output_filename}")
        with open(f"{self.output_location}/{self.output_filename}", 'w') as f:
            f.write(json.dumps(self.sorted_records))

        for channel in self.channel_records:
            output_filename = f"{slugify(channel)}_{self.output_filename}"
            self.loggerv3.info(f"Creating output JSON: {output_filename}")
            with open(f"{self.output_location}/{output_filename}", 'w') as f:
                f.write(json.dumps(self.channel_records[channel]))


    def upload_output_to_s3(self):
        self.loggerv3.info("Uploading to S3")
        self.s3_api_connector.upload_file(f"{self.output_location}/{self.output_filename}", self.output_filename)

        for channel in self.channel_records:
            output_filename = f"{slugify(channel)}_{self.output_filename}"
            self.s3_api_connector.upload_file(f"{self.output_location}/{output_filename}", output_filename)


    def cleanup(self):
        cmd = f"rm {self.output_location}/*.*"
        os.system(cmd)


    def execute(self):
        self.loggerv3.start("Running Popular Job")
        self.set_dates()
        self.get_views()
        self.sort_records()
        self.hydrate_channel_records()
        self.limit_records()
        self.write_all_results_to_json()
        self.upload_output_to_s3()
        self.cleanup()
        self.loggerv3.success("All Processing Complete!")
