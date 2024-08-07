from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd


class AggQuarterlyPodcastDownloadsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector)
        self.target_date = target_date
        self.staging_schema = 'staging'
        self.staging_table = 'stage_agg_quarterly_podcast_downloads'
        self.prod_schema = 'warehouse'
        self.prod_table = 'agg_quarterly_podcast_downloads'
        self.podcast_downloads = []
        self.final_dataframe = None


    def get_quarterly_downloads(self):
        self.loggerv3.info('Getting quarterly downloads')
        query = f"""
        WITH max_air_date_cte as (
            SELECT cast(max(air_date) as timestamp) + 1 as max_air_date
            FROM warehouse.agg_daily_podcast_metrics
            WHERE air_date < '{self.target_date}'
        ), start_date_cte as (
            SELECT
                (CASE
                    WHEN max_air_date < concat(concat(date_part('year', '{self.target_date}'), '-04'), '-01') THEN concat(concat(date_part('year', '{self.target_date}'), '-01'), '-01')
                    WHEN max_air_date < concat(concat(date_part('year', '{self.target_date}'), '-07'), '-01') THEN concat(concat(date_part('year', '{self.target_date}'), '-04'), '-01')
                    WHEN max_air_date < concat(concat(date_part('year', '{self.target_date}'), '-10'), '-01') THEN concat(concat(date_part('year', '{self.target_date}'), '-07'), '-01')
                    ELSE concat(concat(date_part('year', '{self.target_date}'), '-10'), '-01')
                END) as start_date
            FROM max_air_date_cte
        ), downloads_cte as (
            SELECT
                series_title,
                count(distinct date_part('week', cast(air_date as timestamp))) as episode_weeks,
                sum(views) as downloads
            FROM warehouse.agg_daily_podcast_metrics
            WHERE
                air_date >= (SELECT start_date FROM start_date_cte)
                AND air_date < (SELECT max_air_date FROM max_air_date_cte)
                AND early_access = false
                AND date_diff('days', cast(air_date as date), cast(viewership_date as date)) <= 45
            GROUP BY 1
        ), targets_cte as (
            SELECT
                series_title,
                count(*) as target_weeks,
                sum(target) as target
            FROM warehouse.podcast_targets
            WHERE
                start_week >= (SELECT start_date FROM start_date_cte)
                AND start_week < (SELECT max_air_date FROM max_air_date_cte)
            GROUP BY 1
        )
        SELECT 
            (SELECT start_date FROM start_date_cte) as quarter,
            d.series_title,
            d.downloads,
            t.target,
            least(d.episode_weeks, t.target_weeks),
            t.target_weeks
        FROM downloads_cte d
        INNER JOIN targets_cte t on t.series_title = d.series_title;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.podcast_downloads.append({
                'quarter': result[0],
                'series_title': result[1],
                'downloads': int(result[2]),
                'target': int(result[3]),
                'episode_weeks': int(result[4]),
                'target_weeks': int(result[5])
            })


    def write_to_redshift_staging(self):
        self.loggerv3.info('Writing to Redshift staging')
        self.final_dataframe = pd.DataFrame(self.podcast_downloads)
        self.db_connector.write_to_sql(self.final_dataframe, self.staging_table, self.db_connector.sv2_engine(), schema=self.staging_schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.staging_table, self.staging_schema)


    def merge_stage_to_prod(self):
        self.loggerv3.info('Merging staging to prod')
        query = f"""
            BEGIN TRANSACTION;

                UPDATE {self.prod_schema}.{self.prod_table}
                SET 
                    downloads = staging.downloads,
                    target = staging.target,
                    episode_weeks = staging.episode_weeks,
                    target_weeks = staging.target_weeks
                FROM {self.staging_schema}.{self.staging_table} staging
                JOIN {self.prod_schema}.{self.prod_table} prod
                    ON staging.quarter = prod.quarter
                    AND staging.series_title = prod.series_title;

                DELETE FROM {self.staging_schema}.{self.staging_table}
                USING {self.prod_schema}.{self.prod_table} prod
                WHERE 
                    prod.quarter = {self.staging_table}.quarter
                    AND prod.series_title = {self.staging_table}.series_title;

                INSERT INTO {self.prod_schema}.{self.prod_table}
                SELECT * FROM {self.staging_schema}.{self.staging_table};

                TRUNCATE {self.staging_schema}.{self.staging_table};

                COMMIT;

            END TRANSACTION;
        """
        self.db_connector.write_redshift(query)


    def execute(self):
        self.loggerv3.start(f"Running Agg Quarterly Podcast Downloads Job for {self.target_date}")
        self.get_quarterly_downloads()
        self.write_to_redshift_staging()
        self.merge_stage_to_prod()
        self.loggerv3.success("All Processing Complete!")
