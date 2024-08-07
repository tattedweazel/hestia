import config
from base.etl_jobv3 import EtlJobV3
from utils.connectors.sns_api_connector import SNSApiConnector


class AnomalyDetectionJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector)
        self.target_date = target_date
        self.adt = AnomalyDetectionTables()
        self.sns_connector = SNSApiConnector(profile_name='roosterteeth', region_name='us-west-2', file_location=config.file_location)
        self.topic_arn = 'arn:aws:sns:us-west-2:928401392503:rt-data-anomalies'
        self.select_query = ""
        self.where_query = ""
        self.table_schemas = []
        self.volumetrics = []
        self.DAILY_AVERAGE_LOOKBACK = 61
        self.ANOMALY_THRESHOLD = .45  # As a percentage change relative to the average, the latest data should not exceed the absolute value of this


    def set_query(self, tbl):
        if 'time_series_data_type' in tbl and tbl['time_series_data_type'] == 'int':
            self.select_query += f"""cast(to_date({tbl['time_series_column']}, 'YYYYMMDD') as varchar(10)),"""
            self.select_query += "count(*) as cnt"
            self.where_query += f"""to_date({tbl['time_series_column']}, 'YYYYMMDD')"""
        else:
            self.select_query += f"""cast({tbl['time_series_column']} as varchar(10)),"""
            self.select_query += "count(*) as cnt"
            self.where_query += f"""{tbl['time_series_column']}"""


    def query_tables(self):
        for tbl in self.adt.tables:
            self.loggerv3.info(f"Querying Table {tbl['table_name']}")
            self.select_query = ""
            self.where_query = ""
            self.set_query(tbl)


            if 'platform_column' in tbl:
                query = f"""
                WITH lastX_days as (
                    SELECT
                        {tbl['platform_column']},
                        {self.select_query}
                    FROM {tbl['schema']}.{tbl['table_name']}
                    WHERE {self.where_query} >= current_date - {self.DAILY_AVERAGE_LOOKBACK}
                    GROUP BY 1, 2
                ), last_day as (
                    SELECT 
                        {tbl['platform_column']},
                        {self.select_query}
                    FROM {tbl['schema']}.{tbl['table_name']}
                    WHERE {self.where_query} >= current_date {tbl['run_date']}
                    GROUP BY 1, 2
                ), avg_cte as (
                    SELECT 
                        {tbl['platform_column']},
                        avg(cnt) as avg_cnt
                    FROM lastX_days
                    GROUP BY 1
                ), joined as (
                    SELECT 
                        ac.{tbl['platform_column']},
                        ac.avg_cnt,
                        ld.cnt as last_cnt
                    FROM avg_cte ac
                    LEFT JOIN last_day ld on ld.{tbl['platform_column']} = ac.{tbl['platform_column']}
                )
                SELECT
                    {tbl['platform_column']},
                    ((last_cnt*1.0 - avg_cnt) / avg_cnt) as avg_delta
                FROM joined;
                """
            else:
                query = f"""
                WITH lastX_days as (
                    SELECT {self.select_query}
                    FROM {tbl['schema']}.{tbl['table_name']}
                    WHERE {self.where_query} >= current_date - {self.DAILY_AVERAGE_LOOKBACK}
                    GROUP BY 1
                ), last_day as (
                    SELECT {self.select_query}
                    FROM {tbl['schema']}.{tbl['table_name']}
                    WHERE {self.where_query} >= current_date {tbl['run_date']}
                    GROUP BY 1
                ), avg_cte as (
                    SELECT avg(cnt) as avg_cnt
                    FROM lastX_days
                ), joined as (
                    SELECT avg_cnt,
                           (SELECT cnt FROM last_day) as last_cnt
                    FROM avg_cte
                )
                SELECT
                    'None' as platform,
                    ((last_cnt*1.0 - avg_cnt) / avg_cnt) as avg_delta
                FROM joined;
                """
            results = self.db_connector.read_redshift(query)
            for result in results:
                self.volumetrics.append({
                    'schema': tbl['schema'],
                    'table_name': tbl['table_name'],
                    'platform': result[0] if result[0] is not None else None,
                    'avg_delta': round(result[1], 2) if result[1] is not None else None,

                })


    def detect_anomalies(self):
        self.loggerv3.info('Detecting Anomalies')
        for vol in self.volumetrics:
            message_title = 'DATA ANOMALY DETECTED'
            try:
                if vol['avg_delta'] is None or abs(vol['avg_delta']) >= self.ANOMALY_THRESHOLD:
                    raise Exception
                else:
                    if vol['platform'] != 'None':
                        self.loggerv3.info(f"No anomalies for {vol['table_name']} on {vol['platform']}")
                    else:
                        self.loggerv3.info(f"No anomalies for {vol['table_name']}")
            except Exception:
                if vol['platform'] is not None:
                    message = f"Anomaly detected for {vol['schema']}.{vol['table_name']} on {vol['platform']}."
                else:
                    message = f"Anomaly detected for {vol['schema']}.{vol['table_name']}."
                message += "\nLatest data is "
                if vol['avg_delta'] is None:
                    message += "returning NULL"
                else:
                    message += f"{int(vol['avg_delta']*100)}% "
                    if vol['avg_delta'] > 0:
                        message += f"above {self.DAILY_AVERAGE_LOOKBACK - 1}-day average."
                    else:
                        message += f"below {self.DAILY_AVERAGE_LOOKBACK - 1}-day average."
                self.sns_connector.send_message(topic_arn=self.topic_arn, message_title=message_title, message_body=message)


    def execute(self):
        self.loggerv3.start(f"Running Anomaly Detection Job for {self.target_date}")
        self.query_tables()
        self.detect_anomalies()
        self.loggerv3.success("All Processing Complete!")



class AnomalyDetectionTables:

    def __init__(self):
        self.tables = [
            {
              "schema": "warehouse",
              "table_name": "fact_visits",
              "time_series_column": "timestamp",
              "run_date": -1,
              "platform_column": "platform"
            },
            {
              "schema": "warehouse",
              "table_name": "vod_viewership",
              "time_series_column": "start_timestamp",
              "run_date": -1,
              "platform_column": "platform"
            },
            {
              "schema": "warehouse",
              "table_name": "livestream_heartbeat",
              "time_series_column": "event_timestamp",
              "run_date": -1
            },
            {
              "schema": "warehouse",
              "table_name": "subscription",
              "time_series_column": "event_timestamp",
              "run_date": -1
            },
            {
              "schema": "warehouse",
              "table_name": "livestream_viewership",
              "time_series_column": "run_date",
              "run_date": -1
            },
            {
              "schema": "warehouse",
              "table_name": "megaphone_metrics",
              "time_series_column": "run_date",
              "run_date": -2
            },
            {
                "schema": "warehouse",
                "table_name": "content_owner_combined_a2",
                "time_series_column": "start_date",
                "run_date": -3,
                "time_series_data_type": "int"
            },
            {
                "schema": "warehouse",
                "table_name": "ad_event",
                "time_series_column": "event_timestamp",
                "run_date": -1,
                "platform_column": "platform"
            },
            {
                "schema": "warehouse",
                "table_name": "vod_sessions",
                "time_series_column": "start_timestamp",
                "run_date": -1,
                "platform_column": "platform"
            },
            {
                "schema": "warehouse",
                "table_name": "search_event",
                "time_series_column": "timestamp",
                "run_date": -1,
                "platform_column": "platform"
            },
            {
                "schema": "warehouse",
                "table_name": "signup_flow_event",
                "time_series_column": "event_timestamp",
                "run_date": -1
            },
            {
                "schema": "warehouse",
                "table_name": "page_event",
                "time_series_column": "event_timestamp",
                "run_date": -1
            },
            {
                "schema": "warehouse",
                "table_name": "pending_cancel_balance",
                "time_series_column": "run_date",
                "run_date": -1
            },
            {
                "schema": "warehouse",
                "table_name": "gate_signup_complete",
                "time_series_column": "event_timestamp",
                "run_date": -1
            },
            {
                "schema": "warehouse",
                "table_name": "hero_event",
                "time_series_column": "event_timestamp",
                "run_date": -1,
                "platform_column": "platform"
            },
            {
                "schema": "warehouse",
                "table_name": "content_block_item_clicked",
                "time_series_column": "event_timestamp",
                "run_date": -1
            },
            {
                "schema": "warehouse",
                "table_name": "content_block_carousel_item_clicked",
                "time_series_column": "event_timestamp",
                "run_date": -1
            },
            {
                "schema": "warehouse",
                "table_name": "click_event",
                "time_series_column": "event_timestamp",
                "run_date": -1,
                "platform_column": "platform"
            },
            {
                "schema": "warehouse",
                "table_name": "signup_button_clicked",
                "time_series_column": "event_timestamp",
                "run_date": -1,
                "platform_column": "platform"
            }
        ]
