import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class AggDailyRttvViewershipJobV2(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='agg_daily_rttv_viewership')
        self.target_date = target_date
        self.target_date_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
        self.next_date_dt = self.target_date_dt + timedelta(1)
        self.next_date = datetime.strftime(self.next_date_dt, '%Y-%m-%d')
        self.rttv_events = []
        self.master_df = None


    def load_rttv_events(self):
        query = f"""
        SELECT
            category as event_type,
            title as event_title,
            start_time,
            end_time,
            channel
        FROM
            warehouse.livestream_schedule_v2
        WHERE
            start_time >= '{self.target_date}'
            AND start_time < '{self.next_date}'
        ORDER BY 3;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.rttv_events.append({
                'type': result[0],
                'title': result[1],
                'start_date': result[2],
                'end_date': result[3],
                'channel': result[4]
            })


    def get_event_viewership(self, event):
        event_start = event['start_date']
        event_end = event['end_date']
        query = f"""
        SELECT
            CASE
                WHEN lower(user_tier) = 'first' THEN 'premium'
                WHEN lower(user_tier) = 'free' AND user_uuid = 'null' THEN 'anon'
                WHEN lower(user_tier) = 'free' AND user_uuid is NULL THEN 'anon'
                WHEN lower(user_tier) = 'free' AND user_id is NULL AND user_uuid is not NULL and user_uuid != 'null' THEN 'free'
                ELSE lower(user_tier)
            END as user_tier,
            count(distinct CASE
                    WHEN user_id is NOT NULL THEN user_id
                    WHEN user_uuid = 'null' THEN anonymous_id
                    WHEN user_uuid is NOT NULL THEN user_uuid
                    ELSE anonymous_id
                END
            )
        FROM warehouse.livestream_heartbeat
        WHERE
            event_timestamp BETWEEN '{event_start}' AND '{event_end}'
        GROUP BY 1;
        """
        results = self.db_connector.read_redshift(query)
        event_viewership = {
            'total': 0,
            'premium': 0,
            'trial': 0,
            'free': 0,
            'anon': 0,
            'grant': 0
        }
        for result in results:
            user_tier = result[0]
            uv = result[1]
            event_viewership[user_tier] = uv
            event_viewership['total'] += uv

        return event_viewership


    def get_event_concurrents(self, event):
        start_time = datetime.strftime(event['start_date'], '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strftime(event['end_date'], '%Y-%m-%d %H:%M:%S')
        query = f"""
        WITH by_minute AS (
            SELECT cast(event_timestamp as char(16)) as event_time,
               count(distinct CASE
                        WHEN user_id is NOT NULL THEN user_id
                        WHEN user_uuid = 'null' THEN anonymous_id
                        WHEN user_uuid is NOT NULL THEN user_uuid
                        ELSE anonymous_id
                    END
               ) as concurrents
            FROM warehouse.livestream_heartbeat
            WHERE event_timestamp >= '{start_time}'
            AND event_timestamp < '{end_time}'
            GROUP BY 1
            ORDER BY 1
        )
        SELECT
            max(concurrents) as peak_concurrents,
            avg(concurrents) as avg_concurrents
        FROM by_minute;
        """

        results = self.db_connector.read_redshift(query)
        event_concurrents = {
            'peak_concurrents': 0,
            'avg_concurrents': 0
        }
        for result in results:
            event_concurrents['peak_concurrents'] = result[0],
            event_concurrents['avg_concurrents'] = result[1]

        return event_concurrents


    def build_record(self, event):
        event_viewership = self.get_event_viewership(event)
        event_concurrents = self.get_event_concurrents(event)

        return {
            'event_title': event['title'],
            'event_type': event['type'],
            'start_date': event['start_date'],
            'total_uv': event_viewership['total'],
            'premium_uv': event_viewership['premium'],
            'trial_uv': event_viewership['trial'],
            'free_uv': event_viewership['free'],
            'anon_uv': event_viewership['anon'],
            'grant_uv': event_viewership['grant'],
            'peak_concurrents': event_concurrents['peak_concurrents'],
            'avg_concurrents': event_concurrents['avg_concurrents'],
            'channel': event['channel']
        }


    def process(self):
        self.load_rttv_events()
        events = []
        for idx, event in enumerate(self.rttv_events):
            self.loggerv3.inline_info(f"Building Record {idx} of {len(self.rttv_events)}")
            events.append(self.build_record(event))
        self.master_df = pd.DataFrame(events)


    def write_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.master_df, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)


    def execute(self):
        self.loggerv3.start(f"Running Agg Daily RTTV Viewership V2 for {self.target_date}")
        self.process()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
