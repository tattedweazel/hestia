import pandas as pd
from base.etl_jobv3 import EtlJobV3
from datetime import datetime, timedelta


class DecileReportingJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, file_location=''):
        super().__init__(jobname = __name__, db_connector = db_connector, table_name = 'vod_decile_reporting')
        self.target_date = target_date
        self.schema = 'warehouse'
        self.decile_records = None
        self.parsed_deciles = None
        self.target_dto = datetime.strptime(self.target_date, '%Y-%m-%d')
        self.next_dto = self.target_dto + timedelta(1)
        self.next_date = datetime.strftime(self.next_dto, '%Y-%m-%d')


    def load_decile_records(self):
        self.loggerv3.info(f"Loading Decile Records for {self.target_date} ")
        records = []
        results = self.db_connector.read_redshift(f"""
            SELECT
                    *
            FROM warehouse.vod_deciles
            WHERE start_timestamp >= '{self.target_date}' AND
                  start_timestamp < '{self.next_date}'
        """)

        for result in results:
            records.append({
                'session_id': result[0],
                'user_key': result[1],
                'user_tier': result[2],
                'anonymous_id': result[3],
                'episode_key': result[4],
                'start_timestamp': result[5],
                'platform': result[6],
                'on_mobile_device': result[7],
                'decile': result[8]
                })
        self.decile_records = records


    def process_decile_records(self):
        self.loggerv3.info('Parsing Decile Records')
        parsed_records = []
        for record in self.decile_records:
            parsed_record = {
                'session_id': record['session_id'],
                'user_key': record['user_key'],
                'user_tier': record['user_tier'],
                'anonymous_id': record['anonymous_id'],
                'episode_key': record['episode_key'],
                'start_timestamp': record['start_timestamp'],
                'platform': record['platform'],
                'on_mobile_device': record['on_mobile_device'],
                'p0': 0,
                'p10': 0,
                'p20': 0,
                'p30': 0,
                'p40': 0,
                'p50': 0,
                'p60': 0,
                'p70': 0,
                'p80': 0,
                'p90': 0,
                'p100': 0
            }
            string_decile = str(record['decile']) # turn the decile value to a string
            reversed_decile = string_decile[::-1] # reverse the decile string
            # iterate through the reversed decile string and assign the values to the parsed record
            for i in range(0, len(reversed_decile)):
                if i == 0:
                    parsed_record[f'p0'] = reversed_decile[i] 
                else:
                    parsed_record[f'p{i}0'] = reversed_decile[i]
            parsed_records.append(parsed_record)
        self.parsed_deciles = pd.DataFrame(parsed_records)




    
    def write_all_results_to_redshift(self):
        self.loggerv3.info("Writing results to Red Shift")
        self.db_connector.write_to_sql(self.parsed_deciles, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


    def execute(self):
        self.loggerv3.start(f"Running Decile Reporting Job")
        self.load_decile_records()
        self.process_decile_records()
        self.write_all_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
