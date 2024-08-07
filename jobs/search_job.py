import json
import os
from base.etl_jobv3 import EtlJobV3
from utils.connectors.s3_api_connector import S3ApiConnector


class SearchJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector)
        self.rt_bucket = 'rt-popularity'
        self.s3_api_connector = S3ApiConnector(file_location=self.file_location, bucket=self.rt_bucket, profile_name='roosterteeth', dry_run=self.db_connector.dry_run)
        self.output_location = self.file_location + 'uploads'
        self.output_filename = 'search_conversions.json'
        self.records = []
        self.data = {}
        self.finalized_data = []


    def collect(self):
        self.loggerv3.info("Loading Conversion data")
        query = """
        WITH cte as (
            SELECT LOWER(search_term) AS term_used,
                   CASE
                        WHEN target_type in ('episodes', 'episode', 'epısode') THEN 'episode'
                        WHEN target_type in ('show', 'series') THEN 'series'
                        ELSE 'none'
                        END            AS target_type,
                   target_id,
                   count(*) as cnt
            FROM warehouse.search_event
            WHERE search_term is NOT NULL
            GROUP BY 1, 2, 3
            ORDER BY 4 desc
        )
        SELECT term_used,
           target_type,
           target_id,
           cnt
        FROM cte
        WHERE target_type != 'none';
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            record = {
                "term_used": result[0],
                "target_type": result[1],
                "target_id": result[2],
                "count": result[3]
            }
            self.records.append(record)


    def process_records(self):
        self.loggerv3.info("Processing Records")
        for record in self.records:
            target_type = record['target_type']
            target_id = record['target_id']
            term_used = record['term_used']
            conv_count = record['count']
            if conv_count < 100:
                continue
            if term_used not in self.data:
                self.data[term_used] = {
                    'results': [
                        {
                            'type': record['target_type'],
                            'uuid': record['target_id'],
                            'count': record['count']
                        }
                    ]
                }
            else:
                found = False
                for result in self.data[term_used]['results']:
                    if result['type'] == target_type and result['uuid'] == target_id:
                        result['count'] += record['count']
                        found = True
                if found is False:
                    self.data[term_used]['results'].append({
                        'type': record['target_type'],
                        'uuid': record['target_id'],
                        'count': record['count']
                    })


    def restructure_data(self):
        self.loggerv3.info("Re-structuring Data for Output")
        for query_term in self.data:
            record = {
                'conversions': self.get_conversion_count(self.data[query_term]),
                'query': query_term,
                'results': self.data[query_term]['results']
            }
            self.finalized_data.append(record)


    def get_conversion_count(self, query_term):
        conversions = 0
        for result in query_term['results']:
            conversions += result['count']
        return conversions


    def write_all_results_to_json(self):
        self.loggerv3.info(f"Creating output JSON: {self.output_filename}")
        with open(f"{self.output_location}/{self.output_filename}", 'w') as f:
            f.write(json.dumps(self.finalized_data, indent=2))


    def upload_output_to_s3(self):
        self.loggerv3.info("Uploading to S3")
        self.s3_api_connector.upload_file(f"{self.output_location}/{self.output_filename}", self.output_filename)


    def cleanup(self):
        cmd = f"rm {self.output_location}/{self.output_filename}"
        os.system(cmd)


    def execute(self):
        self.loggerv3.start(f"Running Search Job")
        self.collect()
        self.process_records()
        self.restructure_data()
        self.write_all_results_to_json()
        self.upload_output_to_s3()
        self.cleanup()
        self.loggerv3.success("All Processing Complete!")
