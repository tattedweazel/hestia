import pandas as pd
from base.etl_jobv3 import EtlJobV3
from utils.connectors.supporting_cast_api_connector import SupportingCastApiConnector


class SupportingCastMembersJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
        super().__init__(jobname = __name__, db_connector = db_connector, table_name = 'supporting_cast_members', local_mode=True)
        self.mapping_table = 'supporting_cast_member_plans'
        self.page = 1
        self.records_per_page = 500
        self.supporting_cast_api_connector = SupportingCastApiConnector(file_location=self.file_location, logger=self.loggerv3)
        self.total_pages = None
        self.export_endpoint = 'memberships'
        self.records = None
        self.SLEEP = 10
        self.member_records = None
        self.rt_users = None
        self.member_plans = []
        self.final_members_dataframe = None


    def request_page_count_from_supporting_cast(self):
        self.loggerv3.info('Requesting page count from Supporting Cast')
        response = self.supporting_cast_api_connector.make_single_page_request(endpoint=self.export_endpoint,
                                                                               page=self.page,
                                                                               records_per_page=self.records_per_page
                                                                               )
        self.total_pages = response['pages']
        self.loggerv3.info(f'{self.total_pages} Pages')


    def request_bulk_data(self):
        self.loggerv3.info('Requesting bulk data from Supporting Cast')
        self.records = self.supporting_cast_api_connector.make_bulk_request(endpoint=self.export_endpoint,
                                                                            records_per_page=self.records_per_page,
                                                                            total_pages=self.total_pages
                                                                            )


    def select_member_records(self):
        self.loggerv3.info('Selecting member records')
        self.member_records = []
        unique_ids = []
        for record in self.records:
            if record['id'] not in unique_ids and record['email'] is not None:
                unique_ids.append(record['id'])
                data = {
                    'sc_id': record['id'],
                    'email': record['email'].lower(),
                    'status': record['status'],
                    'joined': record['joined'],
                    'plan_id': record['plan_id'],
                    'plan_ids': record['plan_ids']
                }
                self.member_records.append(data)


    def get_rt_users(self):
        self.loggerv3.info('Getting RT users')
        self.rt_users = []
        results = self.db_connector.query_v2_db(f"""SELECT u.uuid, u.email, u.id
                                                    FROM production.users u;
                                                """)

        for result in results:
            self.rt_users.append({
                'rt_uuid': result[0],
                'email': result[1].lower(),
                'rt_id': result[2]
            })


    def merge_member_records_to_rt_users(self):
        self.loggerv3.info('Merging records to RT users')
        member_records_df = pd.DataFrame(self.member_records, index=None)
        rt_users_df = pd.DataFrame(self.rt_users, index=None)
        self.final_members_dataframe = member_records_df.merge(rt_users_df, on='email', how='left')


    def build_member_plans(self):
        self.loggerv3.info('Building member plans')
        for data in self.member_records:
            plan_ids = data['plan_ids']
            for plan_id in plan_ids:
                self.member_plans.append({
                    'sc_id': data['sc_id'],
                    'plan_id': plan_id
                })


    def remove_attributes_from_members(self):
        self.loggerv3.info('Removing email from members')
        self.final_members_dataframe.drop('email', axis=1, inplace=True)
        self.final_members_dataframe.drop('plan_ids', axis=1, inplace=True)


    def truncate_members_table(self):
        self.loggerv3.info(f'Truncating {self.table_name}')
        self.db_connector.write_redshift(f"""TRUNCATE TABLE warehouse.{self.table_name};""")


    def write_members_to_redshift(self):
        self.loggerv3.info("Writing members to Redshift")
        self.db_connector.write_to_sql(self.final_members_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')


    def truncate_member_plans_table(self):
        self.loggerv3.info(f'Truncating member_plans')
        self.db_connector.write_redshift(f"""TRUNCATE TABLE warehouse.{self.mapping_table};""")


    def write_member_plans_to_redshift(self):
        self.loggerv3.info("Writing member plans to Redshift")
        member_plans_df = pd.DataFrame(self.member_plans, index=None)
        member_plans_df.dropna(inplace=True)
        self.db_connector.write_to_sql(member_plans_df, self.mapping_table, self.db_connector.sv2_engine(), schema='warehouse', method='multi', chunksize=5000, index=False, if_exists='append')


    def execute(self):
        self.loggerv3.start(f"Running Supporting Cast Members Job")
        self.request_page_count_from_supporting_cast()
        self.request_bulk_data()
        self.select_member_records()
        self.get_rt_users()
        self.merge_member_records_to_rt_users()
        self.build_member_plans()
        self.remove_attributes_from_members()
        self.truncate_members_table()
        self.write_members_to_redshift()
        self.db_connector.update_redshift_table_permissions(self.table_name)
        self.truncate_member_plans_table()
        self.write_member_plans_to_redshift()
        self.db_connector.update_redshift_table_permissions(self.mapping_table)
        self.loggerv3.success("All Processing Complete!")
