import pandas as pd
from base.etl_jobv3 import EtlJobV3


class Sv2TableUsageJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, api_connector = None, file_location = ''):
        super().__init__(jobname = __name__, target_date = target_date, db_connector = db_connector, table_name = 'table_usage')
        self.target_date = target_date
        self.records = []
        self.final_dataframe = None


    def load_looker_table_usage(self):
        self.loggerv3.info('Loading looker table usage')
        query = f"""
            WITH base as (
                SELECT
                    st.table_schema,
                    st.table_name,
                    t.table_id
                FROM svv_tables st
                LEFT JOIN svv_table_info t on t."table" = st.table_name
                WHERE
                    st.table_schema in ('warehouse', 'rooster_teeth_ecomm')
                    AND st.table_type = 'BASE TABLE'
            )
            SELECT base.table_schema,
                   base.table_name,
                   base.table_id,
                   count(*) as query_scans,
                   max(starttime) as last_scan
            FROM base JOIN STL_QUERY ON STL_QUERY.querytxt LIKE '%%' || base.table_name || '%%'
            WHERE STL_QUERY.querytxt like '-- Looker Query Context%%'
            GROUP BY 1, 2, 3;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.records.append({
                'run_date': self.target_date,
                'table_schema': result[0],
                'table_name': result[1],
                'table_id': result[2],
                'query_scans': result[3],
                'last_scan': result[4],
                'location': 'looker'
            })


    def load_query_scans_table_usage(self):
        self.loggerv3.info('Loading query scans table usage')
        query = f"""
            WITH scan as (
                SELECT
                    tbl,
                    perm_table_name,
                    count(distinct query) num_qs,
                    MAX(endtime) last_scan
                FROM stl_scan s
                WHERE
                    s.userid > 1
                    AND s.perm_table_name NOT IN ('Internal Worktable','S3')
                GROUP BY 1, 2
            )
            SELECT
                st.table_schema,
                st.table_name,
                t.table_id,
                NVL(s.num_qs,0) num_qs,
                s.last_scan
            FROM svv_tables st
            LEFT JOIN svv_table_info t on t."table" = st.table_name
            LEFT JOIN scan s ON s.tbl = t.table_id
            WHERE
                st.table_schema in ('warehouse', 'rooster_teeth_ecomm')
                AND st.table_type = 'BASE TABLE'
            ORDER BY 4 desc;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.records.append({
                'run_date': self.target_date,
                'table_schema': result[0],
                'table_name': result[1],
                'table_id': result[2],
                'query_scans': result[3],
                'last_scan': result[4],
                'location': 'redshift'
            })


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframes')
        self.final_dataframe = pd.DataFrame(self.records)


    def write_to_redshift(self):
        self.loggerv3.info("Writing to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema='warehouse', method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name)

    # TODO: Identify tables from warehouse.table_usage that no longer exist (since Data deletes some) and then delete those records from warehouse.table_usage


    def execute(self):
        self.loggerv3.start(f"Running SV2 Table Usage Job for {self.target_date}")
        self.load_looker_table_usage()
        self.load_query_scans_table_usage()
        self.build_final_dataframe()
        self.write_to_redshift()
        self.loggerv3.success("All Processing Complete!")
