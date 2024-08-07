from base.etl_jobv3 import EtlJobV3
from rapidfuzz import fuzz, process
from pandas import pandas as pd
import numpy as np


class QuarterlySalesEpisodeMapperJob(EtlJobV3):

    def __init__(self, quarter_start, quarter_end, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, db_connector=db_connector, table_name='TBD', local_mode=True)
        self.quarter_start = quarter_start
        self.quarter_end = quarter_end
        self.schema = 'warehouse'
        self.EXTRACT_LIMIT = 1
        self.MATCH_THRESHOLD = 95
        self.records = []
        self.episodes_by_platform = {}
        self.dim_map_df = pd.DataFrame(columns=['series_title', 'youtube_title', 'roosterteeth_title', 'megaphone_title'])


    def get_episodes_by_platform(self):
        self.loggerv3.info('Getting episodes by platform')
        query = """
        SELECT
            series_title,
            platform,
            episode_title
        FROM warehouse.agg_daily_quarterly_sales_metrics
        WHERE series_title = 'annual pass'
        GROUP BY 1, 2, 3
        ORDER BY 1, 2;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.records.append({
                'series_title': result[0],
                'platform': result[1],
                'episode_title': result[2]
            })


    def build_struct(self):
        self.loggerv3.info('Building data struct')
        for record in self.records:
            if record['series_title'] not in self.episodes_by_platform:
                self.episodes_by_platform[record['series_title']] = {record['platform']: [record['episode_title']]}
            else:
                if record['platform'] not in self.episodes_by_platform[record['series_title']]:
                    self.episodes_by_platform[record['series_title']][record['platform']] = [record['episode_title']]
                else:
                    self.episodes_by_platform[record['series_title']][record['platform']].append(record['episode_title'])


    def map_episodes_by_series(self):
        self.loggerv3.info('Mapping episodes by series')
        for series_title in self.episodes_by_platform:
            self.fuzzy_map(series_title)


    def fuzzy_map(self, series_title):
        self.loggerv3.info(f'Fuzzy mapping episodes for {series_title}')
        df = None
        for key, val in self.episodes_by_platform[series_title].items():
            s = pd.Series(val, name=key)
            if df is None:
                df = s.to_frame()
            else:
                df = pd.concat([df, s.to_frame()])
        df['series_title'] = series_title


        if 'megaphone' in df and 'youtube' in df and 'roosterteeth' in df:
            megaphone_titles = [x for x in df['megaphone'].tolist() if str(x) != 'nan']
            df = df[~df['youtube'].isna()]

            # Creates a list of tuples with (title, accuracy 1-100)
            df['matches1'] = df['youtube'].apply(lambda x: process.extract(x, megaphone_titles, limit=self.EXTRACT_LIMIT))

            # Filters tuple list to most accurate
            df['results1'] = df['matches1'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= self.MATCH_THRESHOLD]))

            roosterteeth_titles = [x for x in df['roosterteeth'].tolist() if str(x) != 'nan']

            # Creates a list of tuples with (title, accuracy 1-100)
            df['matches2'] = df['youtube'].apply(lambda x: process.extract(x, roosterteeth_titles, limit=self.EXTRACT_LIMIT))

            # Filters tuple list to most accurate
            df['results2'] = df['matches2'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= self.MATCH_THRESHOLD]))

            df.rename(columns={'youtube': 'youtube_title', 'results1': 'megaphone_title', 'results2': 'roosterteeth_title'}, inplace=True)
            df = df[['series_title', 'youtube_title', 'megaphone_title', 'roosterteeth_title']]


        elif 'roosterteeth' in df and 'megaphone' in df:
            roosterteeth_titles = [x for x in df['roosterteeth'].tolist() if str(x) != 'nan']
            df = df[~df['megaphone'].isna()]

            # Creates a list of tuples with (title, accuracy 1-100)
            df['matches1'] = df['megaphone'].apply(lambda x: process.extract(x, roosterteeth_titles, limit=self.EXTRACT_LIMIT))

            # Filters tuple list to most accurate
            df['results1'] = df['matches1'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= self.MATCH_THRESHOLD]))

            df.rename(columns={'megaphone': 'megaphone_title', 'results1': 'roosterteeth_title'}, inplace=True)
            df = df[['series_title', 'megaphone_title', 'roosterteeth_title']]


        elif 'roosterteeth' in df and 'youtube' in df:
            roosterteeth_titles = [x for x in df['roosterteeth'].tolist() if str(x) != 'nan']
            df = df[~df['youtube'].isna()]

            # Creates a list of tuples with (title, accuracy 1-100)
            df['matches1'] = df['youtube'].apply(lambda x: process.extract(x, roosterteeth_titles, limit=self.EXTRACT_LIMIT))

            # Filters tuple list to most accurate
            df['results1'] = df['matches1'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= self.MATCH_THRESHOLD]))

            df.rename(columns={'youtube': 'youtube_title', 'results1': 'roosterteeth_title'}, inplace=True)
            df = df[['series_title', 'youtube_title', 'roosterteeth_title']]


        elif 'megaphone' in df and 'youtube' in df:
            megaphone_titles = [x for x in df['megaphone'].tolist() if str(x) != 'nan']
            df = df[~df['youtube'].isna()]

            # Creates a list of tuples with (title, accuracy 1-100)
            df['matches1'] = df['youtube'].apply(lambda x: process.extract(x, megaphone_titles, limit=self.EXTRACT_LIMIT))

            # Filters tuple list to most accurate
            df['results1'] = df['matches1'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= self.MATCH_THRESHOLD]))

            df.rename(columns={'youtube': 'youtube_title', 'results1': 'megaphone_title'}, inplace=True)
            df = df[['series_title', 'youtube_title', 'megaphone_title']]


        elif 'youtube' in df:
            df.rename(columns={'youtube': 'youtube_title'}, inplace=True)
            df = df[['series_title', 'youtube_title']]


        elif 'megaphone' in df:
            df.rename(columns={'megaphone': 'megaphone_title'}, inplace=True)
            df = df[['series_title', 'megaphone_title']]


        elif 'roosterteeth' in df:
            df.rename(columns={'roosterteeth': 'roosterteeth_title'}, inplace=True)
            df = df[['series_title', 'roosterteeth_title']]


        if self.dim_map_df is None:
            self.dim_map_df = df
        else:
            self.dim_map_df = pd.concat([self.dim_map_df, df])


    def write_to_csv(self):
        self.dim_map_df.to_csv('output.csv')


    # def truncate_table(self):
    #     self.loggerv3.info('Truncating table')
    #     self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.table_name};")


    # def write_results_to_redshift(self):
    #     self.loggerv3.info("Writing results to Red Shift")
    #     self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', chunksize=5000, index=False, if_exists='append')
    #     self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


    def execute(self):
        self.loggerv3.start('Running Quarterly Sales Episode Mapper Job')
        self.get_episodes_by_platform()
        self.build_struct()
        self.map_episodes_by_series()
        self.write_to_csv()


        # self.truncate_table()
        # self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
