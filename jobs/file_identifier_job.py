import config
import os
import pandas as pd
from base.etl_jobv3 import EtlJobV3


class FileIdentifierJob(EtlJobV3):

    def __init__(self, target_date = None, db_connector = None, file_location=''):
        super().__init__(jobname = __name__, db_connector = db_connector, table_name = 'dim_hestia_files')
        self.users_df = None
        self.schema = 'data_monitoring_tools'
        self.base = '.' if config.file_location == '' else config.file_location
        self.dirs = ['jobs/', 'process_handlers/', 'tools/modules/']
        self.filenames = {}
        self.files = []
        self.final_dataframe = None


    def get_filenames(self):
        self.loggerv3.info('Get filenames')
        for dir in self.dirs:
            sub_dir = '/'.join([self.base, dir])
            filenames = os.listdir(sub_dir)
            for filename in filenames:
                if filename not in ['.DS_Store', '__pycache__']:
                    self.recursive_file_finder_helper(dir, sub_dir, filename)


    def recursive_file_finder_helper(self, dir, sub_dir, filename):
        file = sub_dir + filename
        if os.path.isfile(file) and file.endswith('.py'):
            if dir not in self.filenames:
                self.filenames[dir] = [filename]
            else:
                self.filenames[dir].append(filename)
        elif os.path.isdir(file):
            new_filenames = os.listdir(file)
            for new_filename in new_filenames:
                if new_filename not in ['.DS_Store', '__pycache__']:
                    new_sub_dir = sub_dir + filename + '/'
                    self.recursive_file_finder_helper(dir, new_sub_dir, new_filename)


    def dir_name_helper(self, dir):
        if dir == 'tools/modules/':
            return 'tools/'
        else:
            return dir


    def filename_helper(self, dir, filename):
        if dir == 'tools/modules/':
            return '.'.join(['tools', filename.replace('.py', '')])
        else:
            return '.'.join([dir.replace('/', ''), filename.replace('.py', '')])


    def build_final_dataframe(self):
        self.loggerv3.info('Building final dataframe')
        for dir, filenames in self.filenames.items():
            for filename in filenames:
                self.files.append({
                    'path': self.dir_name_helper(dir),
                    'filename': self.filename_helper(dir, filename)
                })
        self.final_dataframe = pd.DataFrame(self.files)


    def clear_table(self):
        self.loggerv3.info('Truncating table')
        self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.table_name}")


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Redshift")
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)


    def execute(self):
        self.loggerv3.start(f"Running File Identifier Job")
        self.get_filenames()
        self.build_final_dataframe()
        self.clear_table()
        self.write_results_to_redshift()
        self.loggerv3.success("All Processing Complete!")
