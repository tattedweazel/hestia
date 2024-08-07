import os
import pkg_resources
import subprocess
from base.etl_jobv3 import EtlJobV3
from pandas import pandas as pd


class FrameworkRequirementsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, table_name='hestia_requirements', local_mode=True)
        self.schema = 'data_monitoring_tools'
        self.dim_table = 'dim_hestia_packages'
        self.packages = []
        self.requirements_filename = 'requirements.txt'
        self.package_requirements = []
        self.requires_list = []
        self.required_by_list = []
        self.final_dataframe = None
        self.dim_dataframe = None


    def get_installed_packages(self):
        self.loggerv3.info('Getting installed packages')
        installed_packages = pkg_resources.working_set
        for package in installed_packages:
            self.packages.append({
                'package': package.key,
                'version': package.version
            })


    def get_imported_packages(self):
        self.loggerv3.info('Getting imported packages')
        os.system('pigar gen --with-referenced-comments --dont-show-differences')


    def read_imported_packages(self):
        self.loggerv3.info('Reading imported packages')
        """
        'package': <package>
        'imported_by': <file1>, <file2>, etc.
        """
        files = []
        with open(self.requirements_filename, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith('#') and 'rt-data-hestia' in line:
                    files.append(
                        line.split('rt-data-hestia/')[1].split(':')[0]
                    )
                elif '==' in line:
                    for file in files:
                        self.package_requirements.append({
                            'package': line.split('==')[0],
                            'type': 'imported_by',
                            'module': file
                        })
                    files = []


    def get_package_dependencies(self):
        self.loggerv3.info('Getting package dependencies')
        for package in self.packages:
            details = subprocess.check_output(f"pip show {package['package']}", shell=True)
            details_list = str(details).split('\\n')

            for detail in details_list:
                if 'Requires' in detail and len(detail.replace('Requires:', '').strip()) > 0:
                    self.requires_list.append({
                        'package': package['package'],
                        'modules_list': (detail.replace('Requires:', '').strip()).split(', ')
                    })
                if 'Required-by' in detail and len(detail.replace('Required-by:', '').strip()) > 0:
                    self.required_by_list.append({
                        'package': package['package'],
                        'modules_list': (detail.replace('Required-by:', '').strip()).split(', ')
                    })

        for pkg in self.requires_list:
            for module in pkg['modules_list']:
                self.package_requirements.append({
                    'package': pkg['package'],
                    'type': 'requires',
                    'module': module
                })

        for pkg in self.required_by_list:
            for module in pkg['modules_list']:
                self.package_requirements.append({
                    'package': pkg['package'],
                    'type': 'required_by',
                    'module': module
                })



    def build_final_dataframes(self):
        self.loggerv3.info('Building final dataframes')
        # Primary Dataframe
        self.final_dataframe = pd.DataFrame(self.package_requirements)
        # Dim Dataframe
        self.dim_dataframe = pd.DataFrame(self.packages)


    def truncate_tables(self):
        self.loggerv3.info('Truncating tables')
        # Primary Table
        self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.table_name};")
        # Dim Table
        self.db_connector.write_redshift(f"TRUNCATE TABLE {self.schema}.{self.dim_table};")


    def write_results_to_redshift(self):
        self.loggerv3.info("Writing results to Redshift")
        # Primary Table
        self.db_connector.write_to_sql(self.final_dataframe, self.table_name, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.table_name, schema=self.schema)
        # Dim Table
        self.db_connector.write_to_sql(self.dim_dataframe, self.dim_table, self.db_connector.sv2_engine(), schema=self.schema, method='multi', index=False, if_exists='append')
        self.db_connector.update_redshift_table_permissions(self.dim_table, schema=self.schema)


    def execute(self):
        self.loggerv3.start('Starting Framework Requirements Job')
        self.get_installed_packages()
        self.get_imported_packages()
        self.read_imported_packages()
        self.get_package_dependencies()
        self.build_final_dataframes()
        self.truncate_tables()
        self.write_results_to_redshift()
        self.loggerv3.success('All Processing Complete!')