import os
from base.etl_jobv3 import EtlJobV3
from jobs.framework_requirements_job import FrameworkRequirementsJob


class CleanRequirementsJob(EtlJobV3):

    def __init__(self, target_date=None, db_connector=None, api_connector=None, file_location=''):
        super().__init__(jobname=__name__, target_date=target_date, db_connector=db_connector, local_mode=True)
        """NOTE: Run FrameworkRequirementsJob before running this job to get latest requirements usage"""
        self.framework_requirements_job = FrameworkRequirementsJob(db_connector=self.db_connector)
        self.unused_requirements = []



    def get_unused_requirements(self):
        self.loggerv3.info('Getting unused requirements')
        query = f"""
        WITH imported_by_cte as (
          select *
          from data_monitoring_tools.hestia_requirements
          where type = 'imported_by'
        ), required_by_cte as (
          select *
          from data_monitoring_tools.hestia_requirements
          where type = 'required_by'
        ), requires_cte as (
          select *
          from data_monitoring_tools.hestia_requirements
          where type = 'requires'
        ), joined_cte as (
            select p.package,
                 p.version,
                 i.module as imported_by,
                 rb.module as required_by,
                 r.module as requires
            from data_monitoring_tools.dim_hestia_packages p
            left join imported_by_cte i on i.package = p.package
            left join required_by_cte rb on rb.package = p.package
            left join requires_cte r on r.package = p.package
        )
        select package
        from joined_cte
        where imported_by is null
          and required_by is null
        group by 1;
        """
        results = self.db_connector.read_redshift(query)
        for result in results:
            self.unused_requirements.append(result[0])


    def delete_unused_requirements(self):
        self.loggerv3.info('Deleting unused requirements')
        for requirement in self.unused_requirements:
            os.system(f'pip uninstall {requirement}')


    def update_requirements(self):
        self.framework_requirements_job.execute()


    def execute(self):
        self.loggerv3.start('Starting Clean Requirements Job')
        self.get_unused_requirements()
        self.delete_unused_requirements()
        self.update_requirements()
        self.loggerv3.success('All Processing Complete!')
