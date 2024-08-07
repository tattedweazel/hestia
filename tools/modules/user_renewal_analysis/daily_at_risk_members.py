import config
from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class DailyAtRiskMembers:

    def __init__(self, target_date):
        """target_date: the current date of running the job (i.e. today) with format of YYY-MM-DD"""
        self.target_date = target_date
        self.db_connector = DatabaseConnector(file_location=config.file_location)
        self.loggerv3 = Loggerv3(name=__name__, file_location=config.file_location, local_mode=config.local_mode)


    def execute(self):
        self.loggerv3.start('Getting at risk members')
        results = self.db_connector.read_redshift(f"""
                                                       WITH user_subs as (
                                                            SELECT user_key,
                                                                   start_timestamp,
                                                                   listagg(subscription_event_type, '-') within group ( order by user_key ) as subs
                                                            FROM warehouse.subscription
                                                            WHERE
                                                                  start_timestamp >= date_add('day', -14, '{self.target_date}') AND
                                                                  start_timestamp < date_add('day', -13, '{self.target_date}') AND
                                                                  subscription_event_type in ('FTP Conversion', 'New Paid', 'Returning Paid') AND
                                                                  membership_plan = '1month'
                                                            GROUP BY 1, 2
                                                       ), signups as (
                                                            SELECT
                                                                user_key,
                                                                (CASE
                                                                    WHEN subs like '%%Returning Paid%%' THEN 'Returning Paid'
                                                                    WHEN subs like '%%FTP Conversion%%' THEN 'FTP Conversion'
                                                                    WHEN subs like '%%New Paid%%' THEN 'New Paid'
                                                                END) as sub_type,
                                                                start_timestamp
                                                            FROM user_subs
                                                       ), viewership as (
                                                            SELECT
                                                                   vv.user_key,
                                                                   sum(vv.active_seconds) / 60.0 as vod_time_watched_min
                                                            FROM warehouse.vod_viewership vv
                                                            WHERE
                                                                vv.start_timestamp >= date_add('day', -14, '{self.target_date}') AND
                                                                vv.start_timestamp < '{self.target_date}' AND
                                                                vv.user_key in (SELECT user_key FROM signups) AND
                                                                vv.user_key is not null AND
                                                                vv.user_tier = 'premium' AND
                                                                vv.max_position > 0
                                                            GROUP BY 1
                                                       )
                                                        SELECT
                                                               s.user_key,
                                                               v.vod_time_watched_min,
                                                               (SELECT count(*) FROM signups) as total_signups
                                                        FROM signups s
                                                        LEFT JOIN viewership v on s.user_key = v.user_key
                                                        WHERE v.vod_time_watched_min < 500 or v.vod_time_watched_min is null;
                                                       """)
        at_risk_members = []
        for result in results:
            at_risk_members.append({
                'user_key': str(result[0]),
                'min_watched': result[1],
                'total_population': result[2]
            })

        self.loggerv3.success('Returned at risk members')
        return at_risk_members
