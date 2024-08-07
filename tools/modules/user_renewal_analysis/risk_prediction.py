from utils.connectors.database_connector import DatabaseConnector
from utils.components.loggerv3 import Loggerv3


class RiskPrediction:

    def __init__(self, start_date, threshold, excluded_users):
        self.start_date = start_date
        self.threshold = threshold
        self.excluded_users = excluded_users
        self.db_connector = DatabaseConnector('')
        self.loggerv3 = Loggerv3(name=__name__, file_location='', local_mode=1)
        self.results = []
        self.loggerv3.alert = False


    def get_results_by_range(self, range):
        range_results = []

        rp_threshold = None
        np_threshold = None
        ftp_threshold = None
        if range == 7:
            rp_threshold = self.threshold
            np_threshold = self.threshold
            ftp_threshold = self.threshold
        elif range == 14:
            rp_threshold = self.threshold
            np_threshold = self.threshold
            ftp_threshold = self.threshold

        results = self.db_connector.read_redshift(f"""
                                                        WITH sub_types as (
                                                            SELECT sub_type
                                                            FROM warehouse.daily_user_renewal_cohorts
                                                            GROUP BY 1
                                                        ), topp as (
                                                            SELECT user_key,
                                                                   signup_date,
                                                                   sub_type,
                                                                   (CASE
                                                                       WHEN sub_type = 'Returning Paid' THEN {rp_threshold}
                                                                       WHEN sub_type = 'New Paid' THEN {np_threshold}
                                                                       WHEN sub_type = 'FTP Conversion' THEN {ftp_threshold}
                                                                    END) as threshold,
                                                                   sum(vod_time_watched_min) as vod_min
                                                            FROM warehouse.daily_user_renewal_cohorts
                                                            WHERE total_sub_days > 31
                                                              and days_into_sub <= {range}
                                                              and signup_date >= '{self.start_date}' and signup_date < dateadd('day', 1, '{self.start_date}')
                                                              and user_key not in ({','.join(self.excluded_users)})
                                                            GROUP BY 1, 2, 3, 4
                                                        ), top_left as (
                                                            SELECT sub_type,
                                                                   count(*) as renew_members_met_threshold
                                                            FROM topp
                                                            WHERE vod_min >= threshold
                                                            GROUP BY 1
                                                        ), top_right as (
                                                            SELECT sub_type,
                                                                   count(*) as renew_members_not_met_threshold
                                                            FROM topp
                                                            WHERE vod_min < threshold
                                                            GROUP BY 1
                                                        ), bottom as (
                                                            SELECT user_key,
                                                                   signup_date,
                                                                   sub_type,
                                                                    (CASE
                                                                       WHEN sub_type = 'Returning Paid' THEN {rp_threshold}
                                                                       WHEN sub_type = 'New Paid' THEN {np_threshold}
                                                                       WHEN sub_type = 'FTP Conversion' THEN {ftp_threshold}
                                                                    END) as threshold,
                                                                   sum(vod_time_watched_min) as vod_min
                                                            FROM warehouse.daily_user_non_renewal_cohorts
                                                            WHERE total_sub_days <= 31
                                                              and days_into_sub <= {range}
                                                              and signup_date >= '{self.start_date}' and signup_date < dateadd('day', 1, '{self.start_date}')
                                                              and user_key not in ({','.join(self.excluded_users)})
                                                            GROUP BY 1, 2, 3, 4
                                                        ), bottom_left as (
                                                            SELECT sub_type,
                                                                   count(*) as not_renew_members_met_threshold
                                                            FROM bottom
                                                            WHERE vod_min >= threshold
                                                            GROUP BY 1
                                                        ), bottom_right as (
                                                            SELECT sub_type,
                                                                   count(*) as not_renew_members_not_met_threshold
                                                            FROM bottom
                                                            WHERE vod_min < threshold
                                                            GROUP BY 1
                                                        ), agg as (
                                                            SELECT
                                                                st.sub_type,
                                                                CASE
                                                                  WHEN tl.renew_members_met_threshold IS NULL THEN 0 ELSE tl.renew_members_met_threshold
                                                                END as top_left,
                                                                CASE
                                                                  WHEN tr.renew_members_not_met_threshold IS NULL THEN 0 ELSE tr.renew_members_not_met_threshold
                                                                END as top_right,
                                                                CASE
                                                                  WHEN bl.not_renew_members_met_threshold IS NULL THEN 0 ELSE bl.not_renew_members_met_threshold
                                                                END as bottom_left,
                                                                CASE
                                                                  WHEN br.not_renew_members_not_met_threshold IS NULL THEN 0 ELSE br.not_renew_members_not_met_threshold
                                                                END as bottom_right
                                                            FROM sub_types st
                                                            LEFT JOIN top_left tl on st.sub_type = tl.sub_type 
                                                            LEFT JOIN top_right tr on st.sub_type = tr.sub_type
                                                            LEFT JOIN bottom_left bl on st.sub_type = bl.sub_type
                                                            LEFT JOIN bottom_right br on st.sub_type = br.sub_type
                                                        ), hits_misses as (
                                                          SELECT
                                                            sub_type,
                                                            top_left + bottom_right as hits,
                                                            top_right + bottom_left as misses
                                                          FROM agg
                                                        )
                                                        SELECT sub_type,
                                                               (CASE
                                                                WHEN hits + misses = 0 THEN 0
                                                                ELSE round((hits*1.0) / (hits + misses), 2)
                                                               END) as accuracy,
                                                               (hits + misses) as total,
                                                               hits,
                                                               misses 
                                                        FROM hits_misses
                                                        ORDER BY 1 desc;
                                                    """
                                                )

        for result in results:
            range_results.append({
                'signup_date': self.start_date,
                'sub_type': result[0],
                'range': range,
                'threshold': self.threshold,
                'accuracy': result[1],
                'total': result[2],
                'hits': result[3],
                'misses': result[4]
            })

        return range_results


    def get_results(self):
        self.loggerv3.info(f'Running risk prediction on {self.start_date} for {self.threshold} minute threshold')
        for r in [7, 14]:
            self.results.extend(self.get_results_by_range(r))
        self.loggerv3.success("All Processing Complete!")


    def display_results(self, type):
        self.get_results()
        for result in self.results:
            if result["sub_type"] == type:
                print(f'{result["range"]}-day range,  {result["accuracy"]}')

