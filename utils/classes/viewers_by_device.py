import config
from utils.connectors.database_connector import DatabaseConnector


class ViewersByDevice:

    def __init__(self, start_date, end_date):
        """Get viewers by <device> only by time range"""
        self.db_connector = DatabaseConnector(config.file_location)
        self.start_date = start_date
        self.end_date = end_date
        self.where_clause = ''
        self.platform = None


    def trigger(self, device):
        if device == 'mobile_web':
            self.platform = 'web'
            self.where_clause = """
            on_mobile_device is True
            """
        elif device == 'desktop':
            self.platform = 'web'
            self.where_clause = """
            on_mobile_device is False
			"""
        elif device == 'ios':
            self.platform = 'ios'
            self.where_clause = """
            platform = 'ios'
            """
        elif device == 'android':
            self.platform = 'android'
            self.where_clause = """
            platform = 'android'
            """


    def get_viewers(self, device):
        self.trigger(device)
        records = []
        query = f"""
			WITH device as (
                SELECT
                    user_key
                FROM warehouse.vod_sessions
                WHERE 
                    platform = '{self.platform}' AND
                    user_key is not NULL AND
                    start_timestamp >= '{self.start_date}' AND
                    start_timestamp < '{self.end_date}' AND
                    {self.where_clause} 
                GROUP BY 1
		    ), not_device as (
		        SELECT
                    user_key
                FROM warehouse.vod_sessions
                WHERE 
                    user_key is not NULL AND
                    start_timestamp >= '{self.start_date}' AND
                    start_timestamp < '{self.end_date}' AND
                    NOT {self.where_clause} 
                GROUP BY 1
		    )
		    SELECT user_key
		    FROM device
		    WHERE user_key NOT in (SELECT user_key FROM not_device);
		"""
        print(query)
        results = self.db_connector.read_redshift(query)

        for result in results:
            records.append(result[0])

        return records


    def get_viewers_by_day(self, device):
        self.trigger(device)
        records = []
        query = f"""
    			WITH device as (
                    SELECT
                        cast(start_timestamp as varchar(10)),
                        user_key
                    FROM warehouse.vod_sessions
                    WHERE 
                        platform = '{self.platform}' AND
                        user_key is not NULL AND
                        start_timestamp >= '{self.start_date}' AND
                        start_timestamp < '{self.end_date}' AND
                        {self.where_clause} 
                    GROUP BY 1, 2
    		    ), not_device as (
    		        SELECT
                        cast(start_timestamp as varchar(10)),
                        user_key
                    FROM warehouse.vod_sessions
                    WHERE 
                        user_key is not NULL AND
                        start_timestamp >= '{self.start_date}' AND
                        start_timestamp < '{self.end_date}' AND
                        NOT {self.where_clause} 
                    GROUP BY 1
    		    )
    		    SELECT user_key
    		    FROM device
    		    WHERE user_key NOT in (SELECT user_key FROM not_device);
    		"""
        # print(query)
        results = self.db_connector.read_redshift(query)

        for result in results:
            records.append(result[0])

        return records
