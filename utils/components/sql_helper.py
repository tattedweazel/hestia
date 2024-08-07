from sqlalchemy.types import VARCHAR, DATETIME, INTEGER, BOOLEAN, DATE, DECIMAL, BIGINT, NUMERIC

class SqlHelper:

    def array_to_sql_list(self, arr):
        return (',').join([f"'{x}'" for x in arr])


    def convert_datatype(self, datatype_str):
        if datatype_str == 'character varying':
            return VARCHAR
        elif datatype_str == 'timestamp without time zone':
            return DATETIME
        elif datatype_str == 'bigint':
            return BIGINT
        elif datatype_str == 'boolean':
            return BOOLEAN
        elif datatype_str == 'date':
            return DATE
        elif datatype_str == 'double precision':
            return DECIMAL
        elif datatype_str == 'integer':
            return INTEGER
        elif datatype_str == 'numeric':
            return NUMERIC
        else:
            raise f'datatype {datatype_str} does not have a mapping. Please create one'
