def byte_helper(column):
    if isinstance(column, bytearray) or isinstance(column, bytes):
        return column.decode('utf-8')
    else:
        return column
