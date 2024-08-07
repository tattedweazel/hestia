

class ResponseCodeException(Exception):

    def __init__(self, response):
        super(ResponseCodeException, self).__init__(response)
        self.code = self.get_code(response)
        self.message = self.get_message(response)


    def __str__(self):
        return f'{self.code}: {self.message}'


    def get_code(self, response):
        if 'code' in response:
            return response['code']
        elif 'status_code' in response:
            return response['status_code']
        else:
            return None

    def get_message(self, response):
        if 'message' in response:
            return response['message']
        elif 'text' in response:
            return response['text']
        else:
            return None


class MissingBrazeRecordsException(Exception):

    def __init__(self):
        super(MissingBrazeRecordsException, self).__init__()
        self.message = 'self.records_to_update must contain all unique emails from Braze exported records'


    def __str__(self):
        return self.message


class CustomEventMergeException(Exception):

    def __init__(self, event_name1, event_name2):
        super(CustomEventMergeException, self).__init__(event_name1, event_name2)
        self.message = f'The custom events being merged ({event_name1}, {event_name2}) are not the same'


    def __str__(self):
        return self.message


class S3ContentsException(Exception):

    def __init__(self):
        super(S3ContentsException, self).__init__()
        self.message = f'The S3 prefix provided does not contain any contents. Check the S3 prefix and S3 bucket "sub-directories" in AWS.'


    def __str__(self):
        return self.message


class DuplicateSupportingCastMemberException(Exception):

    def __init__(self):
        super(DuplicateSupportingCastMemberException, self).__init__()
        self.message = f'Duplicate supporting cast members.'


    def __str__(self):
        return self.message


class DuplicateSupportingCastEpisodeException(Exception):

    def __init__(self):
        super(DuplicateSupportingCastEpisodeException, self).__init__()
        self.message = 'Duplicate supporting cast episodes for email, download date, and episode'


    def __str__(self):
        return self.message
