class CrawlError(Exception):
    pass


class RobotsError(CrawlError):
    pass


class NetworkError(CrawlError):
    def __init__(self, *args, retryable: bool = False):
        super().__init__(*args)
        self.retryable = retryable
