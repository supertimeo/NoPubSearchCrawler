class CrawlError(Exception):
    pass


class RobotsError(CrawlError):
    pass


class NetworkError(CrawlError):
    def __init__(self, *args, retryable: bool = False):
        super().__init__(*args)
        self.retryable = retryable


class DatabaseError(Exception):
    pass


class InitializationError(Exception):
    pass


class ConfigurationError(InitializationError):
    """Base class for configuration related failures."""


class MissingEnvironmentVariableError(ConfigurationError):
    """Raised when a required environment variable is missing."""