class DatabaseError(Exception):
    pass


class InitializationError(Exception):
    pass


class ConfigurationError(InitializationError, ValueError):
    """Base class for configuration related failures."""


class MissingEnvironmentVariableError(ConfigurationError):
    """Raised when a required environment variable is missing."""
