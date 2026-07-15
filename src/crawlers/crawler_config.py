from pydantic import model_validator

from src.configs.base_config import BaseConfig
from .errors import ConfigurationError

class CrawlerNetworkConfig(BaseConfig):
    timeout: float
    allow_redirects: bool
    max_waiting_delay: float
    default_waiting_delay: float

    @model_validator(mode="after")
    def validate_default_waiting_delay(self) -> CrawlerNetworkConfig:
        if self.default_waiting_delay > self.max_waiting_delay:
            raise ConfigurationError(
                "default_waiting_delay must be less than max_waiting_delay"
            )
        return self


class CrawlerConfig(BaseConfig):
    num_crawlers: int
    min_queue_size: int
    max_queue_size: int
    
    network: CrawlerNetworkConfig
        
    @model_validator(mode="after")
    def validate_min_queue_size(self) -> CrawlerConfig:
        if self.min_queue_size > self.max_queue_size:
            raise ConfigurationError("min_queue_size must be less than max_queue_size")
        return self