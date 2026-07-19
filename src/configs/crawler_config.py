from pydantic import model_validator, Field

from src.common.base_config import BaseConfig
from src.common.errors import ConfigurationError


class CrawlerNetworkConfig(BaseConfig):
    timeout: float
    allow_redirects: bool
    max_waiting_delay: float
    default_waiting_delay: float
    user_agent: str
    bot_name: str
    max_retry_per_request: int
    max_retry_per_url: int
    max_retry_per_domain: int
    retry_backoff_factor: float

    @model_validator(mode="after")
    def validate_default_waiting_delay(self) -> CrawlerNetworkConfig:
        if self.default_waiting_delay > self.max_waiting_delay:
            raise ConfigurationError(
                "default_waiting_delay must be less than max_waiting_delay"
            )
        return self
    
    
class CrawlerURLConfig(BaseConfig):
    remove_params: list[str] = Field(min_length=1)


class CrawlerConfig(BaseConfig):
    num_crawlers: int
    min_queue_size: int
    max_queue_size: int
    
    network: CrawlerNetworkConfig
    url: CrawlerURLConfig
        
    @model_validator(mode="after")
    def validate_min_queue_size(self) -> CrawlerConfig:
        if self.min_queue_size > self.max_queue_size:
            raise ConfigurationError("min_queue_size must be less than max_queue_size")
        return self