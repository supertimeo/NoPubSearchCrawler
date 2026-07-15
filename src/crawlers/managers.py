import socket
import time
from functools import lru_cache
from typing import Optional
from urllib.parse import ParseResult, urlparse

import requests
from diskcache import Cache
from loguru import logger
from protego import Protego

from .crawler_config import CrawlerConfig
from .errors import NetworkError

class Manager:
    pass


class NetworkManager(Manager):
    def __init__(self, config: CrawlerConfig):
        self.session = requests.Session()
        self.config = config

    def fetch_page(self, url):
        try:
            response = self.session.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}, timeout=self.config.network.timeout, allow_redirects=self.config.network.allow_redirects)
            response.raise_for_status()

        except requests.Timeout as e:
            raise NetworkError(f"Timeout while fetching {url}", retryable=True) from e

        except requests.exceptions.SSLError as e:
            raise NetworkError(f"SSL error while fetching {url}") from e

        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error while fetching {url}") from e

        except requests.exceptions.HTTPError as e:
            if not e.response:
                raise NetworkError(f"HTTP error while fetching {url})") from e

            status_code = e.response.status_code

            match status_code:
                case 400:
                    raise NetworkError(f"Bad request {url}") from e

                case 401:
                    raise NetworkError(f"Unauthorized {url}") from e

                case 403:
                    raise NetworkError(f"Forbidden {url}") from e

                case 404:
                    raise NetworkError(f"Not found {url}") from e

                case 410:
                    raise NetworkError(f"Gone {url}") from e

                case 408 | 429:
                    raise NetworkError(f"Temporary client error ({status_code}) while fetching {url}", retryable=True) from e

                case 500 | 502 | 503 | 504:
                    raise NetworkError(f"Server error ({status_code}) while fetching {url}", retryable=True) from e

                case _:
                    raise NetworkError(
                        f"HTTP error {status_code} while fetching {url}"
                    ) from e

        return response

    @lru_cache(maxsize=10_000)
    def is_resolvable(self, domain: str) -> bool:
        """
        Vérifie si un domaine est résolvable en tentant d'obtenir son adresse IP.

        Args:
            domain: Le domaine à vérifier.

        Returns:
            True si le domaine est résolvable, False sinon.
        """
        try:
            socket.setdefaulttimeout(self.config.network.timeout)
            socket.gethostbyname(domain)
            return True
        except socket.gaierror:
            return False
        except TimeoutError as e:
            raise NetworkError(
                f"DNS timeout for {domain}"
            ) from e


class RobotsTxtManager(Manager):
    def __init__(self, cache: Cache, network_manager: NetworkManager, config: CrawlerConfig):
        self.cache = cache
        self.parser_dict: dict[str, Protego] = {}

        self.config = config

        self.logger = logger.bind(class_name=self.__class__.__name__)

        self.network_manager = network_manager

    def get_robots_txt(self, url: str | ParseResult) -> Optional[str]:
        """
        Obtient le robots.txt d'un domaine.

        Args:
            url: L'URL à obtenir le robots.txt.

        Returns:
            Le robots.txt.
        """
        parsed_url = urlparse(url) if isinstance(url, str) else url

        self.logger.trace(f"Getting robots.txt for {parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path} ...")
        start_time = time.time()

        netloc = parsed_url.netloc
        robots_txt_url = f"{parsed_url.scheme}://{netloc}/robots.txt"

        if netloc in self.cache:
            return self.cache[netloc] if self.cache[netloc] else None

        try:
            response = self.network_manager.fetch_page(robots_txt_url)
        except NetworkError as e:
            self.logger.debug(f"Error while fetching robots.txt for {robots_txt_url}: {e}")
            if not e.retryable:
                self.cache[netloc] = None
            return None

        robots_txt = response.text
        self.cache[netloc] = robots_txt

        self.logger.trace(f"Get robots.txt for {parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path} in {time.time() - start_time} seconds")
        return robots_txt

    def get_parser(self, url: str | ParseResult) -> Optional[Protego]:
        parsed_url = urlparse(url) if isinstance(url, str) else url

        robots_txt = self.get_robots_txt(parsed_url)
        if robots_txt is None:
            return None

        if parsed_url.netloc not in self.parser_dict:
            self.parser_dict[parsed_url.netloc] = Protego.parse(robots_txt)
        return self.parser_dict[parsed_url.netloc]

    @lru_cache(maxsize=10_000)
    def get_crawl_delay(self, url: str | ParseResult) -> float:
        parser = self.get_parser(url)
        if parser is None:
            return self.config.network.default_waiting_delay
        return min(parser.crawl_delay("*") or self.config.network.default_waiting_delay, self.config.network.max_waiting_delay)

    def is_allowed(self, url: str) -> bool:
        parser = self.get_parser(url)
        if parser is None:
            return True
        return parser.can_fetch(url, "*")