# encoding: utf-8

from __future__ import annotations

import json
import os
import re
import shutil
import socket
import sys
# import standard
import time
from argparse import Namespace
from contextlib import contextmanager
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from queue import PriorityQueue
from typing import Optional, TYPE_CHECKING, cast, Generator
from urllib.parse import urlparse, urljoin, urlunparse, ParseResult, parse_qsl, urlencode, unquote

# import pour le scraping et le crawling
import requests
import urllib3
from dotenv import load_dotenv
# import pour la gestion des logs
from loguru import logger
from protego import Protego
from pydantic.v1.dataclasses import dataclass
from selectolax.parser import HTMLParser
from sqlalchemy import String, Text, ForeignKey, create_engine, select, exists, delete, Engine, \
    UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL as DB_URL
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, scoped_session, sessionmaker, \
    Session as ASession

if TYPE_CHECKING:
    from loguru import Record

# import pour la gestion des threads
import threading

# import pour la gestion du cache
from diskcache import Cache

# import pour les arguments de la ligne de commande
import argparse

# imports pour le bloom filter
from rbloom import Bloom

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

WAITING_DELAY = 5
NUM_CRAWLERS = 15

root_folder_path = Path(__file__).parent.parent
log_folder_path = root_folder_path / "logs"
cache_folder_path = root_folder_path / "caches"
backup_folder_path = root_folder_path / "backups"
assets_folder_path = root_folder_path / "assets"

class FixedList[T]:
    def __init__(self, size: int, value: T):
        self._data = [value] * size

    def __getitem__(self, i: int) -> T:
        return self._data[i]

    def __setitem__(self, i: int, value: T):
        self._data[i] = value

    def __len__(self) -> int:
        return len(self._data)


class Base(DeclarativeBase):
    pass


class URL(Base):
    __tablename__ = "urls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(1024), unique=True, index=True)


class Page(Base):
    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    url_id: Mapped[int] = mapped_column(
        ForeignKey("urls.id"),
        unique=True
    )

    url: Mapped["URL"] = relationship(cascade="")

    title: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True
    )

    content: Mapped[str] = mapped_column(Text)

    links: Mapped[list["Link"]] = relationship(
        back_populates="page",
        cascade="all, delete-orphan"
    )


class Link(Base):
    __tablename__ = "links"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    page_id: Mapped[int] = mapped_column(
        ForeignKey("pages.id")
    )

    url_id: Mapped[int] = mapped_column(
        ForeignKey("urls.id")
    )

    page: Mapped[Page] = relationship(
        back_populates="links",
        cascade=""
    )

    url: Mapped[URL] = relationship()

    __table_args__ = (
        UniqueConstraint("page_id", "url_id"),
    )


class WaitingURL(Base):
    __tablename__ = "waiting_list"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url_id: Mapped[int] = mapped_column(
        ForeignKey("urls.id"),
        unique=True
    )
    url: Mapped["URL"] = relationship(cascade="")
    domain_crawled_at: Mapped[float] = mapped_column(index=True)


class CrawledURL(Base):
    __tablename__ = "crawled_urls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url_id: Mapped[int] = mapped_column(
        ForeignKey("urls.id"),
        unique=True
    )
    url: Mapped["URL"] = relationship(cascade="")


@dataclass
class CrawlResult:
    title: str
    content: str
    links: set[str]
    timestamp: float


class CrawlError(Exception):
    pass


class LoggingLevels(StrEnum):
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"


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


class Manager:
    pass


class NetworkManager(Manager):
    def __init__(self):
        self.session = requests.Session()

    def fetch_page(self, url):
        try:
            response = self.session.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}, timeout=5, allow_redirects=False)
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

    @staticmethod
    @lru_cache(maxsize=10_000)
    def is_resolvable(domain: str) -> bool:
        """
        Vérifie si un domaine est résolvable en tentant d'obtenir son adresse IP.

        Args:
            domain: Le domaine à vérifier.

        Returns:
            True si le domaine est résolvable, False sinon.
        """
        try:
            socket.setdefaulttimeout(5)
            socket.gethostbyname(domain)
            return True
        except socket.gaierror:
            return False
        except TimeoutError as e:
            raise NetworkError(
                f"DNS timeout for {domain}"
            ) from e


class RobotsTxtManager(Manager):
    def __init__(self, cache: Cache, network_manager: NetworkManager):
        self.cache = cache
        self.parser_dict: dict[str, Protego] = {}

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
            if not e.retryable: self.cache[netloc] = None
            return None

        robots_txt = response.text
        self.cache[netloc] = robots_txt

        self.logger.trace(f"Get robots.txt for {parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path} in {time.time() - start_time} seconds")
        return robots_txt

    def get_parser(self, url: str | ParseResult) -> Optional[Protego]:
        parsed_url = urlparse(url) if isinstance(url, str) else url

        robots_txt = self.get_robots_txt(parsed_url)
        if robots_txt is None: return None

        if parsed_url.netloc not in self.parser_dict:
            self.parser_dict[parsed_url.netloc] = Protego.parse(robots_txt)
        return self.parser_dict[parsed_url.netloc]

    @lru_cache(maxsize=10_000)
    def get_crawl_delay(self, url: str | ParseResult) -> float:
        parser = self.get_parser(url)
        if parser is None: return WAITING_DELAY
        return min(parser.crawl_delay("*") or WAITING_DELAY, 60)

    def is_allowed(self, url: str) -> bool:
        parser = self.get_parser(url)
        if parser is None: return True
        return parser.can_fetch(url, "*")


class QueueRecharger(threading.Thread):
    def __init__(self, queue: PriorityQueue[tuple[float, str]], Session: scoped_session[ASession], stop_event: threading.Event):
        threading.Thread.__init__(self)
        self.Session = Session
        self.queue = queue
        self.stop_event = stop_event
        self.id = NUM_CRAWLERS + 1

        self.logger = logger.bind(class_name=self.__class__.__name__)

    def run(self):
        with self.logger.catch(level=LoggingLevels.CRITICAL, message=f"A fatal error an occured while running the running loop of the QueueRecharger {self.name} ({self.native_id})"):
            self.logger.info("QueueRecharger started.")
            while not self.stop_event.is_set():
                if self.queue.qsize() < 200:
                    time.sleep(10)
                    with self.logger.catch(message="Error while recharging the queue"):
                        self.logger.debug("Recharging the queue...")
                        start_time = time.time()
                        self.recharge_queue()
                        self.logger.debug(f"Queue recharged in {time.time() - start_time} seconds")

    def recharge_queue(self):
        with self.Session() as session:
            urls = session.execute(select(URL.url, WaitingURL.domain_crawled_at, WaitingURL.id).join(WaitingURL.url).order_by(WaitingURL.domain_crawled_at.asc()).limit(1000 - self.queue.qsize())).all()
        ids = [url[2] for url in urls]

        if not ids: return

        for url in urls : self.queue.put((url[1], url[0]))
        with self.Session() as session, self.logger.catch(message="Error while recharging the queue", onerror=lambda _: session.rollback()):
            session.execute(delete(WaitingURL).where(WaitingURL.id.in_(ids)))
            session.commit()


class Crawler(threading.Thread):
    def __init__(self, queue: PriorityQueue[tuple[float, str]], Session: scoped_session[ASession],
                 cache: Cache, stop_event: threading.Event, id: int, crawling_urls: FixedList[str], crawling_urls_lock: threading.Lock, domain_crawl_time: dict[str, float],
                 domain_crawl_time_lock: threading.Lock, crawled_urls_bf: Bloom, remove_params: list[str],
                 crawled_urls_bf_lock: threading.Lock):
        threading.Thread.__init__(self)
        self.Session = Session
        self.queue = queue
        self.stop_event = stop_event
        self.crawling_urls = crawling_urls
        self.crawling_urls_lock = crawling_urls_lock
        self.domain_crawl_time = domain_crawl_time
        self.domain_crawl_time_lock = domain_crawl_time_lock
        self.name = f"Crawler-{id+1}"
        self.crawled_urls_bf = crawled_urls_bf
        self.crawler_id = id
        self.remove_params = remove_params
        self.cache = cache

        self.crawled_urls_bf_lock = crawled_urls_bf_lock

        self.logger = logger.bind(class_name=self.__class__.__name__)

        self.network_manager = NetworkManager()
        self.robots_txt_manager = RobotsTxtManager(self.cache, self.network_manager)

    @contextmanager
    def db_transaction(self, autocommit: bool = False) -> Generator[ASession]:
        with self.Session() as session:
            try:
                yield session
                if autocommit: session.commit()
            except Exception as e:
                session.rollback()
                raise DatabaseError("An error occurred while inserting a URL in database") from e

    def get_pure_url(self, url: str) -> str:
        # 1. Analyser l'URL
        parsed = urlparse(url)

        # 2. Normalisation du schéma et domaine (netloc)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Suppression des ports par défaut inutiles (ex: example.com:80 -> example.com)
        if scheme == "http" and netloc.endswith(":80"):
            netloc = netloc[:-3]
        elif scheme == "https" and netloc.endswith(":443"):
            netloc = netloc[:-4]

        # 3. Normalisation du chemin (path)
        # MAGIE ICI : On décode les %D0%98 en vrais caractères (ex: cyrillique, accents...)
        path = unquote(parsed.path)

        # Remplacement des slashes multiples par un seul (ex: /wiki//test -> /wiki/test)
        path = re.sub(r'//+', '/', path)

        # Retrait du slash final si ce n'est pas la racine
        if len(path) > 1 and path.endswith('/'):
            path = path[:-1]

        # 4. Traitement des paramètres (Query String)
        query_params = parse_qsl(parsed.query, keep_blank_values=True)

        # Filtrer et trier
        filtered_params = [
            (key, value) for key, value in query_params
            if key.lower() not in self.remove_params
        ]
        filtered_params.sort()

        # Reconstruction de la query string
        new_query = urlencode(filtered_params)

        # 5. Reconstruction de l'URL propre sans le fragment ('')
        return urlunparse((scheme, netloc, path, parsed.params, new_query, ''))

    def url_is_crawled(self, url: str) -> bool:
        """
        Vérifie si une URL a déjà été crawlée.

        Args:
            url: L'URL à vérifier.

        Returns:
            True si l'URL a déjà été crawlée, False sinon.
        """
        with self.db_transaction() as session:
            return url in self.crawled_urls_bf and cast(bool, session.scalar(select(exists().join(CrawledURL.url).where(URL.url == url))))

    @staticmethod
    def extract_main_content(tree: HTMLParser) -> str:
        """
        Extrait le contenu textuel principal d'un arbre HTML parsé avec selectolax.

        Cette fonction tente d'abord de localiser les balises de contenu principales
        comme <main> ou <article>. Si elles ne sont pas trouvées, elle se rabat sur
        le <body> entier. Dans tous les cas, elle supprime les éléments non pertinents
        (nav, footer, etc.) avant de retourner le texte.

        Args:
            tree: L'objet HTMLParser de selectolax représentant la page HTML.

        Returns:
            Une chaîne de caractères contenant le contenu principal nettoyé.
        """
        # Sélecteurs CSS pour les conteneurs de contenu potentiels, par ordre de priorité
        main_content_selectors = ["main", "article", ".main-content", ".post", "#content", "#main"]

        main_element = None
        for selector in main_content_selectors:
            main_element = tree.css_first(selector)
            if main_element is not None:
                break

        # Si aucun conteneur principal n'est trouvé, utiliser le body comme base
        if main_element is None:
            main_element = tree.body
        if main_element is None:
            return "" # Retourner une chaîne vide si même le body est absent

        # Cloner l'élément pour ne pas modifier l'arbre original si ce n'est pas souhaité
        # C'est une bonne pratique, bien que selectolax ne fournisse pas de méthode de clonage directe.
        # Les opérations de suppression modifieront le `main_element`.

        # Sélecteurs des éléments à supprimer
        tags_to_remove = ["nav", "footer", "header", "aside", "script", "style", ".noprint"]

        for tag_selector in tags_to_remove:
            # Trouver tous les éléments correspondants dans le conteneur principal
            elements_to_remove = main_element.css(tag_selector)
            for element in elements_to_remove:
                element.decompose() # Supprime l'élément de l'arbre [1]

        # Extraire le texte de l'élément nettoyé
        # strip=True aide à enlever les espaces superflus en début et fin de chaque morceau de texte
        # separator=' ' ajoute un espace entre les blocs de texte pour une meilleure lisibilité
        return main_element.text(strip=True, separator=' ').replace('\x00', '')

    def insert_urls_in_waiting_list(self, session: ASession, urls: list[URL]) -> None:
        if not urls:
            return

        with self.domain_crawl_time_lock:
            domain_crawl_time = self.domain_crawl_time.copy()

        session.execute(insert(WaitingURL).values([
            {
                "url_id": url.id,
                "domain_crawled_at": domain_crawl_time.get(urlparse(url.url).netloc, time.time()),
            }
            for url in urls
        ]).on_conflict_do_nothing(index_elements=["url_id"]))

    def is_crawlable(self, url: str) -> bool:
        # Préparser l'URL
        parsed_url = urlparse(url)

        # Précalculer le netloc et le robots.txt
        netloc = parsed_url.netloc

        # Vérification si l'URL est crawlable
        if not (parsed_url.scheme in ["http", "https"] or netloc is not None) or netloc == "":
            self.logger.debug(f"URL {url} is not crawlable because it doesn't start with http or https or it doesn't have a netloc")
            return False

        # Vérification si l'URL est résolvable
        try:
            if not NetworkManager.is_resolvable(netloc):
                self.logger.debug(f"URL {url} is not crawlable because it's not resolvable")
                return False
        except NetworkError as e:
            self.logger.warning(f"A timeout occurred while resolving the domain {netloc} domain: {e}")

        # Vérification de la permission de crawler la page avec le robots.txt
        if not self.robots_txt_manager.is_allowed(url):
            self.logger.debug(f"The NoPubSearch crawler is not allowed to crawl {url}")
            return False

        return True

    def crawl(self, url: str) -> CrawlResult:
        # sourcery skip: de-morgan
        """
        Crawle une URL et retourne le titre, le contenu et les liens.

        Args:
            url: L'URL à crawler.

        Returns:
            Un tuple contenant le titre, le contenu et les liens.
        """
        # Préparser l'URL
        start_time = time.time()
        self.logger.trace(f"Getting {url}...")
        try:
            response = self.network_manager.fetch_page(url)
        except NetworkError:
            self.logger.opt(exception=True).warning("Network error while crawling")
            raise
        self.logger.trace(f"Get {url} in {time.time() - start_time} seconds")

        if not response.headers.get("Content-Type") or not response.headers.get("Content-Type").startswith("text/html"): # type: ignore
            self.logger.debug(f"URL {url} is not text/html page")
            raise CrawlError(f"URL {url} is not text/html page")

        # Analyse du contenu de la page selectolax beaucoup plus rapide que BeautifulSoup
        tree = HTMLParser(response.text)

        # Récupération du titre et du contenu de la page
        title = tree.css_first("title").text() if tree.css_first("title") is not None else "Sans titre" # type: ignore
        title = title.replace('\x00', '')[:512]

        # Récupération des liens de la page
        links = {
            pure_url
            for link in tree.css("a")
            if (href := link.attributes.get("href")) is not None
            if (full_url := urljoin(url, href))
            if urlparse(full_url).scheme in ("http", "https")
            if (pure_url := self.get_pure_url(full_url))
        }

        # TODO: Ajouter un parser pour le sitemap.xml

        content = self.extract_main_content(tree)

        # Retourne le titre, le contenu et les liens de la page
        self.logger.info(f"Crawled page : {url}")
        return CrawlResult(title=title, content=content, links=links, timestamp=time.time())

    @staticmethod
    def insert_urls_in_db(
            urls: set[str] | tuple[str, ...],
            session: ASession,
    ) -> dict[str, URL]:
        # Sécurité : si la page n'avait aucun lien, on arrête les frais
        if not urls:
            return {}

        urls_list = sorted(list(urls))

        session.execute(
            insert(URL)
            .values([{"url": url} for url in urls_list])
            .on_conflict_do_nothing(index_elements=["url"])
        )

        stmt = select(URL).where(URL.url.in_(urls_list))
        url_orm_obj = cast(list[URL], session.scalars(stmt).all())

        return {url.url: url for url in url_orm_obj}

    def run(self):
        """
        Lance le crawler.
        """
        with self.logger.catch(level=LoggingLevels.CRITICAL, message=f"A fatal error an occured while running loop of the crawler {self.name} ({self.native_id})"):
            self.logger.info(f"Crawler {self.name} started")
            time.sleep(5) # Attente de 5 secondes pour permettre le démarrage des threads

            while not self.stop_event.is_set():
                # Récupération d'une page à crawler
                if self.queue.empty(): continue
                domain_crawled_at, url = self.queue.get()
                self.logger.trace(f"Get {url} in the queue")

                delay = self.robots_txt_manager.get_crawl_delay(url)
                if domain_crawled_at + delay > time.time():
                    self.queue.put((domain_crawled_at, url))
                    self.logger.trace(f"{urlparse(url).netloc} is not ready for crawling")
                    time.sleep(0.5)
                    continue

                if not self.is_crawlable(url):
                    continue

                with self.crawled_urls_bf_lock:
                    # Vérification si la page a déjà été crawlée
                    if self.url_is_crawled(url):
                        self.logger.debug(f"{url} is already crawled")
                        continue

                with self.crawling_urls_lock:
                    if url in self.crawling_urls:
                        self.logger.trace(f"{url} The URL is already being crawled by another crawler.")
                        continue
                    self.crawling_urls[self.crawler_id] = url

                # Nettoyer l'URL
                url = self.get_pure_url(url)

                # Crawling de la page
                self.logger.debug(f"Crawling page {url}...")

                start_time = time.time()
                try:
                    crawl_result = self.crawl(url)
                except CrawlError as e:
                    if isinstance(e, NetworkError):
                        if e.retryable:
                            with self.domain_crawl_time_lock:
                                self.queue.put((self.domain_crawl_time.get(urlparse(url).netloc, time.time()), url))
                        else:
                            try:
                                with self.db_transaction(autocommit=True) as session:
                                    session.add(CrawledURL(url=list(self.insert_urls_in_db({url}, session).values())[0]))
                            except DatabaseError:
                                self.logger.exception(f"Error while adding {url} to crawled urls")
                            else:
                                self.crawled_urls_bf.add(url)
                    continue

                self.logger.debug(f"Crawled page {url} in {time.time() - start_time} seconds")

                with self.domain_crawl_time_lock:
                    self.domain_crawl_time[urlparse(url).netloc] = time.time() + delay

                # Vérification si le titre, le contenu et les liens sont valides
                if not crawl_result.title and not crawl_result.content and not crawl_result.links:
                    self.logger.warning(f"Invalid page {url}")
                    continue

                try:
                    with self.db_transaction(autocommit=True) as session:
                        all_urls = {url} | crawl_result.links

                        url_objs_dict = self.insert_urls_in_db(all_urls, session)

                        url_orm_obj = url_objs_dict[url]

                        # On récupère les objets des enfants (en filtrant les éventuels absents)
                        del url_objs_dict[url]
                        link_orm_objs = list(url_objs_dict.values())

                        link_orm_objs.sort(key=lambda u: u.id)

                        session.add(CrawledURL(url=url_orm_obj))
                        session.add(Page(url=url_orm_obj, title=crawl_result.title, content=crawl_result.content, links=[Link(url=link) for link in link_orm_objs]))
                        self.insert_urls_in_waiting_list(session, link_orm_objs)
                except DatabaseError:
                    self.logger.exception(f"Error while adding {url} to database")
                else:
                    self.crawled_urls_bf.add(url)


def parsing_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--delete-logs", action="store_true", help="Delete the logs")
    parser.add_argument("-c", "--delete-cache", action="store_true", help="Delete the cache")
    parser.add_argument("-d", "--delete-db", action="store_true", help="Delete the database")
    parser.add_argument("-a", "--delete-all", action="store_true", help="Delete the logs, the database and the cache")
    parser.add_argument("-v", "--vacuum", action="store_true", help="Vacuum the database")
    args = parser.parse_args()
    return args

def init_logger(args: argparse.Namespace):
    if args.delete_logs or args.delete_all and log_folder_path.exists():
            shutil.rmtree(log_folder_path)
            log_folder_path.mkdir()

    def log_location_patcher(record: Record):
        class_name = record["extra"].get("class_name")
        record["extra"][
            "location"] = f"{record['file'].name}{f":{class_name}" if class_name is not None else ""}{f":{record['function']}" if record['function'] != "<module>" else ""}:{record['line']}"

    def log_format(record: Record) -> str:
        return (
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
            "<level>{level: <8}</level> | "
            f"{record["extra"]["location"]} | "
            "{thread.name} ({thread.id}) - "
            "{message}\n"
            "{exception}"
        )

    # création du logger
    logger.remove()

    logger.level(
        LoggingLevels.FATAL,
        no=60,
        color="<white><bold><bg red>",
        icon="☠️",
    )

    logger.configure(patcher=log_location_patcher)

    logger.add(log_folder_path / "latest.log", rotation="100 MB", enqueue=True, compression="zip", level=LoggingLevels.INFO,
               format=log_format)
    logger.add(log_folder_path / "error.log", rotation="100 MB", enqueue=True, compression="zip", level=LoggingLevels.ERROR,
               format=log_format, backtrace=True, diagnose=True)
    logger.add(log_folder_path / "trace.log", rotation="100 MB", enqueue=True, compression="zip", level=LoggingLevels.TRACE,
               format=log_format, backtrace=True, diagnose=True)
    logger.add(sys.stdout, enqueue=True, level=LoggingLevels.TRACE, format=log_format, backtrace=True, diagnose=True)
    logger.info("Logger initialized")

def init_db(args: Namespace) -> tuple[Engine, scoped_session[ASession]]:
    logger.info("Initializing database...")
    start_time = time.time()

    db_url = DB_URL.create(
    "postgresql+psycopg",
    username=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"),
    host="localhost",
    port=5432,
    database="nopubsearch",
)

    engine = create_engine(
        db_url,
        pool_size=20,  # Assez de connexions pour tes 15 crawlers + tes threads de fond
        max_overflow=10  # Une petite marge de sécurité
    )

    Session = scoped_session(sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
    ))

    if args.delete_db or args.delete_all:
        Base.metadata.drop_all(engine)

    Base.metadata.create_all(engine)

    logger.success(f"Database initialized successfully in {time.time() - start_time}")

    return engine, Session

def init_bloom_filter(Session: scoped_session[ASession]) -> tuple[Bloom, threading.Lock]:
    logger.info("Initializing crawled url bloom-filter...")
    start_time = time.time()

    crawled_urls_bf = Bloom(100_000, 0.001)
    with Session() as session:
        crawled_urls_bf.update(set(session.execute(select(URL.url).join(CrawledURL.url)).all()))
    crawled_urls_bf_lock = threading.Lock()

    logger.success(f"Crawled urls bloom-filter initialized successfully in {time.time() - start_time}")

    return crawled_urls_bf, crawled_urls_bf_lock

def init_queue() -> PriorityQueue:
    logger.info("Initializing queue...")
    start_time = time.time()

    queue = PriorityQueue[tuple[float, str]]()
    with open(assets_folder_path / "start_url_lists" / "start_urls.json", "r") as f:
        for id, url in enumerate(seed["url"] for seed in json.load(f)["crawler_seeds"]):
            queue.put((0, url))

    logger.success(f"Queue initialized successfully in {time.time() - start_time}")

    return queue

def init_cache(args: Namespace) -> Cache:
    if args.delete_cache or args.delete_all and cache_folder_path.exists():
        shutil.rmtree(cache_folder_path)
        cache_folder_path.mkdir()
    return Cache(str(cache_folder_path / "robot_txts_cache"))

def init() -> tuple[Engine, scoped_session[ASession], Cache, Bloom, threading.Lock, PriorityQueue[tuple[float, str]]]:
    args = parsing_arguments()

    init_logger(args)

    initialization_errors = []
    if os.getenv("DB_USERNAME") is None:
        initialization_errors.append(
            MissingEnvironmentVariableError(
                "Missing required environment variable: DB_USERNAME"
            )
        )
    if os.getenv("DB_PASSWORD") is None:
        initialization_errors.append(
            MissingEnvironmentVariableError(
                "Missing required environment variable: DB_PASSWORD"
            )
        )

    if initialization_errors:
        ExceptionGroup(
            "Failed to initialize database configuration",
            initialization_errors
        )

    backup_folder_path.mkdir(parents=True, exist_ok=True)

    try:
        engine, Session = init_db(args)
    except Exception as e:
        raise InitializationError("Failed to initialize database") from e

    try:
        crawled_urls_bf, crawled_urls_bf_lock = init_bloom_filter(Session)
    except Exception as e:
        raise InitializationError("Failed to initialize bloom filter") from e

    try:
        queue = init_queue()
    except Exception as e:
        raise InitializationError("Failed to initialize queue") from e

    try:
        cache = init_cache(args)
    except Exception as e:
        raise InitializationError("Failed to initialize cache") from e

    return engine, Session, cache, crawled_urls_bf, crawled_urls_bf_lock, queue

def main():
    # création des threads
    crawlers = []
    stop_event = threading.Event()
    domain_crawl_time = {}
    domain_crawl_time_lock = threading.Lock()
    crawling_urls = FixedList(NUM_CRAWLERS, "")
    crawling_urls_lock = threading.Lock()

    remove_params = [
        'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
        'gclid', 'fbclid', 'ref', 'source', 'yclid', '_ga'
    ]

    with logger.catch(level=LoggingLevels.FATAL, message="Initialization failed !", onerror=lambda _: sys.exit(-1)):
        engine, Session, cache, crawled_urls_bf, crawled_urls_bf_lock, queue = init()

    logger.info("Launching crawlers...")
    start_time = time.time()
    with logger.catch(level=LoggingLevels.FATAL, message="Launching crawlers failed", onerror=lambda _: sys.exit(-1)):
        for thread_id in range(NUM_CRAWLERS):
            crawler = Crawler(queue, Session, cache, stop_event, thread_id, crawling_urls, crawling_urls_lock, domain_crawl_time, domain_crawl_time_lock, crawled_urls_bf, remove_params, crawled_urls_bf_lock)
            crawler.start()
            crawlers.append(crawler)
    logger.success(f"Crawlers initialized successfully in {time.time() - start_time}")

    recharger = QueueRecharger(queue, Session, stop_event)
    recharger.start()

    while True:
        # noinspection PyBroadException
        try:
            time.sleep(1)
        except BaseException:
            stop_event.set()
            break

    # attente de la fin des threads
    for t in crawlers:
        t.join()
    recharger.join()

    logger.info("All crawlers finished")

if __name__ == "__main__":
    main()