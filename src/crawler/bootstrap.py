from __future__ import annotations

import json
import os
import shutil
import sys
import threading
import time
from argparse import Namespace
from dataclasses import dataclass
from queue import PriorityQueue
from typing import cast, TYPE_CHECKING, Optional, overload, Literal

from diskcache import Cache
from loguru import logger
from rbloom import Bloom
from sqlalchemy import Engine, select
from sqlalchemy.orm import scoped_session, Session as ASession
from watchdog.observers import Observer

if TYPE_CHECKING:
    from watchdog.observers import BaseObserver

from src.common.errors import ConfigurationError, InitializationError, MissingEnvironmentVariableError
from src.common.paths import assets_folder_path, backup_folder_path, cache_folder_path, crawler_config_file_path
from src.configs.crawler_config import CrawlerConfig
from src.database.model import URL, CrawledURL
from src.database.session import create_db_engine, create_session_factory, init_schema
from .engine import Crawler, ConfigFileEventHandler, FixedList, QueueRecharger
from .log_levels import LoggingLevels


def validate_environment() -> None:
    required_variables = ["DB_USERNAME", "DB_PASSWORD", "DB_NAME", "DB_HOST", "DB_PORT"]

    errors: list[ConfigurationError] = [
        MissingEnvironmentVariableError(f"Missing required environment variable: {variable}")
        for variable in required_variables
        if os.getenv(variable) is None
    ]

    if os.getenv("DB_PORT") is not None and not cast(str, os.getenv("DB_PORT")).isdigit():
        errors.append(
            ConfigurationError("The DB_PORT environment variable must be a valid port number.")
        )

    if errors:
        raise ExceptionGroup("Failed to initialize database configuration", errors)


@dataclass
class CrawlerDependencies:
    engine: Engine
    session_factory: scoped_session[ASession]
    cache: Cache
    crawled_urls_bf: Bloom
    crawled_urls_bf_lock: threading.Lock
    queue: PriorityQueue[tuple[float, str]]


def init_db(args: Namespace) -> tuple[Engine, scoped_session[ASession]]:
    logger.info("Initializing database...")
    start_time = time.time()

    engine = create_db_engine()
    Session = create_session_factory(engine)

    init_schema(engine, drop_existing=args.delete_db or args.delete_all)

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
        for seed in json.load(f)["crawler_seeds"]:
            queue.put((0, seed["url"]))

    logger.success(f"Queue initialized successfully in {time.time() - start_time}")

    return queue


def init_cache(args: Namespace) -> Cache:
    if args.delete_cache or args.delete_all and cache_folder_path.exists():
        shutil.rmtree(cache_folder_path)
        cache_folder_path.mkdir()
    return Cache(str(cache_folder_path / "robot_txts_cache"))


def init_config(crawlers: list[Crawler], queue_recharger: QueueRecharger) -> BaseObserver:
    observer = Observer()
    observer.schedule(ConfigFileEventHandler(crawlers, queue_recharger), path=".", recursive=False)

    observer.start()
    return observer


def build_dependencies(args: Namespace, cache: Cache | None = None) -> CrawlerDependencies:
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

    if cache is None:
        try:
            cache = init_cache(args)
        except Exception as e:
            raise InitializationError("Failed to initialize cache") from e

    return CrawlerDependencies(engine, Session, cache, crawled_urls_bf, crawled_urls_bf_lock, queue)

@overload
def launch_crawler(args: Namespace, stop_events: list[threading.Event], pause_events: list[threading.Event], cache: Cache | None, return_crawlers: Literal[False]) -> None: ...

@overload
def launch_crawler(args: Namespace, stop_events: list[threading.Event], pause_events: list[threading.Event], cache: Cache | None, return_crawlers: Literal[True]) -> tuple[list[dict[Crawler, tuple[threading.Event, threading.Event]]], dict[QueueRecharger, tuple[threading.Event, threading.Event]], BaseObserver, Bloom]: ...

def launch_crawler(args: Namespace, stop_events: list[threading.Event], pause_events: list[threading.Event], cache: Cache | None = None, return_crawlers: bool = False) -> Optional[tuple[list[dict[Crawler, tuple[threading.Event, threading.Event]]], dict[QueueRecharger, tuple[threading.Event, threading.Event]], BaseObserver, Bloom]]:
    # création des threads
    crawlers = []
    config = CrawlerConfig.load_from_yml(crawler_config_file_path)
    domain_crawl_time = {}
    domain_crawl_time_lock = threading.Lock()
    crawling_urls = FixedList(config.num_crawlers, "")
    crawling_urls_lock = threading.Lock()

    remove_params = [
        'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
        'gclid', 'fbclid', 'ref', 'source', 'yclid', '_ga'
    ]

    with logger.catch(level=LoggingLevels.FATAL, message="Initialization failed !", onerror=lambda _: sys.exit(-1)):
        deps = build_dependencies(args, cache)

    logger.info("Launching crawler...")
    start_time = time.time()
    with logger.catch(level=LoggingLevels.FATAL, message="Launching crawler failed", onerror=lambda _: sys.exit(-1)):
        for thread_id, stop_event, pause_event in zip(range(config.num_crawlers), stop_events[:-1], pause_events[:-1]):
            crawler = Crawler(
                deps.queue, deps.session_factory, deps.cache, stop_event, pause_event, thread_id, crawling_urls,
                crawling_urls_lock, domain_crawl_time, domain_crawl_time_lock, deps.crawled_urls_bf,
                remove_params, deps.crawled_urls_bf_lock,
            )
            crawler.start()
            crawlers.append({crawler: (stop_event, pause_event)})
    logger.success(f"Crawlers initialized successfully in {time.time() - start_time}")

    queue_recharger = QueueRecharger(deps.queue, deps.session_factory, stop_events[-1], pause_events[-1])
    queue_recharger.start()

    observer = init_config(crawlers, queue_recharger)

    # attente de la fin des threads
    if return_crawlers:
        return crawlers, {queue_recharger: (stop_events[-1], pause_events[-1])}, observer, deps.crawled_urls_bf
        
    for t in crawlers:
        t.join()

    queue_recharger.join()

    observer.stop()
    observer.join()

    logger.success("All crawler finished sucessfully!")
    return None
