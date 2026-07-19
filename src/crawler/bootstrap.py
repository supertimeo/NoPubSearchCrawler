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

import yappi
from diskcache import Cache
from loguru import logger
from rbloom import Bloom
from sqlalchemy import Engine, select, func
from sqlalchemy.orm import scoped_session, Session as ASession
from watchdog.observers import Observer

if TYPE_CHECKING:
    from watchdog.observers import BaseObserver

from src.common.errors import ConfigurationError, InitializationError, MissingEnvironmentVariableError
from src.common.paths import assets_folder_path, cache_folder_path, crawler_config_file_path
from src.configs.crawler_config import CrawlerConfig
from src.database.model import URL, CrawledURL
from src.database.session import create_db_engine, create_session_factory, init_schema
from .engine import Crawler, ConfigFileEventHandler, ThreadLocalURLTracker, QueueRecharger
from .log_levels import LoggingLevels


def validate_environment() -> None:
    """Valide la configuration de la base de données à partir des variables d'environnement requises. Vérifie la présence et la validité des valeurs nécessaires, puis agrège les erreurs de configuration éventuelles.

    Cette fonction s'assure que toutes les variables d'environnement indispensables à la connexion à la base de données sont définies et que le port fourni est un entier valide. En cas de problèmes, elle regroupe les erreurs détectées dans un `ExceptionGroup` et lève une exception pour empêcher l'initialisation avec une configuration invalide.

    Raises:
        ExceptionGroup: Si une ou plusieurs variables d'environnement manquent ou si la valeur de `DB_PORT` n'est pas un numéro de port valide.
    """
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
    """Regroupe les dépendances nécessaires au fonctionnement du crawler. Fournit un conteneur structuré pour partager les ressources initialisées entre les différentes parties du système de crawling.

    Cette classe permet de transporter l'engine de base de données, la fabrique de sessions, le cache, le bloom filter des URLs crawléess ainsi que la file de priorité et les verrous associés. Elle facilite ainsi l'initialisation et la réutilisation cohérente de ces composants dans les différentes fonctions de démarrage du crawler.

    Attributes:
        engine: Instance de moteur SQLAlchemy utilisée pour interagir avec la base de données.
        session_factory: Fabrique de sessions SQLAlchemy scopées pour créer des sessions de base de données.
        cache: Cache persistant utilisé pour stocker notamment les robots.txt ou autres métadonnées.
        crawled_urls_bf: Bloom filter contenant les URLs déjà explorées afin d'éviter les doublons.
        crawled_urls_bf_lock: Verrou associé au bloom filter pour garantir un accès thread-safe.
        queue: File de priorité contenant les URLs à crawler, ordonnées par leur priorité.
    """
    engine: Engine
    session_factory: scoped_session[ASession]
    cache: Cache
    crawled_urls_bf: Bloom
    crawled_urls_bf_lock: threading.Lock
    queue: PriorityQueue[tuple[float, str]]


def init_db(args: Namespace) -> tuple[Engine, scoped_session[ASession]]:
    """Initialise la base de données et prépare la fabrique de sessions pour le crawler. Crée le moteur SQLAlchemy, applique le schéma et gère éventuellement la suppression de la base existante selon les arguments fournis.

    Cette fonction configure l'accès à la base de données pour l'application en construisant le moteur et en initialisant le schéma. Elle renvoie ensuite le moteur et une fabrique de sessions scopées pour permettre aux autres composants de créer des sessions isolées.

    Args:
        args: Arguments de ligne de commande contenant notamment les indicateurs de suppression de la base de données.

    Returns:
        Un tuple contenant le moteur SQLAlchemy initialisé et la fabrique de sessions scopées associée.
    """
    logger.info("Initializing database...")
    start_time = time.time()

    engine = create_db_engine()
    session_factory = create_session_factory(engine)

    init_schema(engine, drop_existing=args.delete_db or args.delete_all)

    logger.success(f"Database initialized successfully in {time.time() - start_time}")

    return engine, session_factory


def init_bloom_filter(session_factory: scoped_session[ASession]) -> tuple[Bloom, threading.Lock]:
    """Initialise le bloom filter utilisé pour suivre les URLs déjà crawlées. Pré-remplit la structure avec les URLs présentes en base afin d’éviter de les re-crawler.

    Cette fonction construit un bloom filter en mémoire avec une capacité et une probabilité d’erreur prédéfinies, puis y ajoute toutes les URLs déjà enregistrées comme crawlées dans la base de données. Elle retourne ensuite le bloom filter initialisé accompagné d’un verrou permettant un accès thread-safe.

    Args:
        session_factory: Fabrique de sessions SQLAlchemy scopées utilisée pour récupérer les URLs déjà crawlées depuis la base de données.

    Returns:
        Un tuple contenant le bloom filter initialisé et le verrou de synchronisation associé.
    """
    logger.info("Initializing crawled url bloom-filter...")
    start_time = time.time()

    with session_factory() as session:
        crawled_urls_bf = Bloom(
            max(100_000_000, cast(int, session.scalar(select(func.count(URL.id)).execution_options(yield_per=10_000)))), 0.001
        )
        crawled_urls_bf.update(set(session.execute(select(URL.url).join(CrawledURL.url)).all()))
    crawled_urls_bf_lock = threading.Lock()

    logger.success(f"Crawled urls bloom-filter initialized successfully in {time.time() - start_time}")

    return crawled_urls_bf, crawled_urls_bf_lock


def init_queue() -> PriorityQueue:
    """Initialise la file de priorité des URLs à crawler. Charge les URLs de départ depuis le fichier de configuration et les insère avec une priorité initiale.

    Cette fonction lit la liste des URLs graines depuis le fichier `start_urls.json` et remplit une `PriorityQueue` avec chacune d’elles en leur attribuant une priorité de base. Elle retourne ensuite cette file prête à être utilisée par les threads du crawler.

    Returns:
        Une file de priorité contenant les URLs de départ à explorer.
    """
    logger.info("Initializing queue...")
    start_time = time.time()

    queue = PriorityQueue[tuple[float, str]]()
    with open(assets_folder_path / "start_url_lists" / "start_urls.json", "r") as f:
        for seed in json.load(f)["crawler_seeds"]:
            queue.put((0, seed["url"]))

    logger.success(f"Queue initialized successfully in {time.time() - start_time}")

    return queue


def init_cache(args: Namespace) -> Cache:
    """Initialise le cache persistant utilisé par le crawler. Gère éventuellement la suppression complète du cache existant en fonction des arguments de ligne de commande.

    Cette fonction vérifie si le cache doit être réinitialisé, supprime alors le dossier de cache et le recrée si nécessaire, puis instancie un objet `Cache` pointant vers l’emplacement prévu pour stocker les données comme les robots.txt. Elle renvoie ensuite ce cache prêt à être utilisé par les composants du crawler.

    Args:
        args: Arguments de ligne de commande indiquant notamment s’il faut supprimer le cache (`delete_cache` ou `delete_all`).

    Returns:
        Une instance de `Cache` initialisée dans le dossier de cache de l’application.
    """
    if args.delete_cache or args.delete_all and cache_folder_path.exists():
        shutil.rmtree(cache_folder_path)
        cache_folder_path.mkdir()
    return Cache(str(cache_folder_path / "robot_txts_cache"))


def init_config(crawlers: list[Crawler], queue_recharger: QueueRecharger) -> BaseObserver:
    """Initialise et démarre l’observateur de fichier de configuration du crawler. Configure un gestionnaire d’événements pour réagir aux modifications du fichier de configuration et adapter le comportement des crawlers en conséquence.

    Cette fonction crée un `Observer` de watchdog, y enregistre un `ConfigFileEventHandler` responsable de la gestion des changements de configuration, puis démarre l’observateur pour surveiller le répertoire courant. Elle renvoie l’instance d’observateur afin de permettre son arrêt ou sa gestion ultérieure par l’appelant.

    Args:
        crawlers: Liste des instances de `Crawler` dont la configuration pourra être mise à jour lors des changements du fichier de configuration.
        queue_recharger: Instance de `QueueRecharger` utilisée pour recharger ou ajuster la file de priorité en réponse aux modifications de configuration.

    Returns:
        L’observateur watchdog initialisé et démarré, chargé de surveiller le fichier de configuration.
    """
    observer = Observer()
    observer.schedule(ConfigFileEventHandler(crawlers, queue_recharger), path=".", recursive=False)

    observer.start()
    return observer


def build_dependencies(args: Namespace, cache: Cache | None = None) -> CrawlerDependencies:
    """Construit et assemble les dépendances nécessaires au démarrage du crawler. Orchestre l’initialisation de la base de données, du bloom filter, de la file de priorité et du cache pour fournir un ensemble cohérent de ressources.

    Cette fonction crée le dossier de sauvegarde si nécessaire, initialise chaque composant critique du système de crawling en gérant les erreurs d’initialisation, puis regroupe ces éléments dans une instance de `CrawlerDependencies`. Elle renvoie ainsi un conteneur prêt à être utilisé pour lancer les différents threads du crawler.

    Args:
        args: Arguments de ligne de commande utilisés pour paramétrer l’initialisation, notamment la suppression conditionnelle de la base ou du cache.
        cache: Instance optionnelle de cache déjà initialisée à réutiliser ; si elle est absente, un nouveau cache est créé.

    Returns:
        Une instance de `CrawlerDependencies` contenant le moteur de base de données, la fabrique de sessions, le cache, le bloom filter des URLs crawlées, son verrou et la file de priorité.
    """
    try:
        engine, session_factory = init_db(args)
    except Exception as e:
        raise InitializationError("Failed to initialize database") from e

    try:
        crawled_urls_bf, crawled_urls_bf_lock = init_bloom_filter(session_factory)
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

    return CrawlerDependencies(engine, session_factory, cache, crawled_urls_bf, crawled_urls_bf_lock, queue)

@overload
def launch_crawler(args: Namespace, stop_events: list[threading.Event], pause_events: list[threading.Event], cache: Cache | None, return_crawlers: Literal[False]) -> None: ...

@overload
def launch_crawler(args: Namespace, stop_events: list[threading.Event], pause_events: list[threading.Event], cache: Cache | None, return_crawlers: Literal[True]) -> tuple[list[dict[Crawler, tuple[threading.Event, threading.Event]]], dict[QueueRecharger, tuple[threading.Event, threading.Event]], BaseObserver, Bloom]: ...

def launch_crawler(args: Namespace, stop_events: list[threading.Event], pause_events: list[threading.Event], cache: Cache | None = None, return_crawlers: bool = False) -> Optional[tuple[list[dict[Crawler, tuple[threading.Event, threading.Event]]], dict[QueueRecharger, tuple[threading.Event, threading.Event]], BaseObserver, Bloom]]:
    """Lance les threads du crawler et orchestre leur cycle de vie complet. Initialise les dépendances, crée les crawlers, démarre la recharge de la file d’attente et configure la surveillance du fichier de configuration.

    Cette fonction instancie les objets nécessaires au crawling, démarre chaque thread de crawler ainsi que le `QueueRecharger` et l’observateur de configuration, puis attend leur terminaison ou renvoie ces objets selon le mode demandé. Elle permet ainsi de gérer centralement le démarrage et l’arrêt coordonné de l’ensemble du système de crawling.

    Args:
        args: Arguments de ligne de commande déterminant la configuration du crawler et des dépendances (base de données, cache, etc.).
        stop_events: Liste d’événements de synchronisation utilisés pour demander l’arrêt des différents threads de crawler et du recharger de file.
        pause_events: Liste d’événements permettant de mettre en pause ou de reprendre les threads de crawler et le recharger de file.
        cache: Instance optionnelle de cache à réutiliser ; si elle est absente, un nouveau cache est construit via les dépendances.
        return_crawlers: Indique si la fonction doit renvoyer les crawlers, le recharger de file, l’observateur et le bloom filter plutôt que d’attendre leur fin.

    Returns:
        Un tuple contenant la liste des crawlers avec leurs événements, le `QueueRecharger` avec ses événements, l’observateur de configuration et le bloom filter des URLs crawlées si `return_crawlers` est vrai ; `None` sinon, après la fin de tous les threads.
    """
    yappi.set_clock_type("wall")
    yappi.start()

    # création des threads
    crawlers = []
    config = CrawlerConfig.load_from_yml(crawler_config_file_path)
    domain_crawl_time = {}
    domain_crawl_time_lock = threading.Lock()
    crawling_urls = ThreadLocalURLTracker(config.num_crawlers, "")
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

    logger.success("All crawlers finished successfully!")
    return None
