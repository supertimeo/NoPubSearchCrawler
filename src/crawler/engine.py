from __future__ import annotations

import math

# import pour la gestion des threads
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from queue import PriorityQueue
from typing import Generator, cast

import sentry_sdk
# import pour le scraping et le crawling
import urllib3
from cachetools import TTLCache

# import pour la gestion du cache
from diskcache import Cache

# import pour la gestion des logs
from loguru import logger
from psycopg.errors import StringDataRightTruncation
from pydantic.v1.dataclasses import dataclass

# imports pour le bloom filter
from rbloom import Bloom
from sqlalchemy import delete, exists, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DataError
from sqlalchemy.orm import Session as ASession
from sqlalchemy.orm import scoped_session
from watchdog.events import DirMovedEvent, FileMovedEvent, FileSystemEventHandler

from src.common.errors import DatabaseError
from src.common.paths import crawler_config_file_path
from src.configs.crawler_config import CrawlerConfig
from src.database.model import URL, CrawledURL, Link, Page, WaitingURL

from .errors import CrawlError, NetworkError
from .log_levels import LoggingLevels
from .managers import HTMLParsingManager, NetworkManager, RobotsTxtManager, URLManager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@contextmanager
def profile_block(name: str):
    start = time.perf_counter()  # perf_counter est plus précis que time.time()
    yield
    elapsed = (time.perf_counter() - start) * 1000  # En millisecondes
    logger.trace(f"[PROFILER] {name} a pris {elapsed:.2f} ms")


class ThreadLocalURLTracker[T]:
    def __init__(self, size: int, value: T):
        self._data = [value] * size

    def __getitem__(self, i: int) -> T:
        return self._data[i]

    def __setitem__(self, i: int, value: T):
        self._data[i] = value

    def __len__(self) -> int:
        return len(self._data)


@dataclass
class CrawlResult:
    """Représente le résultat d’une opération de crawling sur une page web. Regroupe les informations principales extraites afin de pouvoir les stocker ou les traiter facilement.

    Cette classe contient le titre de la page, son contenu textuel principal, l’ensemble des liens sortants trouvés ainsi qu’un horodatage du moment où le crawl a été réalisé. Elle sert de structure de données de retour pour les méthodes de crawling.

    Attributes:
        title: Titre de la page crawlée.
        content: Contenu textuel principal extrait de la page.
        links: Ensemble des URLs découvertes sur la page.
        timestamp: Horodatage (en secondes depuis l’époque Unix) indiquant le moment du crawl.
    """

    title: str
    content: str
    links: set[str]
    timestamp: float


class ConfigFileEventHandler(FileSystemEventHandler):
    """Gère les événements de déplacement du fichier de configuration du crawler. Permet de recharger dynamiquement la configuration lorsque le fichier suivi est modifié ou remplacé.

    Cette classe observe les mouvements du fichier de configuration et applique le nouveau `CrawlerConfig` aux différents composants impliqués dans le crawling. Elle assure ainsi une mise à jour cohérente de la configuration des instances de `Crawler` et du `QueueRecharger` sans redémarrer l’application.

    Attributes:
        crawlers: Liste des instances de `Crawler` dont la configuration doit être mise à jour lorsque le fichier de configuration change.
        queue_recharger: Instance de `QueueRecharger` dont la configuration de recharge de la file doit être synchronisée avec le nouveau fichier de configuration.
    """

    def __init__(self, crawlers: list[Crawler], queue_recharger: QueueRecharger):
        """Initialise le gestionnaire d’événements avec les composants concernés par les changements de configuration. Lie les instances de `Crawler` et le `QueueRecharger` au gestionnaire afin qu’ils puissent être mis à jour lors des mouvements du fichier de configuration.

        Cette méthode conserve les références fournies pour pouvoir leur appliquer un nouveau `CrawlerConfig` quand un événement de déplacement pertinent est détecté. Elle ne modifie pas la configuration elle-même mais prépare le gestionnaire à propager les futures mises à jour.

        Args:
            crawlers: Liste des instances de `Crawler` à notifier et à reconfigurer lors d’un changement du fichier de configuration.
            queue_recharger: Instance de `QueueRecharger` dont la configuration doit être synchronisée avec celle du crawler.
        """
        self.crawlers = crawlers
        self.queue_recharger = queue_recharger

    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        """Réagit aux événements de déplacement impliquant le fichier de configuration du crawler. Déclenche un rechargement de la configuration lorsque le fichier surveillé est déplacé ou renommé.

        Cette méthode compare le chemin source de l’événement au chemin du fichier de configuration suivi, et, en cas de correspondance, recharge le `CrawlerConfig` à partir du disque. Elle applique ensuite cette nouvelle configuration au `QueueRecharger` et à l’ensemble des instances de `Crawler` associées afin de refléter immédiatement les changements.

        Args:
            event: Événement de déplacement de fichier ou de répertoire généré par watchdog, contenant notamment le chemin source du fichier déplacé.
        """
        if Path(cast(str, event.src_path)).resolve() == crawler_config_file_path:
            new_config = CrawlerConfig.load_from_yml(crawler_config_file_path)
            self.queue_recharger.set_config(new_config)
            for crawler in self.crawlers:
                crawler.set_config(new_config)


class QueueRecharger(threading.Thread):
    """Recharge périodiquement la file de priorité des URLs à crawler. Supervise la taille de la file et ajoute de nouvelles URLs en attente lorsque le niveau minimal configuré est atteint.

    Cette classe exécute un thread dédié qui interroge la base de données pour récupérer des URLs en attente et les réinsère dans la file, tout en respectant les signaux de pause et d’arrêt. Elle s’appuie sur la configuration du crawler pour déterminer les seuils de recharge et la quantité maximale d’URLs à ajouter.

    Attributes:
        queue: File de priorité contenant les URLs à crawler, utilisée comme cible de la recharge.
        session_factory: Fabrique de sessions SQLAlchemy permettant d’accéder à la liste des URLs en attente dans la base de données.
        stop_event: Événement de synchronisation indiquant au thread qu’il doit arrêter sa boucle principale.
        pause_event: Événement de synchronisation permettant de mettre temporairement en pause la logique de recharge.
        config: Configuration du crawler utilisée pour déterminer les paramètres de recharge (taille minimale de file, taille maximale à recharger, etc.).
        id: Identifiant logique du thread calculé à partir du nombre de crawlers afin de le distinguer des threads de crawling.
        name: Nom du thread, utilisé pour les logs et le diagnostic.
        paused: Indique si le thread est actuellement en état de pause.
        last_activity: Horodatage de la dernière activité significative du thread, utile pour la supervision.
        logger: Logger enrichi avec le nom de la classe, utilisé pour tracer les opérations et erreurs du thread.
    """

    def __init__(
        self,
        queue: PriorityQueue[tuple[float, int, int, str]],
        session_factory: scoped_session[ASession],
        stop_event: threading.Event,
        pause_event: threading.Event,
    ):
        """Initialise le thread de recharge de la file d’attente du crawler. Configure les ressources nécessaires pour surveiller la file de priorité et interagir avec la base de données selon la configuration courante.

        Cette méthode prépare une instance de `QueueRecharger` en enregistrant la file de priorité, la fabrique de sessions, les événements de contrôle, ainsi que la configuration du crawler utilisée pour déterminer les paramètres de recharge. Elle définit également l’identifiant et le nom du thread, initialise les indicateurs d’état et crée un logger enrichi pour tracer le fonctionnement du recharger.

        Args:
            queue: File de priorité des URLs à crawler à surveiller et à recharger lorsque sa taille tombe sous le seuil minimal configuré.
            session_factory: Fabrique de sessions SQLAlchemy permettant d’ouvrir des sessions vers la base de données pour lire et mettre à jour les URLs en attente.
            stop_event: Événement de synchronisation utilisé pour demander l’arrêt du thread de recharge et mettre fin à sa boucle principale.
            pause_event: Événement de synchronisation utilisé pour mettre temporairement en pause la logique de recharge sans arrêter complètement le thread.
        """
        super().__init__()
        self.session_factory = session_factory
        self.queue = queue
        self.stop_event = stop_event
        self.pause_event = pause_event
        self.config = CrawlerConfig.load_from_yml(crawler_config_file_path)
        self.id = self.config.num_crawlers + 1
        self.name = "QueueRecharger"

        self.paused = False
        self.last_activity = time.time()

        self.logger = logger.bind(class_name=self.__class__.__name__)

    def run(self):
        """Exécute la boucle principale du thread de crawling. Coordonne la récupération des URLs, le respect des délais de crawl, la collecte des pages et l’enregistrement des résultats en base.

        Cette méthode gère les signaux de pause et d’arrêt, applique les règles de robots.txt et de délais par domaine, puis orchestre le crawling des pages en évitant les doublons et les conflits entre threads. Elle met à jour les compteurs internes, le bloom filter et la liste d’attente des URLs à explorer jusqu’à la demande d’arrêt du thread.

        """
        def onerror(e: BaseException):
            sentry_sdk.capture_exception(e)

        with self.logger.catch(
            level=LoggingLevels.CRITICAL,
            message=f"A fatal, unexpected error occurred in the run loop of QueueRecharger {self.name} ({self.native_id}). The thread is stopping.",
            onerror=onerror
        ):
            self.logger.info("QueueRecharger started.")

            while not self.stop_event.is_set():
                if self.pause_event.is_set():
                    if not self.paused:
                        self.paused = True
                        self.logger.success(f"{self.name} is paused")
                    time.sleep(1)
                    continue

                if self.paused:
                    self.paused = False
                    self.logger.success(f"{self.name} is resumed")

                self.last_activity = time.time()
                time.sleep(1)
                if self.queue.qsize() < self.config.min_queue_size:
                    with self.logger.catch(
                        level=LoggingLevels.ERROR,
                        message=f"Failed to recharge the queue (current size: {self.queue.qsize()}, min: {self.config.min_queue_size}). Will retry on the next cycle.",
                    ):
                        self.logger.debug("Recharging the queue...")
                        start_time = time.time()
                        self.recharge_queue()
                        self.logger.debug(
                            f"Queue recharged in {time.time() - start_time} seconds"
                        )
            self.logger.success(f"{self.name} finished successfully.")

    def recharge_queue(self):
        """Recharge la file de priorité avec des URLs issues de la liste d’attente. Sélectionne les URLs en attente les plus anciennes, les ajoute à la file puis les retire de la liste d’attente en base.

        Cette méthode interroge la base de données pour récupérer un lot d’URLs en attente, limité par la capacité restante de la file de priorité, puis les insère avec leur horodatage de dernier crawl de domaine. Elle supprime ensuite, dans une seconde transaction protégée par le logger, les entrées correspondantes de la table `WaitingURL` afin d’éviter de recharger plusieurs fois les mêmes URLs.

        """
        with self.session_factory() as session:
            urls = session.execute(
                select(
                    URL.url,
                    WaitingURL.domain_crawled_at,
                    WaitingURL.id,
                    WaitingURL.priority,
                )
                .join(WaitingURL.url)
                .order_by(WaitingURL.domain_crawled_at.asc(), WaitingURL.priority)
                .limit(self.config.max_queue_size - self.queue.qsize())
            ).all()
        ids = [url[2] for url in urls]

        if not ids:
            return

        for url in urls:
            self.queue.put((url[1], url[3], 0, url[0]))
        with (
            self.session_factory() as session,
            self.logger.catch(
                level=LoggingLevels.ERROR,
                message=f"Failed to remove {len(ids)} recharged URL(s) from the waiting list. Rolling back transaction.",
                onerror=lambda _: session.rollback(),
            ),
        ):
            session.execute(delete(WaitingURL).where(WaitingURL.id.in_(ids)))
            session.commit()

    def set_config(self, config: CrawlerConfig):
        self.config = config


class Crawler(threading.Thread):
    """Implémente un thread chargé de crawler des pages web à partir d’une file de priorité. Coordonne le respect des règles de robots.txt, la gestion des délais par domaine et l’enregistrement des résultats en base de données.

    Cette classe consomme des URLs depuis une file partagée, vérifie leur crawlabilité, télécharge les pages et en extrait le contenu ainsi que les liens sortants avant de les persister. Elle assure également la synchronisation des ressources partagées (bloom filter, temps de crawl par domaine, liste d’URLs en cours de traitement) entre plusieurs threads de crawling.

    Attributes:
        session_factory: Fabrique de sessions SQLAlchemy utilisée pour ouvrir des transactions vers la base de données.
        queue: File de priorité contenant les URLs à crawler, partagée entre les différents threads.
        stop_event: Événement de synchronisation permettant de demander l’arrêt propre du thread.
        pause_event: Événement de synchronisation permettant de mettre temporairement en pause le crawling.
        crawling_urls: Structure de suivi des URLs actuellement en cours de crawling par chaque thread.
        crawling_urls_lock: Verrou protégeant l’accès concurrent à `crawling_urls`.
        domain_crawl_time: Dictionnaire indiquant, pour chaque domaine, le prochain instant autorisé de crawl en fonction des délais.
        domain_crawl_time_lock: Verrou protégeant l’accès concurrent à `domain_crawl_time`.
        name: Nom lisible du thread, utilisé notamment pour les logs.
        crawled_urls_bf: Bloom filter contenant les URLs déjà crawlées pour éviter les doublons.
        crawler_id: Identifiant numérique du thread au sein du pool de crawlers.
        remove_params: Liste de paramètres de requête à supprimer lors de la normalisation des URLs.
        cache: Cache persistant utilisé notamment pour stocker les robots.txt ou d’autres métadonnées réseau.
        config: Configuration courante du crawler définissant divers paramètres de comportement (délais, nombre maximum de pages, etc.).
        crawled_urls_bf_lock: Verrou protégeant l’accès concurrent au bloom filter des URLs crawlées.
        logger: Logger enrichi avec le nom de la classe, utilisé pour tracer le fonctionnement du crawler.
        network_manager: Gestionnaire des requêtes réseau chargé de télécharger les pages et de vérifier la résolubilité des domaines.
        robots_txt_manager: Gestionnaire des robots.txt responsable de déterminer si une URL est autorisée et de fournir les délais de crawl.
        paused: Indique si le thread est actuellement en état de pause.
        pages_crawled: Compteur du nombre de pages crawlées avec succès par ce thread.
        last_activity: Horodatage de la dernière activité significative du thread, utile pour la supervision.

    """

    def __init__(
        self,
        queue: PriorityQueue[tuple[float, int, int, str]],
        session_factory: scoped_session[ASession],
        cache: Cache,
        stop_event: threading.Event,
        pause_event: threading.Event,
        crawler_id: int,
        crawling_urls: ThreadLocalURLTracker[str],
        crawling_urls_lock: threading.Lock,
        domain_crawl_time: dict[str, float],
        domain_crawl_time_lock: threading.Lock,
        crawled_urls_bf: Bloom,
        remove_params: list[str],
        crawled_urls_bf_lock: threading.Lock,
    ):
        """Initialise un thread de crawler avec toutes ses dépendances partagées. Prépare les structures de synchronisation, la configuration, les gestionnaires réseau et robots.txt ainsi que les compteurs internes pour le cycle de vie du thread.

        Cette méthode enregistre la file de priorité, la fabrique de sessions, le cache, les verrous et les structures de suivi nécessaires pour permettre au crawler de fonctionner correctement en environnement concurrent. Elle configure également le bloom filter, les paramètres de nettoyage d’URLs, le logger et les gestionnaires externes afin que le thread soit prêt à consommer des URLs dès son démarrage.

        Args:
            queue: File de priorité partagée depuis laquelle le crawler récupère les URLs à traiter.
            session_factory: Fabrique de sessions SQLAlchemy permettant au crawler d’accéder à la base de données.
            cache: Cache persistant utilisé pour stocker des informations réseau, notamment les robots.txt.
            stop_event: Événement de synchronisation servant à signaler au thread qu’il doit s’arrêter.
            pause_event: Événement de synchronisation servant à mettre en pause ou à reprendre temporairement le crawling.
            crawler_id: Identifiant unique du crawler au sein du pool de threads, utilisé notamment pour le nommage et le suivi des URLs.
            crawling_urls: Structure de suivi des URLs actuellement en cours de crawling par chaque thread.
            crawling_urls_lock: Verrou protégeant l’accès concurrent à la structure `crawling_urls`.
            domain_crawl_time: Dictionnaire stockant, pour chaque domaine, le prochain instant autorisé de crawl en fonction des délais.
            domain_crawl_time_lock: Verrou protégeant l’accès concurrent au dictionnaire `domain_crawl_time`.
            crawled_urls_bf: Bloom filter contenant les URLs déjà crawlées afin d’éviter les doublons.
            remove_params: Liste de paramètres de query à supprimer lors de la normalisation des URLs.
            crawled_urls_bf_lock: Verrou protégeant l’accès concurrent au bloom filter des URLs crawlées.
        """
        super().__init__()
        self.session_factory = session_factory
        self.queue = queue
        self.stop_event = stop_event
        self.pause_event = pause_event
        self.crawling_urls = crawling_urls
        self.crawling_urls_lock = crawling_urls_lock
        self.domain_crawl_time = domain_crawl_time
        self.domain_crawl_time_lock = domain_crawl_time_lock
        self.name = f"Crawler-{crawler_id + 1}"
        self.crawled_urls_bf = crawled_urls_bf
        self.crawler_id = crawler_id
        self.remove_params = remove_params
        self.cache = cache

        self.domain_errors_count_cache = TTLCache(maxsize=1000, ttl=600)
        self.down_domains_cache = TTLCache(maxsize=math.inf, ttl=3600)

        self.config = CrawlerConfig.load_from_yml(crawler_config_file_path)

        self.crawled_urls_bf_lock = crawled_urls_bf_lock

        self.logger = logger.bind(class_name=self.__class__.__name__)

        self.network_manager = NetworkManager(self.config)
        self.robots_txt_manager = RobotsTxtManager(
            self.cache, self.network_manager, self.config
        )
        self.url_manager = URLManager(self.config, self.robots_txt_manager)
        self.html_parsing_manager = HTMLParsingManager(self.url_manager)

        self.paused = False
        self.pages_crawled = 0
        self.last_activity = time.time()

    def set_config(self, config: CrawlerConfig):
        self.config = config

    @contextmanager
    def db_transaction(self, autocommit: bool = False) -> Generator[ASession]:
        """Gère une transaction de base de données autour d’un bloc de code. Fournit une session SQLAlchemy et assure le commit ou le rollback en fonction du succès ou de l’échec des opérations.

        Cette méthode ouvre une session via la fabrique fournie, la transmet au bloc appelant, puis effectue un commit automatique si `autocommit` est activé et qu’aucune exception n’est levée. En cas d’erreur, elle annule la transaction en cours via un rollback et enveloppe l’exception d’origine dans une `DatabaseError` cohérente avec le reste du système.

        Args:
            autocommit: Indique si la transaction doit être automatiquement validée (commit) à la sortie du bloc lorsque aucune exception n’est survenue.

        Returns:
            Un générateur fournissant une instance de `ASession` à utiliser dans le bloc `with`.

        Raises:
            DatabaseError: Si une exception survient lors de l’exécution des opérations de base de données dans le bloc de transaction.
        """
        with self.session_factory() as session:
            try:
                yield session
                if autocommit:
                    session.commit()
            except DataError as e:
                session.rollback()

                if isinstance(e.orig, StringDataRightTruncation):
                    raise DatabaseError(
                        "A string exceeds the maximum allowed length."
                    ) from e

                raise DatabaseError("A database error occurred.") from e
            except Exception as e:
                session.rollback()
                raise DatabaseError(
                    "An error occurred while inserting a URL in database"
                ) from e

    def url_is_crawled(self, url: str) -> bool:
        """Indique si une URL a déjà été traitée par le crawler. Combine une vérification rapide via bloom filter et une vérification persistante en base de données pour fiabiliser le résultat.

        Cette méthode ouvre une transaction de lecture et interroge la table des URLs crawlées afin de confirmer la présence de l’URL après un premier filtrage dans `crawled_urls_bf`. Elle renvoie un booléen permettant au crawler de décider s’il doit ignorer l’URL ou la traiter.

        Args:
            url: L’URL à vérifier dans le bloom filter et la base de données.

        Returns:
            True si l’URL est déjà enregistrée comme crawlée, False sinon.
        """
        with self.db_transaction() as session:
            return url in self.crawled_urls_bf and cast(
                bool,
                session.scalar(
                    select(
                        exists(
                            select(CrawledURL)
                            .join(CrawledURL.url)
                            .where(URL.url == url)
                        )
                    )
                ),
            )

    def insert_urls_in_waiting_list(self, session: ASession, urls: list[URL]) -> None:
        """Ajoute une liste d’URLs dans la table des URLs en attente de crawl. Prépare pour chaque URL un enregistrement contenant son identifiant et l’instant approximatif de dernier crawl de domaine afin de respecter les délais.

        Cette méthode duplique le dictionnaire des temps de crawl par domaine sous verrou, calcule pour chaque URL un `domain_crawled_at` par défaut si nécessaire, puis insère les entrées correspondantes dans `WaitingURL` avec une clause `on_conflict_do_nothing` pour éviter les doublons. Elle ne retourne pas de valeur mais met à jour la base de données pour que ces URLs puissent être rechargées ultérieurement dans la file de priorité.

        Args:
            session: Session SQLAlchemy active dans laquelle les entrées `WaitingURL` doivent être insérées.
            urls: Liste d’objets `URL` à ajouter à la liste d’attente, chacun fournissant son identifiant et son domaine.
        """
        if not urls:
            return

        with self.domain_crawl_time_lock:
            domain_crawl_time = self.domain_crawl_time.copy()

        session.execute(
            insert(WaitingURL)
            .values(
                [
                    {
                        "url_id": url.id,
                        "domain_crawled_at": domain_crawl_time.get(
                            self.url_manager.urlparse_(url.url).netloc, time.time()
                        ),
                        "priority": 1,
                    }
                    for url in urls
                ]
            )
            .on_conflict_do_nothing(index_elements=["url_id"])
        )

    def crawl(self, url: str) -> CrawlResult:
        """Crawle une URL et extrait les informations principales de la page. Retourne le titre, le contenu textuel principal et l’ensemble des liens pertinents découverts.

        Cette méthode télécharge la page via le `NetworkManager`, vérifie qu’il s’agit bien d’une ressource HTML, puis construit un arbre `HTMLParser` pour en extraire le titre, le contenu et les liens sortants en les normalisant. Elle encapsule ces données dans un objet `CrawlResult` accompagné d’un horodatage, ou lève une `CrawlError` si la page n’est pas une page HTML textuelle.

        Args:
            url: L’URL à crawler et analyser.

        Returns:
            Un objet `CrawlResult` contenant le titre, le contenu principal nettoyé, les URLs des liens sortants normalisées et l’horodatage du crawl.

        Raises:
            NetworkError: Si une erreur réseau survient lors du téléchargement de la page.
            CrawlError: Si la ressource obtenue n’est pas une page HTML textuelle exploitable.
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

        if not response.headers.get("Content-Type") or not response.headers.get(
            "Content-Type"
        ).startswith("text/html"):  # type: ignore
            self.logger.debug(f"URL {url} is not text/html page")
            raise CrawlError(f"URL {url} is not text/html page")

        tree = self.html_parsing_manager.parse_html(response.text)

        title = self.html_parsing_manager.extract_title(tree)
        links = self.html_parsing_manager.extract_links(url, tree)
        content = self.html_parsing_manager.extract_main_content(tree)

        # Retourne le titre, le contenu et les liens de la page
        self.logger.info(f"Crawled page : {url}")
        return CrawlResult(
            title=title, content=content, links=links, timestamp=time.time()
        )

    @staticmethod
    def insert_urls_in_db(
        urls: set[str] | tuple[str, ...],
        session: ASession,
    ) -> dict[str, URL]:
        """Insère un ensemble d’URLs dans la base de données si elles n’existent pas encore. Retourne un dictionnaire associant chaque URL texte à son objet `URL` persistant.

        Cette méthode ignore silencieusement les appels avec un ensemble vide, puis trie et insère les URLs fournies dans la table correspondante en utilisant une clause `on_conflict_do_nothing` pour éviter les doublons. Elle relit ensuite les enregistrements persistés et construit un mapping pratique entre les chaînes d’URL et leurs objets ORM, permettant de réutiliser ces références dans les opérations de crawling.

        Args:
            urls: Ensemble ou tuple de chaînes d’URLs à insérer ou récupérer depuis la base.
            session: Session SQLAlchemy active utilisée pour exécuter les opérations d’insertion et de sélection.

        Returns:
            Un dictionnaire où chaque clé est une chaîne d’URL et chaque valeur l’objet `URL` associé en base ; un dictionnaire vide si aucun URL n’a été fourni.
        """
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

    def flush_batch(self, batch_data: dict[str, CrawlResult]):
        if not batch_data:
            return

        try:
            with self.db_transaction(autocommit=True) as session:
                all_links_set = {  # sourcery skip
                    link for cr in batch_data.values() for link in cr.links
                }
                all_urls = set(batch_data.keys()) | set(
                    filter(
                        lambda link: (
                            self.url_manager.urlparse_(link).netloc
                            not in self.down_domains_cache
                        ),
                        all_links_set,
                    )
                )

                with profile_block("Insert all urls in db"):
                    url_objs_dict = self.insert_urls_in_db(all_urls, session)

                # Objets CrawledURL
                crawled_urls_to_add = [
                    CrawledURL(url=url_objs_dict[u]) for u in batch_data.keys()
                ]
                session.add_all(crawled_urls_to_add)

                # Objets Page
                pages_to_add = []
                for page_url, cr in batch_data.items():
                    page_orm_obj = url_objs_dict[page_url]
                    page_specific_links = [
                        Link(url=url_objs_dict[link])
                        for link in cr.links
                        if link in url_objs_dict
                    ]
                    pages_to_add.append(
                        Page(
                            url=page_orm_obj,
                            title=cr.title,
                            content=cr.content,
                            links=page_specific_links,
                        )
                    )
                session.add_all(pages_to_add)

                # Liste d'attente
                out_links_orm = [
                    url_objs_dict[link]
                    for link in all_links_set
                    if link in url_objs_dict
                ]
                self.insert_urls_in_waiting_list(session, out_links_orm)

        except DatabaseError:
            self.logger.exception("Error while adding a batch to database")
            sentry_sdk.capture_exception()
        else:
            for processed_url in batch_data.keys():
                self.crawled_urls_bf.add(processed_url)
            self.pages_crawled += len(batch_data)
        finally:
            batch_data.clear()

    def run(self):  # sourcery skip: low-code-quality
        """Exécute la boucle principale du thread de crawler. Coordonne la récupération des URLs, leur filtrage, le respect des délais de crawl et l’enregistrement des résultats en base de données.

        Cette méthode gère les signaux de pause et d’arrêt, contrôle les accès concurrents aux structures partagées et orchestre le cycle complet de traitement d’une URL, depuis la file de priorité jusqu’à la persistance des pages et des liens. Elle met à jour les compteurs internes, les temps de crawl par domaine et le bloom filter jusqu’à ce qu’un arrêt soit demandé via `stop_event`.

        """
        def onerror(e: BaseException) -> None:
            sentry_sdk.capture_exception(e)

        with self.logger.catch(
            level=LoggingLevels.CRITICAL,
            message=f"A fatal, unexpected error occurred in the run loop of {self.name} ({self.native_id}). The thread is stopping.",
            onerror=onerror,
        ):
            self.logger.info(f"Crawler {self.name} started")
            time.sleep(
                5
            )  # Attente de 5 secondes pour permettre le démarrage des threads

            batch_data: dict[str, CrawlResult] = {}

            while not self.stop_event.is_set():
                # Récupération d'une page à crawler
                if self.pause_event.is_set():
                    if not self.paused:
                        self.paused = True
                        self.logger.success(f"{self.name} is paused")
                    time.sleep(1)
                    continue

                if self.paused:
                    self.paused = False
                    self.logger.success(f"{self.name} is resumed")

                if self.queue.empty():
                    time.sleep(0.5)
                    self.flush_batch(batch_data)
                    continue

                domain_crawled_at, priority, num_retry_per_url, url = self.queue.get()
                self.last_activity = time.time()
                self.logger.trace(f"Get {url} in the queue")

                if num_retry_per_url >= self.config.network.max_retry_per_url:
                    self.logger.debug(
                        f"Failed to crawl {url} after {num_retry_per_url} attempts (maximum: {self.config.network.max_retry_per_url}). Giving up."
                    )
                    continue

                parsed_url = self.url_manager.urlparse_(url)
                domain = parsed_url.netloc

                if domain in self.down_domains_cache:
                    logger.debug(f"{domain} is down. Giving up.")
                    continue

                # Nettoyer l'URL
                url = self.url_manager.get_pure_url(url)

                delay = self.robots_txt_manager.get_crawl_delay(url)
                if domain_crawled_at + delay > time.time():
                    self.queue.put(
                        (domain_crawled_at, priority, num_retry_per_url, url)
                    )
                    self.logger.trace(f"{domain} is not ready for crawling")
                    time.sleep(0.5)
                    continue

                if not self.url_manager.url_is_crawlable(url):
                    time.sleep(0.5)
                    continue

                with self.crawled_urls_bf_lock:
                    # Vérification si la page a déjà été crawlée
                    if self.url_is_crawled(url):
                        self.logger.debug(f"{url} is already crawled")
                        continue

                with self.crawling_urls_lock:
                    if url in self.crawling_urls:
                        self.logger.trace(
                            f"{url} The URL is already being crawled by another crawler."
                        )
                        time.sleep(0.5)
                        continue
                    self.crawling_urls[self.crawler_id] = url

                # Crawling de la page
                self.logger.debug(f"Crawling page {url}...")

                start_time = time.time()
                try:
                    crawl_result = self.crawl(url)
                except CrawlError as e:
                    if not isinstance(e, NetworkError):
                        continue

                    if e.retryable:
                        if domain not in self.domain_errors_count_cache:
                            self.domain_errors_count_cache[domain] = 0
                        self.domain_errors_count_cache[domain] += 1

                        if (
                            self.domain_errors_count_cache[domain]
                            >= self.config.network.max_retry_per_domain
                        ):
                            if not self.network_manager.tcp_ping(domain):
                                self.down_domains_cache[domain] = 1
                                continue
                            del self.domain_errors_count_cache[domain]

                        with self.domain_crawl_time_lock:
                            self.queue.put((time.time(), 2, num_retry_per_url + 1, url))
                        continue

                    try:
                        with self.db_transaction(autocommit=True) as session:
                            session.add(
                                CrawledURL(
                                    url=list(
                                        self.insert_urls_in_db({url}, session).values()
                                    )[0]
                                )
                            )
                    except DatabaseError:
                        self.logger.exception(
                            f"Error while adding {url} to crawled urls"
                        )
                    else:
                        self.crawled_urls_bf.add(url)
                    continue

                self.logger.debug(
                    f"Crawled page {url} in {time.time() - start_time} seconds"
                )

                with self.domain_crawl_time_lock:
                    self.domain_crawl_time[domain] = time.time() + delay

                # Vérification si le titre, le contenu et les liens sont valides
                if (
                    not crawl_result.title
                    and not crawl_result.content
                    and not crawl_result.links
                ):
                    self.logger.warning(f"Invalid page {url}")
                    continue

                batch_data[url] = crawl_result

                # On attend d'avoir 50 éléments pour insérer
                if len(batch_data) < 50 and not self.queue.empty():
                    continue

                self.flush_batch(batch_data)

            self.logger.success(f"{self.name} finished successfully")