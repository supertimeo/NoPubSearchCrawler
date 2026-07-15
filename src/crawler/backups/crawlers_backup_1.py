from __future__ import annotations

#encoding: utf-8

# import standard
import time
import sys
import json
import os
from typing import Optional, Callable, Any
from contextlib import contextmanager
import gc
from itertools import count
from functools import lru_cache

import multiprocessing
import queue

# imports pour l'optimisation
from numba.typed import Dict, List  # type: ignore
from numba.types import Tuple  # type: ignore
from numba import njit  # type: ignore
import numba  # type: ignore

# import pour le scraping et le crawling
import requests # type: ignore
from selectolax.parser import HTMLParser # type: ignore
import urllib # type: ignore
from urllib.robotparser import RobotFileParser # type: ignore
from urllib.parse import urlparse, urljoin, urlunparse, ParseResult # type: ignore
from urllib.error import URLError # type: ignore
from urllib.request import urlopen # type: ignore
import urllib3 # type: ignore
import socket # type: ignore
from http.client import RemoteDisconnected # type: ignore

# import pour la gestion de la base de données
from sqlite3worker import Sqlite3Worker # type: ignore
import sqlite3 # type: ignore

# import pour la gestion des logs
from loguru import logger # type: ignore

# import pour la gestion des threads
import threading # type: ignore

# import pour la gestion des erreurs et des tracebacks
import traceback # type: ignore

# import pour la gestion du cache
from diskcache import Cache # type: ignore

# import pour les arguments de la ligne de commande
import argparse # type: ignore

# imports pour le bloom filter
from rbloom import Bloom # type: ignore

parser = argparse.ArgumentParser()
parser.add_argument("-l", "--delete-logs", action="store_true", help="Delete the logs")
parser.add_argument("-c", "--delete-cache", action="store_true", help="Delete the cache")
parser.add_argument("-d", "--delete-db", action="store_true", help="Delete the database")
parser.add_argument("-a", "--delete-all", action="store_true", help="Delete the logs, the database and the cache")
parser.add_argument("-v", "--vacuum", action="store_true", help="Vacuum the database")
args = parser.parse_args()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

WAITING_DELAY = 5
NUM_CRAWLERS = 4

if args.delete_logs:
    if os.path.exists("crawler.log"):
        os.remove("crawler.log")
    if os.path.exists("traceback.log"):
        os.remove("traceback.log")

if args.delete_db and os.path.exists("database.db"):
    os.remove("database.db")

if args.delete_all:
    if os.path.exists("crawler.log"):
        os.remove("crawler.log")
    if os.path.exists("traceback.log"):
        os.remove("traceback.log")
    if os.path.exists("database.db"):
        os.remove("database.db")

# création du logger
logger.remove()

# Création du compteur pour tie-breacker de la file d'attente des urls
tie_breaker_counter = count()
tie_breaker_counter_lock = threading.Lock()

crawler_logger = logger.bind(name="crawler")
traceback_logger = logger.bind(name="traceback")

logger.add("crawler.log", rotation="100 MB", enqueue=True, compression="zip", filter=lambda record: record["extra"]["name"] == "crawler", level="DEBUG", format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {extra[name]}:{function}:{line} - {message}")
logger.add("traceback.log", rotation="100 MB", enqueue=True, compression="zip", filter=lambda record: record["extra"]["name"] == "traceback", level="ERROR", format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {extra[name]}:{function}:{line} - {message}")
logger.add(sys.stdout, enqueue=True, filter=lambda record: record["extra"]["name"] == "crawler", level="DEBUG", format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {extra[name]}:{function}:{line} - {message}")
crawler_logger.info("Logger initialized")

# création du thread d'écriture dans la base de données
crawler_logger.info("Initializing database")
start_time = time.time()
db = Sqlite3Worker("database.db", max_queue_size=10_000) # type: ignore
sync_conn = sqlite3.connect("database.db")

if not os.path.exists("backups"):
    os.makedirs("backups")

if args.vacuum:
    crawler_logger.info("Vacuuming the database...")
    start_time = time.time()
    sync_cursor = sync_conn.cursor()
    sync_cursor.execute("VACUUM")
    sync_cursor.execute("PRAGMA auto_vacuum = INCREMENTAL")
    sync_cursor.execute("PRAGMA incremental_vacuum")
    sync_conn.commit()
    sync_conn.close()
    del sync_cursor
    crawler_logger.info(f"Database vacuumed in {time.time() - start_time} seconds")

sync_conn.execute("PRAGMA journal_mode=WAL;") # <--- INDISPENSABLE
sync_conn.execute("PRAGMA synchronous=NORMAL;") # Optionnel, mais boost les perfs d'écriture

sync_cursor.execute("CREATE TABLE IF NOT EXISTS waiting_list (domain_date INTEGER, date INTEGER, url TEXT UNIQUE, id INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)")
sync_cursor.execute("CREATE TABLE IF NOT EXISTS pages (url TEXT, title TEXT, content TEXT, links TEXT)")
sync_cursor.execute("CREATE TABLE IF NOT EXISTS crawled_urls (url TEXT UNIQUE)")
sync_cursor.execute("CREATE TEMP TABLE temp_waiting_list (url TEXT UNIQUE, domain_date INTEGER, date INTEGER)")

sync_cursor.execute("CREATE INDEX IF NOT EXISTS idx_waiting_list ON waiting_list (domain_date, date, url, id)")
sync_cursor.execute("CREATE INDEX IF NOT EXISTS idx_pages ON pages (url, title, content, links)")
sync_cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawled_urls ON crawled_urls (url)")
sync_cursor.execute("CREATE INDEX IF NOT EXISTS idx_temp_waiting_list ON temp_waiting_list (domain_date, date, url)")
sync_conn.commit()
sync_conn.close()
del sync_cursor
crawler_logger.info(f"Database initialized in {time.time() - start_time}")

crawler_logger.info("Initializing crawled url bloom-filter...")
start_time = time.time()
crawled_urls_bf = Bloom(100_000, 0.001)
crawled_urls_bf.update(set(db.execute("SELECT * FROM crawled_urls")))
crawler_logger.info(f"Crawled urls bloom-filter initialized in {time.time() - start_time}")

# création du cache
cache = Cache("robots_txts")

def increment_tie_breaker():
    """Increment and return the tie-breaker counter."""
    with tie_breaker_counter_lock:
        return next(tie_breaker_counter)

def _process_worker_wrapper(func, q, args, kwargs):
    """
    Fonction exécutée dans le processus enfant.
    Cette fonction doit être définie au niveau du module (top-level) pour fonctionner sur Windows.
    """
    try:
        result = func(*args, **kwargs)
        q.put(("OK", result))
    except Exception as e:
        q.put(("ERR", e))

@contextmanager
def timeout_manager(timeout: float):
    """
    Context manager qui fournit une fonction 'run' pour exécuter du code 
    dans un processus séparé avec un timeout strict (kill -9).
    
    Usage:
        with safe_process_timeout(5) as run:
            result = run(ma_fonction_bloquante, arg1, arg2)
    """
    
    def run_in_process(func: Callable, *args, **kwargs) -> Any:
        # Création de la queue de communication
        q: multiprocessing.Queue = multiprocessing.Queue()
        
        # Création du processus
        # On passe la fonction wrapper, pas la fonction cible directement
        p: multiprocessing.Process = multiprocessing.Process(
            target=_process_worker_wrapper, 
            args=(func, q, args, kwargs)
        )
        p.daemon = True  # Le processus meurt si le parent meurt
        p.start()

        try:
            # On attend le résultat avec le timeout défini
            status, payload = q.get(timeout=timeout)
            
            # Si on arrive ici, c'est que le process a fini à temps.
            # On attend qu'il se ferme proprement.
            p.join(timeout=0.1)
            
            if status == "ERR":
                raise payload  # On relève l'exception d'origine (ex: gaierror)
            return payload

        except queue.Empty:
            # TIMEOUT ATTEINT
            p.terminate()  # Arrêt brutal du processus
            p.join()       # Nettoyage des ressources
            raise TimeoutError(f"Process execution timed out after {timeout}s")
            
    # On 'yield' la fonction qui permet d'exécuter le code
    yield run_in_process

@njit()
def optiget_priority_queue(get_priority, queue): 
    return min((get_priority(netloc), date, tie_breaker, idx) for idx, (date, _, tie_breaker, netloc) in enumerate(queue))

class PriorityQueue:
    def __init__(self, get_priority: Callable[[str], float]):
        self._queue = List.empty_type(Tuple[numba.types.float64, numba.types.unicode_type])
        self._lock = threading.Lock()
        self._get_priority = get_priority
        
    def put(self, items: tuple|list[tuple]):
        with self._lock:
            if isinstance(items, tuple):
                items = (*items, increment_tie_breaker(), urlparse(items[1]).netloc)
                self._queue.append(items)
            else:
                items = [(*item, increment_tie_breaker(), urlparse(item[1]).netloc) for item in items]
                self._queue.extend(items)
    
    def get(self):
        with self._lock:
            _, _, _, min_index = optiget_priority_queue(self._get_priority, self._queue)
            return self._queue.pop(min_index)[:-2]
    
    def empty(self):
        with self._lock:
            return len(self._queue) == 0

class AutoBackupManager(threading.Thread):
    def __init__(self, db: Sqlite3Worker, stop_event: threading.Event, database_reorganize_event: threading.Event, database_ready_reorganize_event: threading.Event):
        threading.Thread.__init__(self)
        self.db: Sqlite3Worker = db
        self.stop_event = stop_event
        self.database_reorganize_event = database_reorganize_event
        self.database_ready_reorganize_event = database_ready_reorganize_event
        self.id = NUM_CRAWLERS + 2

    def run(self):
        while not self.stop_event.is_set():
            if self.database_reorganize_event.is_set():
                if not self.database_ready_reorganize_event[self.id].is_set():
                    self.database_ready_reorganize_event[self.id].set()
                time.sleep(10)
                continue
            time.sleep(60 * 30) # On attend 30 minutes avant de faire une sauvegarde
            try:             
                crawler_logger.info("Backing up the database...")
                start_time = time.time()
                num_backups = len(os.listdir("backups")) + 1
                backup_path = f"backups/database_backup_{num_backups}.db"
                self.db.execute("VACUUM INTO '?'", (backup_path,))
                new_db = Sqlite3Worker(backup_path, max_queue_size=10_000)
                new_db.execute("PRAGMA auto_vacuum = INCREMENTAL")
                new_db.close()
                del new_db
                crawler_logger.info(f"Database backed up in {time.time() - start_time}")
            except Exception as e:
                crawler_logger.error(f"Error while backing up the database: {e}")

class CrawlerReorganizer(threading.Thread):
    def __init__(self, db: Sqlite3Worker, stop_event: threading.Event, lock: threading.Lock, database_reorganize_event: threading.Event, database_ready_reorganize_event: list[threading.Event], domain_crawl_time, domain_crawl_time_lock: threading.Lock, sync_conn: sqlite3.Connection):
        threading.Thread.__init__(self)
        self.db = db
        self.sync_conn = sync_conn
        self.stop_event = stop_event
        self.lock = lock
        self.database_reorganize_event = database_reorganize_event
        self.database_ready_reorganize_event = database_ready_reorganize_event
        self.domain_crawl_time = domain_crawl_time
        self.domain_crawl_time_lock = domain_crawl_time_lock

    def run(self):
        while not self.stop_event.is_set():
            time.sleep(60 * 60 * 5) # On attend 5 heures avant de reorganiser la base de données
            try:
                self.database_reorganize_event.set()
                for event in self.database_ready_reorganize_event:
                    event.wait()
                
                with self.lock:
                    crawler_logger.info("Reorganizing the database...")
                    start_time = time.time()
                    sync_cursor = self.sync_conn.cursor()
                    sync_cursor.execute("DELETE FROM crawled_urls WHERE rowid NOT IN (SELECT MIN(rowid) FROM crawled_urls GROUP BY url)")
                    sync_cursor.execute("DELETE FROM waiting_list WHERE rowid NOT IN (SELECT MIN(rowid) FROM waiting_list GROUP BY url)")
                    sync_cursor.execute("DELETE FROM pages WHERE rowid NOT IN (SELECT MIN(rowid) FROM pages GROUP BY url)")
                    sync_cursor.execute("DELETE FROM temp_waiting_list WHERE rowid NOT IN (SELECT MIN(rowid) FROM temp_waiting_list GROUP BY url)")
                    
                    sync_cursor.execute("PRAGMA incremental_vacuum")
                    sync_cursor.execute("ANALYZE")
                    sync_cursor.execute("PRAGMA optimize")
                    sync_cursor.execute("PRAGMA reindex")
                    problems = sync_cursor.execute("PRAGMA integrity_check")
                    self.sync_conn.commit()
                    self.sync_conn.close()
                    del sync_cursor

                    crawler_logger.info(f"Database reorganized in {time.time() - start_time} seconds")
                    crawler_logger.info(f"Detected problems : {problems}")
                
                gc.collect()

                self.database_reorganize_event.clear()
                for event in self.database_ready_reorganize_event:
                    event.clear()

                    
            except Exception as e:
                crawler_logger.error(f"Error while reorganizing the database : {e}")
                traceback_logger.error(traceback.format_exc())

class QueueRecharger(threading.Thread):
    def __init__(self, queue: PriorityQueue, stop_event: threading.Event, database_reorganize_event: threading.Event, database_ready_reorganize_event: list[threading.Event]):
        threading.Thread.__init__(self)
        self.db = db
        self.queue = queue
        self.stop_event = stop_event
        self.database_reorganize_event = database_reorganize_event
        self.database_ready_reorganize_event = database_ready_reorganize_event
        self.id = NUM_CRAWLERS + 1

    def run(self):
        while not self.stop_event.is_set():
            if self.database_reorganize_event.is_set():
                if not self.database_ready_reorganize_event[self.id].is_set():
                    self.database_ready_reorganize_event[self.id].set()
                time.sleep(10)
                continue
            if self.queue.qsize() < 200:
                try:
                    crawler_logger.info("Recharging the queue...")
                    start_time = time.time()
                    self.recharge_queue()
                    crawler_logger.info(f"Queue recharged in {time.time() - start_time} seconds")
                except Exception as e:
                    crawler_logger.error(f"Error while recharging the queue : {e}")
                    traceback_logger.error(traceback.format_exc())

    def recharge_queue(self):  # sourcery skip: identity-comprehension
        urls = [(date, url, id) for url, date, id in self.db.execute("SELECT url, date, id FROM waiting_list ORDER BY domain_date ASC, date ASC LIMIT ?", (1000 - self.queue.qsize(),))]
        ids = [url[2] for url in urls]

        self.queue.put([(url[0], url[1]) for url in urls])
        self.db.execute(f"DELETE FROM waiting_list WHERE id IN ({','.join('?' for _ in ids)})", ids)

class Crawler(threading.Thread):
    def __init__(self, queue: PriorityQueue, stop_event: threading.Event, id: int, domain_crawl_time, domain_crawl_time_lock: threading.Lock, crawled_urls_bf, database_reorganize_event: threading.Event, database_ready_reorganize_event: threading.Event):
        threading.Thread.__init__(self)
        self.db = db
        self.queue = queue
        self.robot_parser = RobotFileParser()
        self.stop_event = stop_event
        self.domain_crawl_time = domain_crawl_time
        self.domain_crawl_time_lock = domain_crawl_time_lock
        self.crawler_name = f"Crawler Thread-{id+1}"
        self.crawled_urls_bf = crawled_urls_bf
        self.database_reorganize_event = database_reorganize_event
        self.database_ready_reorganize_event = database_ready_reorganize_event
        self.crawler_id = id

    def get_pure_url(self, url: str) -> str:
        """
        Obtient la version pure d'une URL, sans les paramètres de requête et le fragment.

        Args:
            url: L'URL à nettoyer.

        Returns:
            L'URL pure.
        """
        parsed_url = urlparse(url)
        return urlunparse(
            (parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', '')
        )

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
            with timeout_manager(5) as execute:
                execute(socket.gethostbyname, domain)
            return True
        except socket.gaierror:
            return False
        except TimeoutError:
            crawler_logger.error(f"Timeout while resolving {domain}")
            return False

    def get_robots_txt_urlpath(self, parsed_url: ParseResult) -> Optional[list[str]]:
        """
        Obtient le robots.txt d'un domaine.

        Args:
            parsed_url: L'URL à obtenir le robots.txt.

        Returns:
            Le robots.txt.
        """
        netloc = parsed_url.netloc
        robots_txt_url = f"{parsed_url.scheme}://{netloc}/robots.txt"
        if netloc not in cache:
            try:
                robots_txt = urllib.request.urlopen(robots_txt_url, timeout=10).read().decode("utf-8") # type: ignore
                cache[netloc] = robots_txt
                return robots_txt.splitlines()
            except urllib.error.URLError as e: # type: ignore
                crawler_logger.error(f"Error while reading robots.txt for {robots_txt_url}: {e}")
                traceback_logger.error(traceback.format_exc())
                return None
            except socket.timeout:
                crawler_logger.error(f"Timeout while reading robots.txt for {robots_txt_url}")
                traceback_logger.error(traceback.format_exc())
                return None
        return cache[netloc].splitlines() # type: ignore

    def url_is_crawled(self, url: str) -> bool:
        """
        Vérifie si une URL a déjà été crawlée.

        Args:
            url: L'URL à vérifier.

        Returns:
            True si l'URL a déjà été crawlée, False sinon.
        """
        if url in self.crawled_urls_bf:
            results = self.db.execute("SELECT url FROM crawled_urls WHERE url = ?", (url,))
        else:
            results = []
        return len(results) > 0

    def read_robots_txt(self, parsed_url: ParseResult, url: str) -> Optional[bool]:
        """
        Lit le robots.txt d'un domaine.

        Args:
            parsed_url: L'URL à lire.
        """
        with crawler_logger.contextualize(name=self.crawler_name), traceback_logger.contextualize(name=self.crawler_name):
            try:
                try:
                    # Récupérer le robots.txt
                    robots_txt = self.get_robots_txt_urlpath(parsed_url)
                except Exception:
                    raise
                
                # Lecture du robots.txt
                if robots_txt:
                    self.robot_parser.parse(robots_txt)
                    return True
                return False
            except TimeoutError as e:
                # Si le timeout est dépassé, on relève l'exception
                crawler_logger.error(f"Error while reading robots.txt for {url} because of timeout : {e}")
                raise
            except urllib.error.URLError as e: # type: ignore
                # Si une erreur de URL est déclenchée, on relève l'exception
                crawler_logger.error(f"Error while reading robots.txt for {url} because of URL error : {e}")
                raise
            except RemoteDisconnected as e:
                # Si une erreur de connexion est déclenchée, on relève l'exception
                crawler_logger.error(f"Error while reading robots.txt for {url} because of connection error : {e}")
                raise
            except Exception as e:
                # Si une erreur est déclenchée, on relève l'exception
                crawler_logger.error(f"Error while reading robots.txt for {url} : {e}")
                raise

    def extract_main_content(self, tree: HTMLParser) -> str:
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
            if main_element:
                break

        # Si aucun conteneur principal n'est trouvé, utiliser le body comme base
        if not main_element:
            main_element = tree.body
        if not main_element:
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
        return main_element.text(strip=True, separator=' ')

    def insert_urls_in_waiting_list(self, urls: set[str]):
        self.crawled_urls_bf.update(urls)

        url_domains_last_crawled_time = {}
        for url in urls:
            parsed = urlparse(url)
            domain = parsed.netloc
            last_crawled_time = self.domain_crawl_time.get(domain, 0)
            url_domains_last_crawled_time[url] = last_crawled_time
            
        for url, last_crawled_time in url_domains_last_crawled_time.items():
            self.db.execute("INSERT INTO temp_waiting_list (url, domain, date) VALUES (?, ?, ?)", (url, domain, int(time.time())))

        
        self.db.execute("""INSERT OR IGNORE INTO waiting_list (url, domain, date)
                            SELECT t.url, t.domain, t.date
                            FROM temp_waiting_list t
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM crawled_urls cu
                                WHERE cu.url = t.url
                            )""")
        self.db.execute("DELETE FROM temp_waiting_list")

    def crawl(self, url: str, session: requests.Session) -> tuple[Optional[str], Optional[str], Optional[set[str]], bool]:
        # sourcery skip: de-morgan
        """
        Crawle une URL et retourne le titre, le contenu et les liens.

        Args:
            url: L'URL à crawler.

        Returns:
            Un tuple contenant le titre, le contenu et les liens.
        """
        with crawler_logger.contextualize(name=self.crawler_name), traceback_logger.contextualize(name=self.crawler_name):
            # Préparser l'URL
            parsed_url = urlparse(url)

            # Précalculer le netloc et le robots.txt
            netloc = parsed_url.netloc

            # Vérification si l'URL est crawlable
            if not (parsed_url.scheme in ["http", "https"] or netloc is not None) or netloc == "":
                crawler_logger.warning(f"URL {url} is not crawlable because it doesn't start with http or https or it doesn't have a netloc")
                return None, None, None, False

            # Vérification si l'URL est résolvable
            if not self.is_resolvable(netloc):
                crawler_logger.warning(f"URL {url} is not crawlable because it's not resolvable")
                return None, None, None, False

            # Lecture du robots.txt
            crawler_logger.info(f"Reading robots.txt for {url}...")
            start_time = time.time()
            is_restricted = self.read_robots_txt(parsed_url, url)
            crawler_logger.info(f"Read robots.txt for {url} in {time.time() - start_time} seconds")

            # Vérification de la permission de crawler la page avec le robots.txt
            if not is_restricted or self.robot_parser.can_fetch("*", url):
                try:
                    # En-têtes pour simuler un navigateur
                    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
                    
                    try:
                        start_time = time.time()
                        # Requête GET pour obtenir le contenu de la page
                        response = requests.get(url, headers=headers, timeout=5, allow_redirects=False)
                        response.raise_for_status() # Si une erreur HTTP est déclenchée, on relève l'exception
                        crawler_logger.info(f"Get {url} in {time.time() - start_time} seconds")
                    except requests.exceptions.RequestException as e:
                        # Si une erreur de requête est déclenchée, on relève l'exception
                        crawler_logger.error(f"Error while getting {url} : {e}")
                        raise
                    
                    if not response.headers.get("Content-Type") or not response.headers.get("Content-Type").startswith("text/html"): # type: ignore
                        crawler_logger.warning(f"URL {url} is not text/html page")
                        return None, None, None, False
                    
                    # Analyse du contenu de la page selectolax beaucoup plus rapide que BeautifulSoup
                    tree = HTMLParser(response.text)
                    
                    # Récupération du titre et du contenu de la page
                    title = tree.css_first("title").text() if tree.css_first("title") else "Sans titre" # type: ignore
                    content = self.extract_main_content(tree)
                    
                    try:
                        # Récupération des liens de la page
                        links = {link.attributes.get("href") for link in tree.css("a") if link.attributes.get("href")}
                        links = {urljoin(url, link) for link in links if urlparse(link).scheme in ["http", "https", ""]}
                        links = {self.get_pure_url(link) for link in links if link}
                    except (AttributeError, KeyError) as e:
                        # Si une erreur d'attribut est déclenchée, on relève l'exception
                        crawler_logger.error(f"Error while getting links for {url}: {e}")
                        links = set()

                    # Retourne le titre, le contenu et les liens de la page
                    crawler_logger.info(f"Crawled page : {url}")
                    return title, content, links, True # type: ignore
                
                except requests.exceptions.Timeout as e:
                    # Si une erreur de timeout est déclenchée, on relève l'exception
                    crawler_logger.error(f"Error while crawling {url} because of timeout : {e}")
                    raise requests.exceptions.Timeout from e
                except requests.exceptions.SSLError as e:
                    # Si une erreur SSL est déclenchée, on relève l'exception
                    crawler_logger.error(f"Error while crawling {url} because of SSL error : {e}")
                    raise requests.exceptions.SSLError from e
                except requests.exceptions.TooManyRedirects as e:
                    # Si une erreur de trop de redirections est déclenchée, on relève l'exception
                    crawler_logger.error(f"Error while crawling {url} because of too many redirects : {e}")
                    raise requests.exceptions.TooManyRedirects from e
                except requests.exceptions.HTTPError as e:
                    # Si une erreur HTTP est déclenchée, on relève l'exception
                    crawler_logger.error(f"Error while crawling {url} because of HTTP error : {e}")
                    raise requests.exceptions.HTTPError from e
                except requests.exceptions.ConnectionError as e:
                    # Si une erreur de connexion est déclenchée, on relève l'exception
                    crawler_logger.error(f"Error while crawling {url} because of connection error : {e}")
                    raise requests.exceptions.ConnectionError from e
            else:
                # Si la page n'est pas crawlable, on relève l'exception
                crawler_logger.warning(f"Page {url} is not crawlable because of robots.txt")
                return None, None, None, False

    def run(self):
        """
        Lance le crawler.
        """
        with crawler_logger.contextualize(name=self.crawler_name), traceback_logger.contextualize(name=self.crawler_name):
            crawler_logger.info(f"Crawler {self.crawler_name} started")
            time.sleep(5) # Attente de 5 secondes pour permettre le démarrage des threads

            with requests.Session() as session:
                while not self.stop_event.is_set():
                    if self.database_reorganize_event.is_set():
                        if not self.database_ready_reorganize_event[self.crawler_id].is_set():
                            self.database_ready_reorganize_event[self.crawler_id].set()
                        time.sleep(10)
                        continue

                    # Récupération d'une page à crawler
                    scheduled_time, url = self.queue.get()
                    crawler_logger.info(f"Get {url} in the queue")
                    
                    # Préparsing de l'URL et extraction du domaine
                    parsed_url = urlparse(url)
                    domain = parsed_url.netloc

                    # Vérification si la page a déjà été crawlée
                    if self.url_is_crawled(url):
                        crawler_logger.info(f"{url} is already crawled")
                        continue

                    # Vérification du délai de crawling
                    crawler_logger.info("Checking a crawl delay for URL...")
                    start_time = time.time()
                    if (current_time := int(time.time())) < scheduled_time:
                        self.queue.put((scheduled_time, url))
                        crawler_logger.info(f"{url} is not ready to be crawled yet. Wait for {scheduled_time - current_time} ms")
                        time.sleep(0.5) # Petite pause pour éviter de surcharger le CPU
                        continue
                    crawler_logger.info(f"Sucess checking the crawl delay for URL in {time.time() - start_time} seconds.")

                    # Vérification du délai de crawling par domaine
                    crawler_logger.info(f"Checking a crawl delay by domain for url {url}...")
                    start_time = time.time()
                    with self.domain_crawl_time_lock:
                        # Lecture du robots.txt
                        is_restricted = self.read_robots_txt(parsed_url, url)

                        # On récupère le délai de crawling depuis le robots.txt, sinon on utilise le délai par défaut
                        delay_from_robots = self.robot_parser.crawl_delay("*")
                        crawl_delay = float(delay_from_robots) if is_restricted and delay_from_robots is not None else WAITING_DELAY

                        # On récupère la date du dernier crawl pour le domaine
                        last_crawled_time = self.domain_crawl_time.get(domain, 0)

                        next_crawl_time = last_crawled_time + crawl_delay
                        
                        if time.time() < next_crawl_time:
                            self.queue.put((next_crawl_time, url))
                            crawler_logger.info(f"{url} is not ready to be crawled yet. Wait for {next_crawl_time - time.time()} ms")
                            time.sleep(0.5) # Petite pause pour éviter de surcharger le CPU
                            continue

                        # Mise à jour de la date du dernier crawl pour le domaine
                        self.domain_crawl_time[domain] = time.time()
                    crawler_logger.info(f"Sucess checking the crawl delay by domain for url {url} in {time.time() - start_time} seconds.")
                    

                    # Nettoyer l'URL
                    url = self.get_pure_url(url)

                    # Crawling de la page
                    crawler_logger.info(f"Crawling page {url}...")
                    try:
                        start_time = time.time()
                        title, content, links, success = self.crawl(url, session)
                        links = links or set()
                        if success:
                            self.db.execute("INSERT INTO crawled_urls (url) VALUES (?)", (url,))
                    except Exception as e:
                        crawler_logger.error(f"the page {url} could not be crawled.")
                        traceback_logger.error(traceback.format_exc())
                        continue

                    # Vérification si le crawling a réussi
                    if not success:
                        continue

                    crawler_logger.info(f"Crawled page {url} in {time.time() - start_time} seconds")

                    # Vérification si le titre, le contenu et les liens sont valides
                    if not title and not content and not links:
                        crawler_logger.warning(f"Invalid page {url}")
                        continue

                    # Ajout des liens à la liste des pages à crawler
                    crawler_logger.info("Add urls to the waiting list...")
                    start_time = time.time()
                    try:
                        self.insert_urls_in_waiting_list(links)
                    except Exception as e:
                        crawler_logger.error(f"Error while adding urls to the waiting list : {e}")
                        traceback_logger.error(traceback.format_exc())
                        continue
                    crawler_logger.info(f"URLs added to the waiting list in {time.time() - start_time} seconds")

                    # Ajout de la page à la base de données
                    crawler_logger.info(f"Add {url} to the pages...")
                    self.db.execute("INSERT INTO pages (url, title, content, links) VALUES (?, ?, ?, ?)", (url, title, content, json.dumps(list(links))))
                    crawler_logger.info(f"Page added to the database : {url}")

def main():
    # création des threads
    crawlers = []
    lock = threading.Lock()
    stop_event = threading.Event()
    database_reorganize_event = threading.Event()
    database_ready_reorganize_event = [threading.Event() for _ in range(NUM_CRAWLERS + 2)]
    domain_crawl_time = Dict.empty(key_type=numba.types.unicode_type, value_type=numba.types.float64)
    domain_crawl_time_lock = threading.Lock()

    @njit
    def get_priority(domain: str) -> int:
        return domain_crawl_time.get(domain, 0)
    
    queue: PriorityQueue[tuple[float, str, int]] = PriorityQueue(get_priority) # type: ignore

    # chargement des urls de départ
    for date, url, id in db.execute("SELECT url, date, id FROM waiting_list ORDER BY date ASC LIMIT 1000"):
        queue.put((url, date, id))
        db.execute("DELETE FROM waiting_list WHERE id = ?", (id,))

    """with open("other_start_urls2.json", "r") as f:
        for id, url in enumerate(json.load(f)):
            queue.put((time.time(), url, id))"""
    
    for thread_id in range(NUM_CRAWLERS):
        crawler = Crawler(queue, stop_event, thread_id, domain_crawl_time, domain_crawl_time_lock, crawled_urls_bf, database_reorganize_event, database_ready_reorganize_event)
        crawler.start()
        crawlers.append(crawler)

    recharger = QueueRecharger(queue, lock, stop_event, database_reorganize_event, database_ready_reorganize_event)
    recharger.start()

    reorganizer = CrawlerReorganizer(db, database_reorganize_event, lock, stop_event, database_ready_reorganize_event, domain_crawl_time, domain_crawl_time_lock)
    reorganizer.start()

    autobackup = AutoBackupManager(db, stop_event, database_reorganize_event, database_ready_reorganize_event)
    autobackup.start()

    while True:
        try:
            time.sleep(1)
        except BaseException:
            stop_event.set()
            break

    # attente de la fin des threads
    for t in crawlers:
        t.join()
    
    if args.delete_cache or args.delete_all:
        cache.clear()
        cache.close()
    
    crawler_logger.info("All crawler finished")

if __name__ == "__main__":
    main()