import re
import socket
import time
from functools import lru_cache
from typing import Optional
from urllib.parse import (
    ParseResult,
    parse_qsl,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)

import requests
import urllib3
from diskcache import Cache
from loguru import logger
from protego import Protego
from requests.adapters import HTTPAdapter
from selectolax.parser import HTMLParser
from urllib3 import Retry
from usp.tree import sitemap_from_str

from src.configs.crawler_config import CrawlerConfig
from .errors import CrawlError, NetworkError


class BaseManager:
    """Fournit une base commune pour les gestionnaires de domaine utilisés par le crawler. Définit une interface minimale permettant de spécialiser le comportement réseau ou robots.txt sans imposer de logique concrète.

    Cette classe ne contient pas de fonctionnalités en elle-même mais sert de point d’extension pour des implémentations comme `NetworkManager` ou `RobotsTxtManager`. Elle permet de typer et organiser les gestionnaires liés aux domaines au sein du code de crawling.

    """
    pass


class NetworkManager(BaseManager):
    """Gère les opérations réseau nécessaires au crawling des pages web. Centralise les requêtes HTTP et la résolution DNS tout en appliquant les paramètres configurés du crawler.

    Cette classe fournit une méthode de téléchargement de page qui interprète finement les erreurs réseau et HTTP en les transformant en `NetworkError` indiquant si une nouvelle tentative est pertinente. Elle expose également une méthode de vérification de résolubilité de domaine, mise en cache, afin de limiter les appels DNS coûteux.

    Attributes:
        session: Session HTTP `requests.Session` réutilisée pour les différentes requêtes afin d’optimiser les connexions.
        config: Configuration du crawler contenant notamment les paramètres réseau (user agent, délais, redirections, etc.).
    """
    def __init__(self, config: CrawlerConfig):
        """Initialise le gestionnaire réseau avec une session HTTP et la configuration du crawler. Prépare les paramètres nécessaires pour effectuer des requêtes cohérentes avec les règles définies (user agent, délais, redirections).

        Cette méthode crée une nouvelle `requests.Session` réutilisable pour toutes les requêtes et enregistre la configuration fournie afin de pouvoir appliquer ses options lors des opérations de téléchargement et de résolution de domaine. Elle ne lance aucune requête mais prépare l’instance à être utilisée immédiatement par le crawler.

        Args:
            config: Configuration du crawler contenant les paramètres réseau à appliquer aux requêtes HTTP et aux résolutions de domaine.
        """
        self.config = config

        retry_strategy = Retry(
            total=self.config.network.max_retry_per_request,  # nombre maximum de tentatives
            backoff_factor=self.config.network.retry_backoff_factor,  # attente entre les retries (1s, 2s, 4s...)
            status_forcelist=[  # codes HTTP qui déclenchent un retry
                429,  # Too Many Requests
                500,  # Internal Server Error
                502,
                503,
                504,
            ],
            allowed_methods={  # méthodes HTTP concernées
                "GET",
                "HEAD",
            },
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    @staticmethod
    def tcp_ping(domain, port=443, timeout=5):
        start = time.perf_counter()

        try:
            with socket.create_connection((domain, port), timeout):
                return (time.perf_counter() - start) * 1000
        except OSError:
            return None

    def fetch_page(self, url):
        """Télécharge la page située à l’URL donnée et renvoie la réponse HTTP associée. Interprète les erreurs réseau et HTTP pour les transformer en `NetworkError` indiquant si une nouvelle tentative est envisageable.

        Cette méthode effectue une requête GET à l’aide de la session configurée, applique les en-têtes et délais définis dans la configuration puis appelle `raise_for_status()` pour déclencher une exception en cas de statut HTTP d’erreur. Elle inspecte ensuite les différents types d’exceptions `requests` (timeout, SSL, connexion, erreurs HTTP 4xx/5xx) et lève des `NetworkError` détaillées, certaines marquées comme `retryable` pour permettre au crawler de replanifier la requête.

        Args:
            url: L’URL de la page à récupérer via une requête HTTP GET.

        Returns:
            L’objet `requests.Response` représentant la réponse HTTP reçue si la requête réussit.

        Raises:
            NetworkError: En cas de timeout, d’erreur SSL, de problème de connexion ou de code HTTP d’erreur, avec un indicateur `retryable` pour les erreurs temporaires.
        """
        try:
            response = self.session.get(url, headers={"User-Agent": self.config.network.user_agent}, timeout=self.config.network.timeout, allow_redirects=self.config.network.allow_redirects)
            response.raise_for_status()

        except requests.Timeout as e:
            raise NetworkError(f"Timeout while fetching {url}", retryable=True) from e

        except urllib3.exceptions.MaxRetryError as e:
            raise NetworkError(f"Max retry while fetching {url}", retryable=True) from e
        
        except requests.exceptions.RetryError as e:
            raise NetworkError(f"Retry failed while fetching {url}", retryable=True) from e

        except requests.exceptions.SSLError as e:
            raise NetworkError(f"SSL error while fetching {url}") from e

        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error while fetching {url}") from e

        except requests.exceptions.HTTPError as e:
            if not e.response:
                raise NetworkError(f"HTTP error while fetching {url}") from e

            status_code = e.response.status_code

            match status_code:
                case 400:
                    raise NetworkError(f"Bad request (400) for {url}") from e

                case 401:
                    raise NetworkError(f"Unauthorized (401) for {url}") from e

                case 403:
                    raise NetworkError(f"Forbidden (403) for {url}") from e

                case 404:
                    raise NetworkError(f"Not found (404) for {url}") from e

                case 410:
                    raise NetworkError(f"Gone (410) for {url}") from e

                case 408 | 429:
                    raise NetworkError(f"Temporary client error ({status_code}) while fetching {url}", retryable=True) from e

                case 500 | 502 | 503 | 504:
                    raise NetworkError(f"Server error ({status_code}) while fetching {url}", retryable=True) from e

                case _:
                    raise NetworkError(
                        f"HTTP error {status_code} while fetching {url}"
                    ) from e

        return response


class RobotsTxtManager(BaseManager):
    """Gère la récupération et l’interprétation des fichiers robots.txt pour le crawler. Centralise le téléchargement, la mise en cache et l’analyse des règles d’accès et des délais de crawl par domaine.

    Cette classe utilise un cache persistant pour éviter de re-télécharger les robots.txt, crée et mémorise des parseurs Protego par domaine, puis expose des méthodes pour obtenir le délai de crawl et vérifier si une URL est autorisée. Elle assure ainsi le respect des politiques définies par les sites web tout en optimisant les requêtes réseau.

    Attributes:
        cache: Cache persistant dans lequel les contenus des robots.txt sont stockés par domaine.
        parser_dict: Dictionnaire associant chaque netloc de domaine à son parseur Protego initialisé.
        config: Configuration du crawler contenant les paramètres réseau et le nom du robot à utiliser dans les robots.txt.
        logger: Logger enrichi avec le nom de la classe, utilisé pour tracer les opérations liées aux robots.txt.
        network_manager: Gestionnaire réseau utilisé pour télécharger les fichiers robots.txt.
    """
    def __init__(self, cache: Cache, network_manager: NetworkManager, config: CrawlerConfig):
        """Initialise le gestionnaire de robots.txt avec un cache, un gestionnaire réseau et la configuration du crawler. Prépare les structures internes nécessaires pour télécharger, mémoriser et analyser les fichiers robots.txt par domaine.

        Cette méthode enregistre le cache persistant, crée le dictionnaire des parseurs Protego, conserve la configuration et instancie un logger dédié pour tracer les opérations liées aux robots.txt. Elle stocke également le `NetworkManager` fourni afin de pouvoir récupérer les robots.txt via HTTP lors des futures requêtes.

        Args:
            cache: Instance de `Cache` utilisée pour stocker le contenu des fichiers robots.txt indexés par domaine.
            network_manager: Gestionnaire réseau chargé de télécharger les robots.txt depuis les domaines ciblés.
            config: Configuration du crawler définissant notamment le nom du robot et les paramètres de délais utilisés lors de l’interprétation des robots.txt.
        """
        self.cache = cache
        self.parser_dict: dict[str, Protego] = {}

        self.config = config

        self.logger = logger.bind(class_name=self.__class__.__name__)

        self.network_manager = network_manager

    def get_robots_txt(self, url: str | ParseResult) -> Optional[str]:
        """Récupère le contenu du fichier robots.txt associé à une URL ou à son objet parsé. Utilise un cache pour éviter les téléchargements répétés et renvoie `None` si le fichier n’est pas disponible ou ne doit plus être tenté.

        Cette méthode construit l’URL du robots.txt à partir du schéma et du netloc, vérifie la présence d’une entrée en cache puis, si besoin, télécharge le fichier via le `NetworkManager`. En cas de succès, elle stocke le contenu dans le cache et le renvoie ; en cas d’erreur non réessayable, elle mémorise une valeur `None` pour le domaine afin d’éviter de re-tenter la récupération lors des prochains appels.

        Args:
            url: L’URL cible sous forme de chaîne ou d’objet `ParseResult` dont on souhaite obtenir le robots.txt associé.

        Returns:
            Le contenu textuel du robots.txt pour le domaine de l’URL, ou `None` si le fichier est inaccessible ou marqué comme non récupérable.
        """
        parsed_url = urlparse(url) if isinstance(url, str) else url

        self.logger.trace(f"Getting robots.txt for {parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path} ...")
        start_time = time.time()

        netloc = parsed_url.netloc
        robots_txt_url = f"{parsed_url.scheme}://{netloc}/robots.txt"

        if netloc in self.cache:
            return self.cache[netloc]

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

    @lru_cache(maxsize=10_000)
    def get_parser(self, url: str | ParseResult) -> Optional[Protego]:
        """Retourne le parseur Protego associé au robots.txt du domaine de l’URL. Crée et mémorise un nouveau parseur si aucun n’existe encore pour ce domaine, ou renvoie `None` si aucun robots.txt n’est disponible.

        Cette méthode commence par normaliser l’URL, récupère son robots.txt via `get_robots_txt` puis, en l’absence de contenu, indique qu’aucune restriction explicite ne peut être analysée en renvoyant `None`. Lorsque le fichier est présent, elle instancie un parseur Protego pour le netloc concerné s’il n’a pas encore été créé, le stocke dans `parser_dict` pour les appels futurs, puis renvoie ce parseur pour interroger les règles de crawl.

        Args:
            url: L’URL cible sous forme de chaîne ou d’objet `ParseResult` pour laquelle on souhaite obtenir un parseur de robots.txt.

        Returns:
            Une instance de `Protego` permettant d’interroger les règles du robots.txt du domaine, ou `None` si aucun fichier robots.txt utilisable n’a été trouvé.
        """
        parsed_url = urlparse(url) if isinstance(url, str) else url

        robots_txt = self.get_robots_txt(parsed_url)
        return None if robots_txt is None else Protego.parse(robots_txt)

    @lru_cache(maxsize=10_000)
    def get_crawl_delay(self, url: str | ParseResult) -> float:
        """Calcule le délai de crawl à respecter pour une URL donnée en fonction de son robots.txt. Retourne un temps d’attente borné entre un délai par défaut et un délai maximal configuré.

        Cette méthode récupère le parseur Protego associé au domaine de l’URL et, en l’absence de robots.txt utilisable, renvoie immédiatement le délai d’attente par défaut défini dans la configuration réseau. Lorsque des règles sont disponibles, elle interroge le crawl-delay pour le nom de bot configuré, applique un délai de repli si aucune valeur n’est fournie, puis borne le résultat au maximum autorisé avant de le renvoyer.

        Args:
            url: L’URL cible sous forme de chaîne ou d’objet `ParseResult` pour laquelle on souhaite déterminer le temps d’attente avant un nouveau crawl.

        Returns:
            Un flottant représentant le délai de crawl en secondes à respecter avant de crawler de nouveau le domaine de l’URL.
        """
        parser = self.get_parser(url)
        if parser is None:
            return self.config.network.default_waiting_delay
        return min(parser.crawl_delay(self.config.network.bot_name) or self.config.network.default_waiting_delay, self.config.network.max_waiting_delay)

    def is_allowed(self, url: str) -> bool:
        """Indique si le crawler est autorisé à récupérer une URL au regard de son robots.txt. Retourne un booléen basé sur les règles déclarées pour le bot configuré.

        Cette méthode obtient le parseur Protego associé au domaine de l’URL et considère l’accès comme autorisé lorsque aucun robots.txt utilisable n’est disponible. Si un parseur existe, elle délègue la décision à `can_fetch` en lui transmettant l’URL et le nom du bot, et renvoie le résultat pour guider le processus de crawling.

        Args:
            url: L’URL à vérifier contre les règles du robots.txt du domaine.

        Returns:
            True si l’URL est autorisée à être crawlée par le bot configuré, False si le robots.txt l’interdit.
        """
        parser = self.get_parser(url)
        if parser is None:
            return True
        return parser.can_fetch(url, self.config.network.bot_name)

    def get_sitemaps(self, url: str) -> list[str]:
        parser = self.get_parser(url)
        return [] if parser is None else list(parser.sitemaps)
    
    
class URLManager(BaseManager):
    def __init__(self, config: CrawlerConfig, robots_txt_manager: RobotsTxtManager):
        self.config = config
        
        self.robots_txt_manager = robots_txt_manager

    @staticmethod
    def urlparse_(url):
        return urlparse(url)

    def get_pure_url(self, url: str) -> str:
        """Normalise une URL afin d’obtenir une version «pure» et stable pour le crawling. Nettoie le schéma, le domaine, le chemin et les paramètres de requête pour réduire les doublons et les variations non pertinentes.

        Cette méthode met en minuscules le schéma et le domaine, supprime les ports par défaut, simplifie et nettoie le chemin, puis filtre et trie les paramètres de query en excluant ceux listés dans `remove_params`. Elle reconstruit finalement une nouvelle URL sans fragment à partir de ces éléments normalisés.

        Args:
            url: L’URL brute à normaliser avant son utilisation dans le processus de crawling.

        Returns:
            Une chaîne représentant l’URL normalisée, avec un schéma et un domaine nettoyés, un chemin simplifié et une query string filtrée et ordonnée.
        """
        # 1. Analyser l'URL
        parsed = self.urlparse_(url)

        # 2. Normalisation du schéma et domaine (netloc)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Suppression des ports par défaut inutiles (ex: example.com:80 -> example.com)
        if scheme == "http" and netloc.endswith(":80"):
            netloc = netloc[:-3]
        elif scheme == "https" and netloc.endswith(":443"):
            netloc = netloc[:-4]


        # Remplacement des slashes multiples par un seul (ex: /wiki//test -> /wiki/test)
        path = re.sub(r'//+', '/', parsed.path)

        # Retrait du slash final si ce n'est pas la racine
        if len(path) > 1 and path.endswith('/'):
            path = path[:-1]

        # 3. Traitement des paramètres (Query String)
        query_params = parse_qsl(parsed.query, keep_blank_values=True)

        # Filtrer et trier
        filtered_params = [
            (key, value) for key, value in query_params
            if key.lower() not in self.config.url.remove_params
        ]
        filtered_params.sort()

        # Reconstruction de la query string
        new_query = urlencode(filtered_params)

        # 4. Reconstruction de l'URL propre sans le fragment ('')
        return urlunparse((scheme, netloc, path, parsed.params, new_query, ''))

    def url_is_crawlable(self, url: str) -> bool:
        """Détermine si une URL peut être crawlée par le crawler. Vérifie successivement la validité de son schéma et de son domaine, sa résolubilité réseau et les permissions définies dans le robots.txt.

        Cette méthode pré-analyse l’URL, s’assure qu’elle utilise un schéma supporté et qu’elle possède un netloc, puis interroge le `NetworkManager` pour confirmer que le domaine est résolvable. Elle consulte enfin le `RobotsTxtManager` pour savoir si le crawling est autorisé sur cette URL, et renvoie un booléen indiquant si l’URL doit être acceptée ou rejetée.

        Args:
            url: L’URL à évaluer pour déterminer si elle est admissible au processus de crawling.

        Returns:
            True si l’URL respecte les contraintes de schéma, de résolubilité et de robots.txt, False sinon.

        Raises:
            NotCrawlableError: Si l'url n'est pas crawlable, on raise une erreur pour laisser le choix a son utilisateur la façon dont gérer cela.
        """
        # Préparser l'URL
        parsed_url = self.urlparse_(url)

        # Précalculer le netloc et le robots.txt
        netloc = parsed_url.netloc

        # Vérification si l'URL est crawlable
        if (
            parsed_url.scheme not in ["http", "https"]
            and netloc is None
            or netloc == ""
        ):
            return False

        # Vérification de la permission de crawler la page avec le robots.txt
        return self.robots_txt_manager.is_allowed(url)

    
class HTMLParsingManager(BaseManager):
    def __init__(self, url_manager: URLManager):
        self.url_manager = url_manager
        self.robots_txt_manager = self.url_manager.robots_txt_manager

    @staticmethod
    def parse_html(html: str) -> HTMLParser:
        return HTMLParser(html)
    
    @staticmethod
    def extract_title(tree: HTMLParser) -> str:
        title = tree.css_first("title").text() if tree.css_first("title") else "Sans titre" # type: ignore
        return title.replace('\x00', '')[:512]

    def extract_links(self, base_url: str, tree: HTMLParser) -> set[str]:
        # TODO: Ajouter un parser pour le sitemap.xml
        try:
            return {
                pure_url
                for link in tree.css("a")
                if (href := link.attributes.get("href")) is not None
                if (full_url := urljoin(base_url, href))
                if urlparse(full_url).scheme in ("http", "https")
                if (pure_url := self.url_manager.get_pure_url(full_url))
            } | {
                link.url
                for sitemap in self.robots_txt_manager.get_sitemaps(base_url)
                for link in sitemap_from_str(sitemap).all_pages()
            }
        except ValueError as e:
            raise CrawlError(f"Invalid url: {base_url}") from e
    
    @staticmethod
    def extract_main_content(tree: HTMLParser) -> str:
        """Extrait le contenu textuel principal d’une page HTML parsée. Identifie les zones de contenu importantes et élimine les éléments de navigation ou décoratifs pour ne conserver que le texte utile.

        Cette méthode recherche en priorité des conteneurs tels que `<main>` ou `<article>`, ou se rabat sur le `<body>` si aucun n’est trouvé, puis supprime les balises non pertinentes (navigation, pied de page, scripts, styles, etc.). Elle renvoie ensuite le texte nettoyé et concaténé du conteneur choisi afin de fournir un contenu lisible et exploitable pour l’indexation.

        Args:
            tree: Objet `HTMLParser` de selectolax représentant l’arbre HTML de la page à analyser.

        Returns:
            Une chaîne de caractères contenant le contenu principal nettoyé de la page, sans éléments de navigation ni scripts.
        """
        # Sélecteurs CSS pour les conteneurs de contenu potentiels, par ordre de priorité
        main_content_selectors = [
            "main",
            "article",
            ".main-content",
            ".post",
            "#content",
            "#main",
        ]

        main_element = None
        for selector in main_content_selectors:
            main_element = tree.css_first(selector)
            if main_element is not None:
                break

        # Si aucun conteneur principal n'est trouvé, utiliser le body comme base
        if main_element is None:
            main_element = tree.body
        if main_element is None:
            return ""  # Retourner une chaîne vide si même le body est absent

        # Cloner l'élément pour ne pas modifier l'arbre original si ce n'est pas souhaité
        # C'est une bonne pratique, bien que selectolax ne fournisse pas de méthode de clonage directe.
        # Les opérations de suppression modifieront le `main_element`.

        # Sélecteurs des éléments à supprimer
        tags_to_remove = [
            "nav",
            "footer",
            "header",
            "aside",
            "script",
            "style",
            ".noprint",
        ]

        for tag_selector in tags_to_remove:
            # Trouver tous les éléments correspondants dans le conteneur principal
            elements_to_remove = main_element.css(tag_selector)
            for element in elements_to_remove:
                element.decompose()  # Supprime l'élément de l'arbre [1]

        # Extraire le texte de l'élément nettoyé
        # strip=True aide à enlever les espaces superflus en début et fin de chaque morceau de texte
        # separator=' ' ajoute un espace entre les blocs de texte pour une meilleure lisibilité
        return main_element.text(strip=True, separator=" ").replace("\x00", "")