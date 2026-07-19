import time
from functools import lru_cache
from typing import Optional
from urllib.parse import ParseResult, urlparse

import requests
from diskcache import Cache
from loguru import logger
from protego import Protego

from src.configs.crawler_config import CrawlerConfig
from .errors import NetworkError


class BaseDomainManager:
    """Fournit une base commune pour les gestionnaires de domaine utilisés par le crawler. Définit une interface minimale permettant de spécialiser le comportement réseau ou robots.txt sans imposer de logique concrète.

    Cette classe ne contient pas de fonctionnalités en elle-même mais sert de point d’extension pour des implémentations comme `NetworkManager` ou `RobotsTxtManager`. Elle permet de typer et organiser les gestionnaires liés aux domaines au sein du code de crawling.

    """
    pass


class NetworkManager(BaseDomainManager):
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
        self.session = requests.Session()
        self.config = config

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


class RobotsTxtManager(BaseDomainManager):
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