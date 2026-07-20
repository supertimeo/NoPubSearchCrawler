import argparse
import asyncio
import os
import re
import threading
import time
import types
from collections import deque
from typing import (
    override,
    TYPE_CHECKING,
    Generator,
    Any,
    get_args,
    cast,
    get_origin,
    Union,
    Callable,
)

from diskcache import Cache
from dotenv import find_dotenv, dotenv_values, set_key
from pydantic import BaseModel
from rbloom import Bloom
from rich.text import Text
from sqlalchemy import func, select
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, VerticalScroll
from textual.widgets import (
    Tabs,
    Label,
    ContentSwitcher,
    Tab,
    Log,
    OptionList,
    TextArea,
    RichLog,
    Input,
    Button,
    Header,
    Footer,
)
from textual.widgets._option_list import Option

if TYPE_CHECKING:
    from watchdog.observers import BaseObserver

from src.crawler.engine import Crawler, QueueRecharger
from src.common.paths import config_files_folder_path
from src.configs.crawler_config import CrawlerConfig
from src.crawler.bootstrap import launch_crawler
from src.crawler.log_levels import LoggingLevels
from src.database.model import CrawledURL, WaitingURL
from src.database.session import create_db_engine
from .i18n_setup import next_locale, set_locale, t

if TYPE_CHECKING:
    from loguru import Message

# Définition ultra-spécifique des scalaires de configuration (sans aucun Any)
type ConfigsTypes = type[int] | type[float] | type[str] | type[bool] | type[None] | types.UnionType
type ConfigsTypesTree = ConfigsTypes | type[list[Any]] | types.GenericAlias | dict[str, ConfigsTypesTree]

class CallbackButton(Button):
    """Bouton Textual qui exécute une fonction de rappel lorsqu'il est pressé.
    Cette classe permet de lier simplement une action personnalisée à un clic sur le bouton.

    Attributes:
        callback (Callable[[], None]): La fonction à appeler lorsque le bouton est pressé.
    """
    def __init__(self, label: str, callback: Callable[[], None], *args, **kwargs):
        super().__init__(label, *args, **kwargs)
        self.callback = callback

    def on_button_pressed(self, _event: Button.Pressed):
        self.callback()


class ThreadManagingWidget(Horizontal):
    """Widget Textual qui affiche l'état d'un thread crawler et permet de le contrôler.
    Cette classe fournit des boutons pour mettre en pause, reprendre ou arrêter le thread tout en mettant à jour les métriques affichées.

    Attributes:
        thread (dict[Crawler | QueueRecharger, tuple[threading.Event, threading.Event]]): Le thread géré avec ses événements de contrôle.
        pause_button (CallbackButton): Le bouton permettant de mettre le thread en pause.
        resume_button (CallbackButton): Le bouton permettant de reprendre le thread après une pause.
    """
    def __init__(self, thread: dict[Crawler | QueueRecharger, tuple[threading.Event, threading.Event]], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.thread = thread
        self.pause_button = CallbackButton(t("thread-pause"), callback=tuple(self.thread.values())[0][1].set, classes="thread-action")
        self.resume_button = CallbackButton(t("thread-resume"), callback=tuple(self.thread.values())[0][1].clear, classes="thread-action")

    def on_mount(self):
        self.set_interval(1, self.update_labels)

    def compose(self) -> ComposeResult:
        """Construit la vue Textual affichant les informations et contrôles du thread.
        Cette méthode crée les labels de statut et de métriques ainsi que les boutons d'action pour piloter le thread.

        Returns:
            ComposeResult: Un générateur de widgets Textual représentant le nom du thread, son statut, ses métriques et les boutons de contrôle.
        """
        self.pause_button.disabled = False
        self.resume_button.disabled = False

        thread = tuple(self.thread.keys())[0]

        yield Label(thread.name, classes="thread-name")
        yield Label(t("thread-running"), id="status_label", classes="status-badge status-running")
        pages_text = t("thread-pages-count", count=0) if hasattr(thread, "pages_crawled") else "—"
        yield Label(pages_text, id="pages_label", classes="thread-metric")
        yield Label("", id="activity_label", classes="thread-metric")
        yield self.pause_button
        yield self.resume_button
        yield CallbackButton(t("thread-stop"), callback=tuple(self.thread.values())[0][0].set, id="stop_button", classes="thread-action")

    def retranslate(self) -> None:
        self.pause_button.label = t("thread-pause")
        self.resume_button.label = t("thread-resume")
        self.query_one("#stop_button", Button).label = t("thread-stop")
        self.update_labels()

    def update_labels(self) -> None:
        """Met à jour les libellés du widget selon la langue courante.
        Cette méthode rafraîchit les textes des boutons et des indicateurs pour refléter la traduction active.

        Returns:
            None: Cette méthode ne renvoie rien mais met à jour l'affichage du widget.
        """
        thread = tuple(self.thread.keys())[0]
        status_label = cast(Label, self.query_one("#status_label"))

        if not thread.is_alive():
            status_label.update(t("thread-stopped"))
            status_label.set_classes("status-badge status-stopped")
        elif thread.paused:
            status_label.update(t("thread-paused"))
            status_label.set_classes("status-badge status-paused")
        else:
            status_label.update(t("thread-running"))
            status_label.set_classes("status-badge status-running")

        if hasattr(thread, "pages_crawled"):
            self.query_one("#pages_label", Label).update(t("thread-pages-count", count=thread.pages_crawled))

        elapsed = int(time.time() - thread.last_activity)
        if elapsed < 60:
            activity_text = t("thread-active-seconds-ago", seconds=elapsed)
        else:
            activity_text = t("thread-active-minutes-ago", minutes=elapsed // 60)
        self.query_one("#activity_label", Label).update(activity_text)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Gère les actions de l'utilisateur lorsqu'un bouton du widget est pressé.
        Cette méthode met à jour l'état du thread et des boutons en fonction du bouton activé.

        Args:
            event (Button.Pressed): L'événement déclenché lors de l'appui sur un bouton du widget.
        """
        if event.button == self.query_one("#stop_button"):
            status_label = cast(Label, self.query_one("#status_label"))
            status_label.update(t("thread-stopped"))
            status_label.set_classes("status-badge status-stopped")
            return
        self.set_state(event.button == self.pause_button)

    def set_state(self, state: bool) -> None:
        self.pause_button.disabled = state
        self.resume_button.disabled = not state


class DashboardPage(Container):
    """Page Textual qui affiche un tableau de bord des métriques du crawler en temps réel.
    Cette classe permet de suivre l'activité globale des threads de crawling et de les contrôler collectivement.

    Attributes:
        cache (Cache | None): Cache utilisé pour compter le nombre de domaines visités.
        crawled_urls_bf (Bloom | None): Bloom filter permettant d'estimer le nombre d'URL déjà crawlées.
        _engine: Moteur de base de données utilisé pour interroger les tables de crawl.
        crawlers (list[dict[Crawler, tuple[threading.Event, threading.Event]]]): Liste des crawlers avec leurs événements de contrôle.
        queue_recharger (dict[QueueRecharger, tuple[threading.Event, threading.Event]]): Thread chargé de réalimenter la file d'attente avec ses événements de contrôle.
        _last_crawled_count (int | None): Dernier nombre d’URLs crawlées utilisé pour calculer le débit.
        _last_sample_time (float | None): Timestamp de la dernière mesure de débit de crawl.
        _last_waiting_count (int | None): Dernier nombre d’URLs en attente, utilisé pour calculer la tendance.
    """
    def __init__(self, crawlers: list[dict[Crawler, tuple[threading.Event, threading.Event]]], queue_recharger: dict[QueueRecharger, tuple[threading.Event, threading.Event]], cache: Cache | None = None, crawled_urls_bf: Bloom | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.cache = cache
        self.crawled_urls_bf = crawled_urls_bf
        self._engine = create_db_engine()

        self.crawlers = crawlers
        self.queue_recharger = queue_recharger

        self._last_crawled_count: int | None = None
        self._last_sample_time: float | None = None
        self._last_waiting_count: int | None = None

    def compose(self) -> ComposeResult:
        """Construit la disposition de la page de tableau de bord du crawler.
        Cette méthode assemble les tuiles de statistiques globales, la barre d’outils de contrôle et la liste des threads.

        Returns:
            ComposeResult: Un générateur de widgets Textual représentant les métriques, les contrôles et les threads gérés par le tableau de bord.
        """
        with Horizontal(classes="stats-row"):
            with Container(classes="stat-tile"):
                yield Label("--", id="domains", classes="stat-value")
                yield Label(t("dashboard-domains-visited"), id="domains_label", classes="stat-label")
            with Container(classes="stat-tile"):
                yield Label("--", id="crawled_pages", classes="stat-value")
                yield Label(t("dashboard-pages-crawled"), id="crawled_pages_label", classes="stat-label")
            with Container(classes="stat-tile"):
                yield Label("--", id="crawl_rate", classes="stat-value")
                yield Label(t("dashboard-pages-per-minute"), id="crawl_rate_label", classes="stat-label")
            with Container(classes="stat-tile"):
                yield Label("--", id="waiting_pages", classes="stat-value")
                yield Label(t("dashboard-urls-waiting"), id="waiting_pages_label", classes="stat-label")

        with Horizontal(classes="stats-row"):
            with Container(classes="stat-tile"):
                yield Label("--", id="recent_errors", classes="stat-value")
                yield Label(t("dashboard-recent-errors"), id="recent_errors_label", classes="stat-label")
            with Container(classes="stat-tile"):
                yield Label("--", id="bloom_filter", classes="stat-value")
                yield Label(t("dashboard-bloom-filter"), id="bloom_filter_label", classes="stat-label")
            with Container(classes="stat-tile"):
                yield Label("--:--:--", id="uptime", classes="stat-value")
                yield Label(t("dashboard-uptime"), id="uptime_label", classes="stat-label")

        def pause_all():
            tuple(self.queue_recharger.values())[0][1].set()
            for crawler in self.crawlers:
                tuple(crawler.values())[0][1].set()

        def resume_all():
            tuple(self.queue_recharger.values())[0][1].clear()
            for crawler in self.crawlers:
                tuple(crawler.values())[0][1].clear()

        def stop_all():
            tuple(self.queue_recharger.values())[0][0].set()
            for crawler in self.crawlers:
                tuple(crawler.values())[0][0].set()

        with Horizontal(classes="toolbar"):
            yield Label(t("dashboard-all-threads"), id="all_threads_label", classes="toolbar-title")
            yield CallbackButton(t("dashboard-pause"), callback=pause_all, id="pause_all_button")
            yield CallbackButton(t("dashboard-resume"), callback=resume_all, id="resume_all_button")
            yield CallbackButton(t("dashboard-stop"), callback=stop_all, id="stop_all_button", variant="error")

        yield Label(t("dashboard-threads-running"), id="threads_running_label", classes="section-title")
        with VerticalScroll(id="threads-list"):
            yield ThreadManagingWidget(self.queue_recharger)
            for crawler in self.crawlers:
                    yield ThreadManagingWidget(crawler)

    def retranslate(self) -> None:
        """Met à jour tous les libellés du tableau de bord selon la langue active.
        Cette méthode rafraîchit les textes des tuiles de statistiques et des contrôles pour refléter la traduction courante.

        Returns:
            None: Cette méthode ne renvoie rien mais met à jour l'affichage des différents widgets du tableau de bord.
        """
        self.query_one("#domains_label", Label).update(t("dashboard-domains-visited"))
        self.query_one("#crawled_pages_label", Label).update(t("dashboard-pages-crawled"))
        self.query_one("#crawl_rate_label", Label).update(t("dashboard-pages-per-minute"))
        self.query_one("#waiting_pages_label", Label).update(t("dashboard-urls-waiting"))
        self.query_one("#recent_errors_label", Label).update(t("dashboard-recent-errors"))
        self.query_one("#bloom_filter_label", Label).update(t("dashboard-bloom-filter"))
        self.query_one("#uptime_label", Label).update(t("dashboard-uptime"))
        self.query_one("#all_threads_label", Label).update(t("dashboard-all-threads"))
        self.query_one("#pause_all_button", Button).label = t("dashboard-pause")
        self.query_one("#resume_all_button", Button).label = t("dashboard-resume")
        self.query_one("#stop_all_button", Button).label = t("dashboard-stop")
        self.query_one("#threads_running_label", Label).update(t("dashboard-threads-running"))

    def on_mount(self):
        self.set_interval(5, self.update_content)

    def update_content(self):
        """Met à jour périodiquement les métriques et compteurs affichés sur le tableau de bord.
        Cette méthode rafraîchit les valeurs issues de la base de données, des logs et des filtres en fonction de l'état courant du crawler.

        Returns:
            None: Cette méthode ne renvoie rien mais met à jour les labels de statistiques globales du tableau de bord.
        """
        now = time.time()

        # noinspection PyBroadException
        try:
            self._extracted_from_update_content_6(now)
        except Exception:
            self.query_one("#domains", Label).update("...")
            self.query_one("#crawled_pages", Label).update("...")
            self.query_one("#waiting_pages", Label).update("...")

        log_history = self.app.logs_home_page.logs_page.log_history
        recent_errors = sum(level_no >= 40 for level_no, _, _ in log_history)
        self.query_one("#recent_errors", Label).update(str(recent_errors))

        if self.crawled_urls_bf is not None:
            self.query_one("#bloom_filter", Label).update(f"{self.crawled_urls_bf.approx_items:,.0f}".replace(",", " "))

        start_time = self.app.start_time
        if start_time is not None:
            elapsed = int(now - start_time)
            hours, remainder = divmod(elapsed, 3600)
            minutes, seconds = divmod(remainder, 60)
            self.query_one("#uptime", Label).update(f"{hours:02d}:{minutes:02d}:{seconds:02d}")

    def _extracted_from_update_content_6(self, now):
        """Calcule et met à jour les métriques détaillées du tableau de bord à partir de la base de données.
        Cette méthode rafraîchit le nombre de pages crawlées, de domaines, le débit et la tendance des URLs en attente.

        Args:
            now (float): Horodatage actuel utilisé pour calculer le débit de crawl sur la période écoulée.
        """
        with self._engine.connect() as conn:
            num_crawled_urls = cast(int, conn.scalar(select(func.count()).select_from(CrawledURL)))
            num_urls_in_waiting_list = cast(int, conn.scalar(select(func.count()).select_from(WaitingURL)))

        num_domains = len(self.cache) if self.cache else 0
        self.query_one("#domains", Label).update(str(num_domains))
        self.query_one("#crawled_pages", Label).update(str(num_crawled_urls))

        if self._last_crawled_count is not None and self._last_sample_time is not None:
            elapsed_minutes = (now - self._last_sample_time) / 60
            rate = (num_crawled_urls - self._last_crawled_count) / elapsed_minutes if elapsed_minutes > 0 else 0
            self.query_one("#crawl_rate", Label).update(f"{rate:.1f}")
        self._last_crawled_count = num_crawled_urls
        self._last_sample_time = now

        trend = ""
        if self._last_waiting_count is not None:
            if num_urls_in_waiting_list > self._last_waiting_count:
                trend = " ▲"
            elif num_urls_in_waiting_list < self._last_waiting_count:
                trend = " ▼"
            else:
                trend = " ="
        self._last_waiting_count = num_urls_in_waiting_list
        self.query_one("#waiting_pages", Label).update(f"{num_urls_in_waiting_list}{trend}")


class LogsPage(RichLog):
    """Page Textual d'affichage des logs du crawler avec filtrage par niveau et source.
    Cette classe permet de sélectionner un crawler, d'ajuster le seuil de logs et d'afficher les messages en temps réel.

    Attributes:
        BINDINGS (list[Binding]): Raccourcis clavier pour revenir à l'accueil et changer le niveau de logs.
        log_history (deque[tuple[int, str, str]]): Historique circulaire des messages de log (niveau, nom du crawler, contenu brut).
        levels (list[int]): Liste ordonnée des niveaux numériques disponibles pour le filtrage des logs.
        _crawler_name (str): Nom du crawler actuellement sélectionné pour l'affichage des logs.
        current_level_index (int): Index courant dans la liste `levels` qui détermine le seuil de log actif.
        buffer_lock (threading.Lock): Verrou protégeant l'accès concurrent au buffer de logs.
        log_buffer (list[tuple[int, str, str]]): Buffer temporaire des messages à tirer périodiquement vers l'affichage.
    """
    BINDINGS = [
        Binding("escape", "back_to_home", t("logs-back")),
        Binding("backspace", "back_to_home", t("logs-back")),
        Binding("+", "increase_level", t("logs-level-up"), key_display="+"),
        Binding("-", "decrease_level", t("logs-level-down"), key_display="-"),
    ]

    def __init__(self, *args, **kwargs):
        kwargs["max_lines"] = 1000
        super().__init__(*args, **kwargs)

        self.log_history: deque[tuple[int, str, str]] = deque(maxlen=1000)
        self.levels = [5, 10, 20, 25, 30, 40, 50, 60]
        self._crawler_name = "Crawler-all"
        self.current_level_index = 2

        self.buffer_lock = threading.Lock()
        self.log_buffer: list[tuple[int, str, str]] = []

    @property
    def crawler_name(self) -> str:
        return self._crawler_name

    @crawler_name.setter
    def crawler_name(self, crawler_name: str) -> None:
        self._crawler_name = crawler_name
        self._refresh_logs()
        self._update_status_bar()

    def _update_status_bar(self) -> None:
        """Met à jour la barre de statut de la page des logs en fonction du crawler et du niveau courant.
        Cette méthode informe la vue parent de l'état actuel du filtre pour que l’interface reflète les paramètres de journalisation actifs.

        Returns:
            None: Cette méthode ne renvoie rien mais met à jour le texte affiché dans la barre de statut des logs.
        """
        home: LogsHomePage = self.app.logs_home_page
        home.update_status_bar(t("logs-status-bar", crawler=self.crawler_name, level=self.current_level_name))

    def on_mount(self) -> None:
        self.set_interval(0.2, self.pull_logs)

    def action_back_to_home(self) -> None:
        """Revient à l’écran d’accueil de sélection de la source des logs.
        Cette action réinitialise la vue et le texte de statut afin de permettre à l’utilisateur de choisir un autre flux de journalisation.

        Returns:
            None: Cette méthode ne renvoie rien mais modifie la vue active et la barre de statut de la page des logs.
        """
        home: LogsHomePage = self.app.logs_home_page
        home.query_one(ContentSwitcher).current = "option_list"
        home.update_status_bar(t("logs-select-source"))
        home.focus()

    def pull_logs(self):
        """Transfère périodiquement les messages du buffer de logs vers l'affichage.
        Cette méthode filtre les messages selon le niveau et la source sélectionnés avant de les écrire dans la vue Textual.

        Returns:
            None: Cette méthode ne renvoie rien mais met à jour le contenu visible des logs et déclenche un rafraîchissement de l'interface.
        """
        with self.buffer_lock:
            if not self.log_buffer:
                return
            batch = self.log_buffer
            self.log_buffer = []

        threshold = self.current_level_no
        to_write = []
        for level_no, crawler_name, raw_msg in batch:
            self.log_history.append((level_no, crawler_name, raw_msg))
            if level_no >= threshold and (crawler_name == self.crawler_name or self.crawler_name == "Crawler-all"):
                to_write.append(raw_msg)

        if to_write:
            for msg in to_write:
                self.write(Text.from_ansi(msg))
            # Bug Textual : après une purge interne (buffer > max_lines, fréquent
            # avec les tracebacks multi-lignes), write_lines() marque la mauvaise
            # plage de lignes "à repeindre" car les index ont été décalés par la
            # purge -> résidus/vides à l'écran. Le cache de rendu reste correct
            # (vérifié), donc un simple refresh() suffit à corriger la zone
            # repeinte, sans coût de recalcul supplémentaire.
            self.refresh()

    @property
    def current_level_no(self) -> int:
        return self.levels[self.current_level_index]

    @property
    def current_level_name(self) -> LoggingLevels:
        levels_map = {
            5: LoggingLevels.TRACE,
            10: LoggingLevels.DEBUG,
            20: LoggingLevels.INFO,
            25: LoggingLevels.SUCCESS,
            30: LoggingLevels.WARNING,
            40: LoggingLevels.ERROR,
            50: LoggingLevels.CRITICAL,
            60: LoggingLevels.FATAL,
        }
        return levels_map.get(self.current_level_no, LoggingLevels.INFO)

    def action_increase_level(self):
        if self.current_level_index < len(self.levels) - 1:
            self.current_level_index += 1
            self._refresh_logs()
            self._update_status_bar()

    def action_decrease_level(self):
        if self.current_level_index > 0:
            self.current_level_index -= 1
            self._refresh_logs()
            self._update_status_bar()

    def _refresh_logs(self):
        """Augmente le seuil de niveau de log utilisé pour filtrer les messages affichés.
        Cette action passe au niveau de gravité supérieur et déclenche un rafraîchissement du contenu et de la barre de statut.

        Returns:
            None: Cette méthode ne renvoie rien mais met à jour le filtre de logs et l'affichage de la page.
        """
        self.clear()
        threshold = self.current_level_no

        if to_write := [
            raw_msg
            for lvl, crawler_name, raw_msg in self.log_history
            if lvl >= threshold
            and (
                crawler_name == self.crawler_name
                or self.crawler_name == "Crawler-all"
            )
        ]:
            # noinspection PyUnboundLocalVariable
            for msg in to_write:
                self.write(Text.from_ansi(msg))
            self.refresh()

class LogsHomePage(Container):
    """Augmente le seuil de niveau de log pour restreindre les messages affichés aux plus importants.
    Cette action passe au niveau de gravité immédiatement supérieur puis réapplique le filtre et met à jour la barre de statut.

    Returns:
        None: Cette méthode ne renvoie rien mais modifie le niveau de filtrage des logs et rafraîchit l’affichage associé.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logs_page = LogsPage(id="logs_page")

    def compose(self) -> ComposeResult:
        """Construit la vue d’accueil de sélection et de consultation des sources de logs.
        Cette méthode assemble la barre de statut et le commutateur de contenu permettant de choisir un crawler ou d’afficher la page détaillée des logs.

        Returns:
            ComposeResult: Un générateur de widgets Textual contenant la barre de statut, la liste des sources de logs et la page d’affichage associée.
        """
        yield Label(t("logs-select-source"), id="logs_status_bar", classes="logs-status-bar")
        with ContentSwitcher(initial="option_list"):
            yield self.logs_page
            yield OptionList(
                Option(t("logs-all-crawlers"), id="Crawler-all"),
                Option("MainThread", id="MainThread"),
                Option("QueueRecharger", id="QueueRecharger"),
                *(Option(f"Crawler-{i+1}", id=f"Crawler-{i+1}") for i in range(self.app.config.num_crawlers)),
                id="option_list"
            )

    def retranslate(self) -> None:
        """Met à jour les libellés et textes de la page d’accueil des logs selon la langue active.
        Cette méthode ajuste la barre de statut et les options de la liste des crawlers pour refléter la traduction courante.

        Returns:
            None: Cette méthode ne renvoie rien mais rafraîchit les textes affichés sur la page d’accueil des logs.
        """
        content_switcher = self.query_one(ContentSwitcher)
        if content_switcher.current == "option_list":
            self.update_status_bar(t("logs-select-source"))
        else:
            self.logs_page._update_status_bar()
        self.query_one("#option_list", OptionList).replace_option_prompt("Crawler-all", t("logs-all-crawlers"))

    def update_status_bar(self, text: str) -> None:
        self.query_one("#logs_status_bar", Label).update(text)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        """Active l’affichage de la page de logs pour la source sélectionnée.
        Cette méthode bascule le commutateur de contenu sur la vue des logs, cible le crawler choisi et donne le focus à la page correspondante.

        Args:
            event (OptionList.OptionSelected): L’événement déclenché lors de la sélection d’une option de source de logs.
        """
        self.query_one(ContentSwitcher).current = "logs_page"
        self.logs_page.crawler_name = event.option_id
        self.logs_page.focus()


class DatabaseConsolePage(Container):
    """Page Textual offrant une console PostgreSQL intégrée pour administrer la base de données du crawler.
    Cette classe gère la saisie des identifiants administrateur, la persistance dans le .env et l'exécution de requêtes SQL via psql.

    Attributes:
        BINDINGS (list[Binding]): Raccourcis clavier permettant d'exécuter la requête en cours dans la console SQL.
        DEFAULT_CSS (str): Feuille de style CSS utilisée pour ajuster l'affichage des champs de connexion et de la console.
    """
    BINDINGS = [
        Binding("f5", "execute_query", t("database-execute-f5")),
        Binding("ctrl+j", "execute_query", t("database-execute-ctrl-enter")),
    ]

    DEFAULT_CSS = """
    #password_container {
        height: auto;
    }
    #admin_pass_input {
        width: 1fr;
    }
    #toggle_password {
        min-width: 6;
        margin-left: 1;
    }
    """

    def __init__(
        self,
        *,
        id: str | None = None,
    ):
        super().__init__(id=id)

    def compose(self) -> ComposeResult:
        """Construit la vue de la console de base de données avec les écrans de connexion et d'exécution SQL.
        Cette méthode assemble le formulaire de saisie des identifiants administrateur et la console PostgreSQL interactive.

        Returns:
            ComposeResult: Un générateur de widgets Textual comprenant le sélecteur de vue, le formulaire de connexion, l’éditeur SQL et le log de résultats.
        """
        with ContentSwitcher(id="db_view_switcher"):
            # --- VUE 1 : FORMULAIRE DE CONNEXION ---
            with Container(id="login_view"):
                yield Label(t("database-admin-credentials-title"), id="login_title")

                yield Input(
                    id="admin_user_input",
                    placeholder=t("database-admin-username-placeholder"),
                )

                with Horizontal(id="password_container"):
                    yield Input(
                        id="admin_pass_input", placeholder=t("database-password-placeholder"), password=True
                    )
                    yield Button("👁", id="toggle_password")

                yield Button(
                    t("database-save-and-access"), id="save_admin_creds", variant="success"
                )

            # --- VUE 2 : CONSOLE POSTGRESQL ---
            with Container(id="console_view"):
                with Horizontal(id="console_header"):
                    yield Label(
                        t("database-console-title", db_name=os.getenv("DB_NAME", "?")),
                        id="console_title",
                    )
                    yield Button(
                        t("database-edit-credentials"),
                        id="edit_admin_creds",
                        variant="warning",
                    )

                yield TextArea(
                    id="editor",
                    language="sql",
                )

                yield RichLog(
                    id="logs",
                    highlight=True,
                    markup=False,
                )

    def retranslate(self) -> None:
        self.query_one("#login_title", Label).update(t("database-admin-credentials-title"))
        self.query_one("#admin_user_input", Input).placeholder = t("database-admin-username-placeholder")
        self.query_one("#admin_pass_input", Input).placeholder = t("database-password-placeholder")
        self.query_one("#save_admin_creds", Button).label = t("database-save-and-access")
        self.query_one("#console_title", Label).update(t("database-console-title", db_name=os.getenv("DB_NAME", "?")))
        self.query_one("#edit_admin_creds", Button).label = t("database-edit-credentials")

    def on_mount(self) -> None:
        admin_user = os.getenv("ADMIN_DB_USERNAME")
        admin_pass = os.getenv("ADMIN_DB_PASSWORD")

        switcher = self.query_one("#db_view_switcher", ContentSwitcher)

        if admin_user and admin_pass:
            switcher.current = "console_view"
        else:
            switcher.current = "login_view"

    @staticmethod
    def update_env_file(key: str, value: str) -> None:
        """Met à jour ou ajoute une variable d'environnement dans le fichier .env du projet.
        Cette méthode garantit la présence du fichier, remplace la valeur existante de la clé ou l'ajoute proprement en fin de fichier.

        Args:
            key (str): Nom de la variable d'environnement à écrire ou mettre à jour dans le fichier .env.
            value (str): Valeur à associer à la clé spécifiée, écrite sous la forme `KEY=VALUE`.
        """
        filepath = ".env"

        # On s'assure que le fichier existe, sinon on le crée
        if not os.path.exists(filepath):
            with open(filepath, "w", encoding="utf-8"):
                pass

        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()

        updated = False
        for i, line in enumerate(lines):
            # On nettoie la ligne pour éviter les espaces parasites lors de la comparaison
            stripped = line.strip()
            if stripped.startswith(f"{key}=") or stripped.startswith(f"{key} ="):
                lines[i] = f"{key}={value}\n"
                updated = True
                break

        # Si la variable n'était pas dans le .env, on l'écrit à la fin
        if not updated:
            # S'il manque un saut de ligne tout à la fin, on le rajoute pour ne pas coller les clés
            if lines and not lines[-1].endswith("\n"):
                lines.append("\n")
            lines.append(f"{key}={value}\n")

        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(lines)

    @staticmethod
    def interpret_postgres_error(output: str) -> str:
        """Interprète un message d'erreur brut de PostgreSQL en texte lisible pour l'utilisateur.
        Cette méthode détecte les principaux cas d'erreur de connexion ou de configuration et renvoie la traduction appropriée.

        Args:
            output (str): Sortie texte complète retournée par la commande psql lors d'une tentative de connexion ou d'exécution.

        Returns:
            str: Message d'erreur contextualisé et traduit décrivant la cause probable du problème de connexion ou de requête.
        """
        if "password authentication failed" in output:
            return t("database-wrong-password")
        elif "role" in output and "does not exist" in output:
            return t("database-role-not-exist")
        elif "database" in output and "does not exist" in output:
            return t("database-db-not-exist")
        elif (
            "connection to server" in output
            or "could not connect to server" in output
            or "Connection refused" in output
        ):
            return t("database-connection-impossible")

        return t("database-generic-connection-error")

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        """Gère les actions des boutons de la page console (affichage du mot de passe, test de connexion et édition des identifiants).
        Cette méthode orchestre la vérification des champs, la tentative de connexion psql, la persistance des identifiants et le basculement entre les vues.

        Args:
            event (Button.Pressed): L'événement émis par Textual lorsqu'un des boutons de la page console est pressé.
        """
        # --- 1. AFFICHER / MASQUER LE MOT DE PASSE ---
        if event.button.id == "toggle_password":
            pass_input = self.query_one("#admin_pass_input", Input)
            pass_input.password = not pass_input.password
            event.button.label = "🙈" if pass_input.password else "👁"

        # --- 2. SAUVEGARDER ET TESTER LA CONNEXION ---
        elif event.button.id == "save_admin_creds":
            user_val = self.query_one("#admin_user_input", Input).value.strip()
            pass_val = self.query_one("#admin_pass_input", Input).value

            if not user_val or not pass_val:
                self.app.notify(
                    t("database-fill-both-fields"),
                    severity="error",
                    title=t("database-error-title"),
                )
                return

            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("DB_NAME", "postgres")

            psql_env = os.environ.copy()
            psql_env["PGPASSWORD"] = pass_val

            event.button.label = t("database-testing-connection")
            event.button.disabled = True

            try:
                args = [
                    "psql",
                    "-h",
                    db_host,
                    "-p",
                    str(db_port),
                    "-U",
                    user_val,
                    "-d",
                    db_name,
                    "-w",
                    "-c",
                    "SELECT 1;",
                ]

                process = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    env=psql_env,
                )

                stdout_data, _ = await process.communicate()
                output = stdout_data.decode("utf-8", errors="replace")

                if process.returncode == 0:
                    # A. Sauvegarde dans la session courante de l'app
                    os.environ["ADMIN_DB_USERNAME"] = user_val
                    os.environ["ADMIN_DB_PASSWORD"] = pass_val

                    # B. Écriture / Mise à jour persistante dans le fichier .env
                    self.update_env_file("ADMIN_DB_USERNAME", user_val)
                    self.update_env_file("ADMIN_DB_PASSWORD", pass_val)

                    self.app.notify(
                        t("database-connection-success"), severity="information"
                    )
                    self.query_one(
                        "#db_view_switcher", ContentSwitcher
                    ).current = "console_view"
                    self.query_one("#editor", TextArea).focus()
                else:
                    err_msg = self.interpret_postgres_error(output)
                    self.app.notify(
                        err_msg, severity="error", title=t("database-connection-failed-title")
                    )

            except FileNotFoundError:
                self.app.notify(
                    t("database-psql-not-found"), severity="error"
                )
            except Exception as e:
                self.app.notify(t("database-unexpected-error", error=e), severity="error")
            finally:
                event.button.label = t("database-save-and-access")
                event.button.disabled = False

        # --- 3. MODIFIER LES IDENTIFIANTS ---
        elif event.button.id == "edit_admin_creds":
            self.query_one("#db_view_switcher", ContentSwitcher).current = "login_view"

            user_input = self.query_one("#admin_user_input", Input)
            user_input.value = os.getenv("ADMIN_DB_USERNAME", "")

            pass_input = self.query_one("#admin_pass_input", Input)
            pass_input.value = ""
            pass_input.password = True
            self.query_one("#toggle_password", Button).label = "👁"

            user_input.focus()

    @property
    def editor(self) -> TextArea:
        return self.query_one("#editor", TextArea)

    @property
    def logs(self) -> RichLog:
        return self.query_one("#logs", RichLog)

    async def action_execute_query(self) -> None:
        """Exécute la requête SQL saisie dans l’éditeur au sein de la console PostgreSQL intégrée.
        Cette méthode gère les validations de base, interdit les changements de connexion et journalise le résultat ou les erreurs dans la vue de logs.

        Returns:
            None: Cette méthode ne renvoie rien mais envoie la requête au processus psql et met à jour le log de la console avec la sortie correspondante.
        """
        if (
            self.query_one("#db_view_switcher", ContentSwitcher).current
            != "console_view"
        ):
            return

        query = self.editor.text.strip()

        if not query:
            return

        if re.search(r"^\\c(onnect)?\s+", query, re.IGNORECASE | re.MULTILINE):
            self.logs.write(f"\n=> {query}")
            self.logs.write(t("database-action-refused-connect"))
            self.editor.text = ""
            return

        self.logs.write(f"\n=> {query}")
        self.editor.text = ""

        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "postgres")

        admin_user = os.getenv("ADMIN_DB_USERNAME", "postgres")
        admin_pass = os.getenv("ADMIN_DB_PASSWORD", "")

        psql_env = os.environ.copy()
        psql_env["PGPASSWORD"] = admin_pass

        try:
            args = [
                "psql",
                "-h",
                db_host,
                "-p",
                str(db_port),
                "-U",
                admin_user,
                "-d",
                db_name,
                "-w",
            ]

            process = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=psql_env,
            )

            stdout_data, _ = await process.communicate(input=query.encode("utf-8"))

            output = stdout_data.decode("utf-8", errors="replace")
            self.logs.write(output)

        except FileNotFoundError:
            self.logs.write(t("database-psql-not-found-console"))
        except Exception as e:
            self.logs.write(t("database-unexpected-error-console", error=e))


class ConfigInput(Input):
    """Champ de saisie Textual spécialisé pour l’édition d’une valeur de configuration du crawler.
    Cette classe associe chaque input à son type cible et à son chemin dans l’arbre de configuration pour permettre une validation et une sauvegarde précises.

    Attributes:
        config_type (ConfigsTypes): Type attendu pour la valeur saisie, utilisé pour la validation et le parsing.
        config_path (list[str | int]): Chemin dans la configuration pointant vers la clé ou l’index à mettre à jour.
    """
    def __init__(
        self,
        config_type: ConfigsTypes,
        config_path: list[str | int],
        value: str | None = None,
        placeholder: str = "",
        highlighter: Any = None,
        password: bool = False,
        restrict: str | None = None,
        id: str | None = None,
        classes: str | None = None,
        disabled: bool = False,
    ):
        # On passe explicitement les paramètres nommés pour éviter que l'IDE ne s'emmêle avec les arguments positionnels
        super().__init__(
            value=value,
            placeholder=placeholder,
            highlighter=highlighter,
            password=password,
            restrict=restrict,
            id=id,
            classes=classes,
            disabled=disabled,
        )
        self.config_type = config_type
        self.config_path = config_path


class ConfigsPage(Container):
    """Page Textual d’édition interactive de la configuration du crawler.
    Cette classe génère dynamiquement les champs de saisie à partir du modèle Pydantic, applique des restrictions de format et enregistre les modifications.

    Attributes:
        config (CrawlerConfig): Instance de configuration actuellement éditée dans l’interface.
        type_restrictions (dict[type, str]): Expressions régulières de saisie associées aux types scalaires pour guider et valider l’entrée utilisateur.
    """
    def __init__(self, config: CrawlerConfig, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.config = config

        self.type_restrictions = {
            int: r"^(?:-?\d+)?$",
            float: r"^(?:-?(?:\d+\.\d+|\d+\.?|\.\d+)(?:[eE][+-]?\d+)?)?$",
            str: r"^.*$",
            bool: r"^.*$",
            type(None): r"^.*$",
        }

    @staticmethod
    def _unwrap_union_type(tp: ConfigsTypesTree) -> ConfigsTypesTree:
        """Simplifie un type Union de configuration en retirant l’option None lorsqu’elle est seule alternative.
        Cette méthode permet de récupérer le type effectif à utiliser pour le rendu et la validation des champs de configuration scalaires ou imbriqués.

        Args:
            tp (ConfigsTypesTree): Type ou arbre de types issu du modèle Pydantic, potentiellement composé d’Unions.

        Returns:
            ConfigsTypesTree: Type de configuration nettoyé, sans Union optionnel superflu, ou le type original s’il ne peut être simplifié.
        """
        origin = get_origin(tp)
        if origin is Union or isinstance(tp, types.UnionType):
            non_none_types = [arg for arg in get_args(tp) if arg is not type(None)]
            if len(non_none_types) == 1:
                return cast(ConfigsTypesTree, non_none_types[0])
        return tp

    def get_restriction(self, tp: ConfigsTypes) -> str:
        """Construit une expression régulière de restriction adaptée au type de configuration fourni.
        Cette méthode gère notamment les Unions en combinant les patterns autorisés pour guider et valider la saisie utilisateur dans les champs de configuration.

        Args:
            tp (ConfigsTypes): Type de configuration pour lequel déterminer une règle de restriction de saisie.

        Returns:
            str: Pattern de restriction à utiliser par les champs de saisie, couvrant les formats acceptés pour le type ou l’Union donnée.
        """
        origin = get_origin(tp)

        if origin is Union or isinstance(tp, types.UnionType):
            sub_restrictions = [self.get_restriction(arg) for arg in get_args(tp)]

            if r"^.*$" in sub_restrictions:
                return r"^.*$"

            clean_patterns = []
            for pat in sub_restrictions:
                if pat.startswith("^"):
                    pat = pat[1:]
                if pat.endswith("$"):
                    pat = pat[:-1]
                clean_patterns.append(pat)
            return f"^(?:{'|'.join(clean_patterns)})$"

        return self.type_restrictions.get(tp, r"^.*$")

    @staticmethod
    def _parse_scalar(
            value_str: str,
        target_type: type[int] | type[float] | type[str] | type[bool] | type[None],
    ) -> int | float | str | bool | None:
        """Convertit une chaîne saisie par l'utilisateur en valeur scalaire du type cible.
        Cette méthode applique des règles strictes pour les booléens et les valeurs nulles, et signale clairement les erreurs de conversion ou de type non supporté.

        Args:
            value_str (str): Texte brut issu du champ de configuration à convertir.
            target_type (type[int] | type[float] | type[str] | type[bool] | type[None]): Type scalaire attendu pour la valeur, utilisé pour choisir la conversion.

        Returns:
            int | float | str | bool | None: Valeur convertie dans le type demandé si la chaîne respecte le format attendu.

        Raises:
            ValueError: Si la chaîne ne correspond pas au format requis, ne peut pas être convertie ou si le type demandé n'est pas supporté.
        """
        value_stripped = value_str.strip()

        if target_type is type(None):
            if value_stripped == "None":
                return None
            raise ValueError(t("config-null-value-must-be-none"))

        if target_type is bool:
            if value_stripped == "True":
                return True
            if value_stripped == "False":
                return False
            raise ValueError(t("config-bool-value-must-be-true-false"))

        try:
            if target_type is int:
                return int(value_stripped)
            if target_type is float:
                return float(value_stripped)
            if target_type is str:
                return value_stripped
        except ValueError as err:
            name = getattr(target_type, "__name__", str(target_type))
            raise ValueError(
                t("config-cannot-convert", value=value_str, type=name)
            ) from err

        raise ValueError(t("config-unsupported-type", type=target_type))

    def parse_value(
        self, value_str: str, config_type: ConfigsTypes
    ) -> int | float | str | bool | None:
        """Interprète une chaîne de configuration en valeur typée en respectant les Unions et les valeurs nulles.
        Cette méthode tente successivement les types possibles d’un Union, gère explicitement "None" et renvoie des erreurs claires si aucun type ne correspond.

        Args:
            value_str (str): Valeur saisie par l'utilisateur dans le champ de configuration.
            config_type (ConfigsTypes): Type de configuration attendu, éventuellement composé d'un Union de types scalaires et de None.

        Returns:
            int | float | str | bool | None: Valeur convertie dans l’un des types autorisés par la configuration, ou None si la chaîne représente une valeur nulle.

        Raises:
            ValueError: Si aucune des variantes de type de l'Union ne peut être utilisée pour convertir la chaîne donnée.
        """
        origin = get_origin(config_type)

        if origin is Union or isinstance(config_type, types.UnionType):
            for arg in get_args(config_type):
                if arg is type(None) and value_str.strip() == "None":
                    return None

            for arg in get_args(config_type):
                if arg is not type(None):
                    try:
                        return self.parse_value(value_str, cast(ConfigsTypes, arg))
                    except ValueError:
                        continue

            allowed_names = ", ".join(
                "None" if arg is type(None) else getattr(arg, "__name__", str(arg))
                for arg in get_args(config_type)
            )
            raise ValueError(
                t("config-no-matching-union-type", value=value_str, types=allowed_names)
            )

        return self._parse_scalar(value_str, config_type)

    def compose(self) -> ComposeResult:
        for key, value, value_type, path in self.parse_config(
            self.config.model_dump(),
            self.get_type_tree(CrawlerConfig),
        ):
            """Construit dynamiquement l’interface d’édition de configuration à partir du modèle du crawler.
            Cette méthode produit les sections et champs de saisie Textual en appliquant l’indentation, les labels et les règles de restriction adaptées à chaque type.

            Returns:
                ComposeResult: Un générateur de widgets représentant les titres de sections et les lignes de configuration éditables.
            """
            leading_spaces = len(key) - len(key.lstrip(" "))
            indent_level = min(leading_spaces // 2, 4)
            display_key = key.strip()

            if value_type in [dict, list]:
                if display_key:
                    yield Label(display_key, classes=f"config-section-title indent-{indent_level}")
                continue

            scalar_type = cast(ConfigsTypes, value_type)
            with Horizontal(classes=f"config-row indent-{indent_level}"):
                label_classes = (
                    "config-label config-list-marker"
                    if display_key == "-"
                    else "config-label"
                )
                yield Label(display_key, classes=label_classes)
                yield ConfigInput(
                    config_type=scalar_type,
                    config_path=path,
                    value=str(value),
                    restrict=self.get_restriction(scalar_type),
                )
    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Valide et enregistre une valeur de configuration lorsque l'utilisateur soumet un champ.
        Cette méthode vérifie le format selon les restrictions, convertit la chaîne dans le type attendu, met à jour le modèle et persiste la configuration sur disque.

        Args:
            event (Input.Submitted): Événement Textual contenant le champ soumis et la valeur saisie.
        """
        config_input = cast(ConfigInput, event.input)

        if config_input.restrict and not re.match(config_input.restrict, event.value):
            self.notify(t("config-invalid-format"), severity="error")
            return

        try:
            value = self.parse_value(event.value, config_input.config_type)
        except ValueError as err:
            self.notify(str(err), severity="error", title=t("config-validation-error-title"))
            return

        self.set_config_value(config_input.config_path, value)
        self.config.save_to_yml(config_files_folder_path / "crawler_config.yaml")
        self.notify(t("config-saved"), severity="information")

    def set_config_value(self, path: list[str | int], value: Any) -> None:
        """Applique une nouvelle valeur dans l’arbre de configuration à l’emplacement indiqué par le chemin.
        Cette méthode navigue dans les attributs et les collections du modèle jusqu’à la feuille ciblée, puis met à jour la clé ou l’index correspondant.

        Args:
            path (list[str | int]): Chemin hiérarchique menant à la valeur de configuration à modifier, composé de noms d’attributs et d’indices de liste.
            value (Any): Nouvelle valeur typée à enregistrer dans la configuration à l’emplacement désigné.
        """
        obj = self.config

        for key in path[:-1]:
            obj = obj[key] if isinstance(key, int) else getattr(obj, key) # type: ignore
        last = path[-1]

        if isinstance(last, int):
            # noinspection PyUnresolvedReferences
            obj[last] = value
        else:
            setattr(obj, last, value)

    def get_type_tree(self, model: type[BaseModel]) -> ConfigsTypesTree:
        """Construit un arbre de types reflétant la structure du modèle de configuration Pydantic.
        Cette méthode déroule récursivement les sous-modèles pour associer à chaque champ soit un type scalaire, soit un sous-arbre correspondant.

        Args:
            model (type[BaseModel]): Classe Pydantic représentant la configuration racine ou une sous-section de configuration.

        Returns:
            ConfigsTypesTree: Arbre de types indiquant pour chaque clé de configuration son type de base ou la structure imbriquée à utiliser.
        """
        result = {}

        for name, field in model.model_fields.items():
            tp = field.annotation
            unwrapped_tp = self._unwrap_union_type(tp)

            if isinstance(unwrapped_tp, type) and issubclass(unwrapped_tp, BaseModel):
                result[name] = self.get_type_tree(unwrapped_tp)
            else:
                result[name] = tp

        return result

    def parse_config(
        self,
        config_dict: dict[str, Any] | list[Any] | None,
        config_type_dict: ConfigsTypesTree,
        path: list[str | int] | None = None,
    ) -> Generator[
        tuple[
            str,
            str,
            ConfigsTypes | type[dict[str, Any]] | type[list[Any]],
            list[str | int],
        ],
        None,
        None,
    ]:  # sourcery skip: low-code-quality
        """Déplie récursivement le dictionnaire de configuration en lignes prêtes à être affichées dans l’éditeur.
        Cette méthode produit pour chaque entrée le libellé, la valeur, le type et le chemin permettant de reconstruire l’interface et de cibler les mises à jour.

        Args:
            config_dict (dict[str, Any] | list[Any] | None): Structure de configuration actuelle, éventuellement imbriquée ou sous forme de liste.
            config_type_dict (ConfigsTypesTree): Arbre de types correspondant à la configuration, utilisé pour distinguer sections, listes et valeurs scalaires.
            path (list[str | int] | None): Chemin accumulé jusqu’à la section ou l’élément courant, utilisé pour suivre la position dans l’arbre.

        Returns:
            Generator[tuple[str, str, ConfigsTypes | type[dict[str, Any]] | type[list[Any]], list[str | int]], None, None]:
                Générateur de tuples décrivant chaque champ ou section à rendre dans l’interface de configuration.
        """
        if path is None:
            path = []

        if config_dict is None:
            return

        if isinstance(config_dict, list):
            leaf_type: ConfigsTypes = type(None)
            unwrapped_type = self._unwrap_union_type(config_type_dict)
            if args := get_args(unwrapped_type):
                leaf_type = cast(ConfigsTypes, args[0])

            for index, value in enumerate(config_dict):
                yield "  - ", str(value), leaf_type, path + [index]
            return

        for key, value in config_dict.items():
            # Résolution propre pour éviter le retour implicite de "None" de dict.get()
            if isinstance(config_type_dict, dict):
                resolved_type = config_type_dict.get(key)
                value_type = resolved_type if resolved_type is not None else type(value)
            else:
                value_type = type(value)

            current_path = path + [key]
            unwrapped_value_type = self._unwrap_union_type(value_type) # type: ignore

            if isinstance(unwrapped_value_type, dict):
                if value is None:
                    yield f"{key}: ", "None", type(None), current_path
                else:
                    yield f"{key}:", "", dict, current_path
                    yield from (
                        (f"  {k}", v, t, p)
                        for k, v, t, p in self.parse_config(
                            value, unwrapped_value_type, current_path
                        )
                    )

            elif (
                get_origin(unwrapped_value_type) is list or unwrapped_value_type is list
            ):
                if value is None:
                    yield f"{key}: ", "None", type(None), current_path
                else:
                    yield f"{key}:", "", list, current_path
                    yield from (
                        (k, v, t, p)
                        for k, v, t, p in self.parse_config(
                            value, unwrapped_value_type, current_path
                        )
                    )

            else:
                yield (
                    f"{key}: ",
                    str(value),
                    cast(ConfigsTypes, value_type),
                    current_path,
                )
                
                
class SecretsConfigsPage(Container):
    """Page Textual dédiée à la consultation et à la modification des secrets stockés dans le fichier .env.
    Cette classe permet d’afficher les valeurs de manière masquée ou visible, de les éditer champ par champ et de les persister en toute sécurité.

    Attributes:
        all_hidden (bool): Indique si tous les champs de secrets doivent être masqués (mode mot de passe) ou affichés en clair.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.all_hidden = True

    def compose(self) -> ComposeResult:
        """Construit l’interface d’édition des secrets à partir du contenu du fichier .env.
        Cette méthode génère une barre d’outils pour afficher ou masquer globalement les valeurs, puis une liste de lignes éditables pour chaque clé non commentée.

        Returns:
            ComposeResult: Un générateur de widgets Textual représentant la barre d’outils et les champs de saisie des secrets.
        """
        if not find_dotenv():
            self.notify(t("secrets-file-not-found"), severity="warning")
            return

        with Horizontal(classes="toolbar"):
            yield Label(t("secrets-title"), classes="toolbar-title")
            yield CallbackButton(
                t("secrets-show-all"),
                callback=self.toggle_all_secrets,
                id="toggle_all_secrets",
            )

        for key, value in dotenv_values(find_dotenv()).items():
            if value is None:
                continue
                
            with Horizontal(classes="secret-row"):
                yield Label(f"{key}=", classes="secret-label")
                yield Input(value=value, id=key, password=True)
                yield CallbackButton(
                    "🙈",
                    callback=lambda key=key: self.toggle_secret(key),
                    classes="secret-toggle",
                )

    def toggle_secret(self, key: str) -> None:
        """Inverse l’affichage masqué ou visible d’un secret individuel dans l’interface.
        Cette méthode bascule le mode mot de passe du champ ciblé et met à jour l’icône du bouton associé pour refléter l’état courant.

        Args:
            key (str): Nom de la variable de secret correspondant au champ à afficher ou masquer.
        """
        input_widget = self.query_one(f"#{key}", Input)
        # noinspection PyInvalidCast
        row = cast(Horizontal, input_widget.parent)
        toggle_button = row.query_one(".secret-toggle", Button)

        input_widget.password = not input_widget.password
        toggle_button.label = "🙈" if input_widget.password else "👁"

    def toggle_all_secrets(self) -> None:
        """Bascule l’affichage masqué ou visible de l’ensemble des secrets de la page.
        Cette méthode met à jour le mode mot de passe de tous les champs, ajuste les icônes individuelles et change le libellé du bouton global en conséquence.

        Returns:
            None: Cette méthode ne renvoie rien mais modifie l’état d’affichage de tous les champs de secrets et de leurs boutons de contrôle.
        """
        self.all_hidden = not self.all_hidden

        for input_widget in self.query(Input):
            input_widget.password = self.all_hidden

        for toggle_button in self.query(".secret-toggle"):
            cast(Button, toggle_button).label = "🙈" if self.all_hidden else "👁"

        self.query_one("#toggle_all_secrets", Button).label = (
            t("secrets-show-all") if self.all_hidden else t("secrets-hide-all")
        )

    def retranslate(self) -> None:
        self.query_one(".toolbar-title", Label).update(t("secrets-title"))
        self.query_one("#toggle_all_secrets", Button).label = (
            t("secrets-show-all") if self.all_hidden else t("secrets-hide-all")
        )

    def on_input_submitted(self, event: Input.Submitted):
        if not find_dotenv():
            self.notify(t("secrets-file-not-found"), severity="warning")
            return

        if not event.value:
            self.notify(t("secrets-value-empty"), severity="error")
            return

        set_key(find_dotenv(), cast(str, event.input.id), event.value)

        self.notify(t("secrets-saved"), severity="information")


class CrawlerTerminalApp(App):
    """Application Textual principale pour le pilotage et la supervision du crawler.
    Cette classe orchestre le démarrage des workers, la navigation entre les pages (dashboard, logs, base de données, configuration, secrets) et la gestion propre de l’arrêt.

    Attributes:
        CSS_PATH (str): Chemin vers la feuille de styles Textual utilisée pour l’apparence de l’application.
        TITLE (str): Titre affiché dans l’interface, traduit dynamiquement selon la langue courante.
        BINDINGS (list[Binding]): Raccourcis clavier déclarés pour l’application, notamment le basculement de langue.
        crawlers (list[dict[Crawler, tuple[threading.Event, threading.Event]]]): Liste des crawlers en cours, associant chaque instance à ses événements de pause et d’arrêt.
        queue_recharger (dict[QueueRecharger, tuple[threading.Event, threading.Event]]): Tâche de rechargement de file d’attente, avec ses événements de contrôle de cycle de vie.
        observer (BaseObserver): Observateur de système de fichiers ou de ressources, suivi pour être correctement arrêté lors de la fermeture.
    """
    CSS_PATH = "crawler_tui.tcss"
    TITLE = t("app-title")

    BINDINGS = [
        Binding("ctrl+l", "toggle_language", t("language-toggle")),
    ]

    crawlers: list[dict[Crawler, tuple[threading.Event, threading.Event]]]
    queue_recharger: dict[QueueRecharger, tuple[threading.Event, threading.Event]]
    observer: BaseObserver


    def __init__(self, cache: Cache | None = None):
        super().__init__()

        self.logs_home_page = LogsHomePage(id="logs_home")
        self.cache = cache

        self.config = CrawlerConfig.load_from_yml(config_files_folder_path / "crawler_config.yaml")

        self.stop_events = []
        self.pause_events = []
        self.crawlers, self.queue_recharger, self.observer = None, None, None # type: ignore
        self.crawled_urls_bf = None
        self.start_time: float | None = None
        
    def start_crawlers(self, args: argparse.Namespace):
        """Initialise et lance les workers du crawler ainsi que les tâches associées.
        Cette méthode prépare les événements de contrôle, démarre les threads via `launch_crawler` et enregistre l’heure de début pour le suivi de session.

        Args:
            args (argparse.Namespace): Arguments de ligne de commande ou paramètres de configuration utilisés pour initialiser le comportement des crawlers.
        """
        self.stop_events = [
            threading.Event() for _ in range(self.config.num_crawlers + 1)
        ]
        self.pause_events = [
            threading.Event() for _ in range(self.config.num_crawlers + 1)
        ]
        self.crawlers, self.queue_recharger, self.observer, self.crawled_urls_bf = launch_crawler(
            args, self.stop_events, self.pause_events, self.cache, return_crawlers=True
        )
        self.start_time = time.time()

    @override
    def compose(self):
        """Construit la mise en page principale de l’application en assemblant les différents onglets et pages.
        Cette méthode initialise le sélecteur de contenu, relie chaque onglet à sa vue correspondante et ajoute en-tête et pied de page pour la navigation générale.

        Returns:
            ComposeResult: Un générateur de widgets Textual représentant l’en-tête, les onglets, le contenu associé et le pied de page de l’application.
        """
        yield Header()

        yield Tabs(
            Tab(t("tabs-dashboard"), id="dashboard"),
            Tab(t("tabs-logs"), id="logs_home"),
            Tab(t("tabs-database-console"), id="database_console"),
            Tab(t("tabs-configs"), id="configs"),
            Tab(t("tabs-secrets-configs"), id="secrets_configs"),
        )

        with ContentSwitcher(initial="dashboard"):
            yield DashboardPage(self.crawlers, self.queue_recharger, self.cache, self.crawled_urls_bf, id="dashboard")
            yield self.logs_home_page
            yield DatabaseConsolePage(id="database_console")
            yield ConfigsPage(self.config, id="configs")
            yield SecretsConfigsPage(id="secrets_configs")

        yield Footer()

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        """Gère le changement d’onglet dans l’interface en activant la vue correspondante.
        Cette méthode synchronise le sélecteur de contenu avec l’onglet choisi et déplace le focus clavier vers la page nouvellement affichée.

        Args:
            event (Tabs.TabActivated): Événement Textual indiquant l’identifiant de l’onglet qui vient d’être activé.
        """
        switcher = self.query_one(ContentSwitcher)
        switcher.current = event.tab.id

        self.query_one(f"#{event.tab.id}").focus()

    def action_toggle_language(self) -> None:
        """Bascule la langue de l’interface utilisateur vers la prochaine locale disponible.
        Cette méthode met à jour les textes affichés en appelant `retranslate` afin de refléter immédiatement le nouveau choix de langue.

        Returns:
            None: Cette méthode ne renvoie rien mais modifie la locale active et rafraîchit les libellés de l’interface.
        """
        set_locale(next_locale())
        self.retranslate()

    def retranslate(self) -> None:
        """Met à jour tous les textes affichés dans la langue courante, sans recomposer
        les pages (ce qui détruirait l'historique des logs et l'état du dashboard)."""
        self.title = t("app-title")

        tab_ids_to_keys = {
            "dashboard": "tabs.dashboard",
            "logs_home": "tabs.logs",
            "database_console": "tabs.database_console",
            "configs": "tabs.configs",
            "secrets_configs": "tabs.secrets_configs",
        }
        for tab in self.query(Tab):
            key = tab_ids_to_keys.get(tab.id) # type: ignore
            if key is not None:
                tab.label = t(key)

        for widget in self.query("*"):
            retranslate = getattr(widget, "retranslate", None)
            if callable(retranslate) and widget is not self:
                retranslate()

    @override
    async def action_quit(self) -> None:
        """Arrête proprement l’application en signalant la fin des crawlers et des tâches associées.
        Cette méthode déclenche les événements d’arrêt, attend la terminaison des threads et de l’observateur, puis délègue la fermeture finale à Textual.

        Returns:
            None: Cette méthode ne renvoie rien mais garantit la libération correcte des ressources et des threads avant la sortie de l’application.
        """
        for stop_event in self.stop_events:
            stop_event.set()
        self.observer.stop()
            
        def join_threads():
            for crawler in self.crawlers:
                list(crawler.keys())[0].join()
            list(self.queue_recharger.keys())[0].join()
            self.observer.join()
        
        await asyncio.to_thread(join_threads)
        
        await super().action_quit()


class TextualSink:
    """Redirige les messages de logs vers l’interface Textual du crawler en appliquant un niveau de filtrage par défaut.
    Cette classe sert de sink pour Loguru et met à jour la page des logs en ajoutant les messages dans un tampon circulaire thread-safe.

    Attributes:
        app (CrawlerTerminalApp): Instance de l’application Textual dans laquelle les logs doivent être affichés.
    """
    def __init__(
        self,
        app: CrawlerTerminalApp,
        default_ui_level: LoggingLevels = LoggingLevels.INFO,
    ):
        self.app = app

        level_map = {
            LoggingLevels.TRACE: 0,
            LoggingLevels.DEBUG: 1,
            LoggingLevels.INFO: 2,
            LoggingLevels.SUCCESS: 3,
            LoggingLevels.WARNING: 4,
            LoggingLevels.ERROR: 5,
            LoggingLevels.CRITICAL: 6,
            LoggingLevels.FATAL: 7,
        }
        self.app.logs_home_page.logs_page.current_level_index = level_map.get(default_ui_level, 2)

    def write(self, message: Message):
        """Ajoute un message de log au tampon d’affichage de l’interface Textual.
        Cette méthode extrait le niveau, le nom de thread et le texte brut du message, puis les enregistre de manière circulaire dans le buffer des logs.

        Args:
            message (Message): Message Loguru à enregistrer, contenant les métadonnées de niveau et de thread ainsi que le texte formaté.
        """
        level_no = message.record["level"].no
        crawler_name = message.record["thread"].name

        # On nettoie le saut de ligne à la fin pour que write_lines() gère le scroll proprement
        raw_msg = str(message).rstrip("\n")

        page = self.app.logs_home_page.logs_page

        with page.buffer_lock:
            page.log_buffer.append((level_no, crawler_name, raw_msg))

            if len(page.log_buffer) > 2000:
                del page.log_buffer[:1000]
