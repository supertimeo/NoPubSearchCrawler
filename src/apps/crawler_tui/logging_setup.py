import argparse
import shutil
from typing import TYPE_CHECKING

from loguru import logger

from src.common.paths import log_folder_path
from src.crawler.log_levels import LoggingLevels
from .tui import CrawlerTerminalApp, TextualSink

if TYPE_CHECKING:
    from loguru import Record


def log_patcher(record: "Record"):
    """Enrichit un enregistrement de log avec des informations de localisation et de thread.
    Cette fonction prépare les champs supplémentaires utilisés par le formatteur de logs.

    Args:
        record (Record): L'enregistrement de log à modifier, contenant les informations
            de contexte et les champs extra.
    """
    class_name = record["extra"].get("class_name")
    record["extra"]["location"] = f"{record['file'].name}{f":{class_name}" if class_name is not None else ""}{f":{record['function']}" if record['function'] != "<module>" else ""}:{record['line']}"
    record["extra"]["thread_info"] = f"{record["thread"].name} ({record["thread"].id})"


def log_format(_record: "Record") -> str:
    """Construit une chaîne de formatage pour les messages de log enrichis.
    Cette fonction définit la présentation des informations de temps, niveau, localisation, thread et message.

    Args:
        _record (Record): L'enregistrement de log à formatter, utilisé pour alimenter les champs du gabarit.

    Returns:
        str: Le gabarit de formatage à utiliser par Loguru pour rendre les messages de log.
    """
    return (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
        "<level>{level: <8}</level> | "
        "{extra[location]: <50} | "
        "{extra[thread_info]: <20} - "
        "{message}\n"
        "{exception}"
    )


def init_logger(args: argparse.Namespace, textual_app: CrawlerTerminalApp):
    """Initialise et configure le système de journalisation de l'application crawler TUI.
    Cette fonction prépare les fichiers de logs, les niveaux personnalisés et le sink Textual pour l'affichage dans l'interface.

    Args:
        args (argparse.Namespace): Les arguments de ligne de commande indiquant notamment les options de suppression des logs.
        textual_app (CrawlerTerminalApp): L'instance de l'application Textual utilisée comme sink pour l'affichage des logs.
    """
    if args.delete_logs or args.delete_all and log_folder_path.exists():
        shutil.rmtree(log_folder_path)
        log_folder_path.mkdir()

    # création du logger
    logger.remove()

    logger.level(
        LoggingLevels.FATAL,
        no=60,
        color="<white><bold><bg red>",
        icon="☠️",
    )

    logger.configure(patcher=log_patcher)

    logger.add(log_folder_path / "latest" / "latest.log", rotation="1 MB", retention="7 days", enqueue=True, compression="zip", level=LoggingLevels.INFO,
               format=log_format)
    logger.add(log_folder_path / "error" / "error.log", rotation="200 MB", retention="7 days", enqueue=True, compression="zip", level=LoggingLevels.ERROR,
               format=log_format, backtrace=True, diagnose=True)
    logger.add(log_folder_path / "trace" / "trace.log", rotation="10 GB", retention="7 days", enqueue=True, compression="zip", level=LoggingLevels.TRACE,
               format=log_format)
    logger.add(
        # C'est ICI qu'on définit ce qui sera affiché par défaut dans l'interface (grâce à ton StrEnum)
        TextualSink(textual_app, default_ui_level=LoggingLevels.INFO),
        enqueue=True,
        # Loguru doit envoyer TOUS les logs (jusqu'à TRACE) au Sink Textual
        # pour qu'ils soient gardés en mémoire.
        level=LoggingLevels.TRACE,
        format=log_format,
        # colorize=False : le widget Log de Textual ne comprend pas les
        # séquences ANSI, il les affiche comme du texte brut (d'où le "[0m"
        # visible). Pire, l'octet ESC embarqué dans le message est transmis
        # tel quel au vrai terminal en plus des codes générés par Textual,
        # ce qui peut corrompre l'affichage des lignes suivantes (résidus,
        # chevauchement) sans que ça se voie dans un test headless.
        colorize=False,
    )
    logger.info("Logger initialized")
