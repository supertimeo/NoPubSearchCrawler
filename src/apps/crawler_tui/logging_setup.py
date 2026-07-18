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
    class_name = record["extra"].get("class_name")
    record["extra"]["location"] = f"{record['file'].name}{f":{class_name}" if class_name is not None else ""}{f":{record['function']}" if record['function'] != "<module>" else ""}:{record['line']}"
    record["extra"]["thread_info"] = f"{record["thread"].name} ({record["thread"].id})"


def log_format(_record: "Record") -> str:
    return (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
        "<level>{level: <8}</level> | "
        "{extra[location]: <50} | "
        "{extra[thread_info]: <20} - "
        "{message}\n"
        "{exception}"
    )


def init_logger(args: argparse.Namespace, textual_app: CrawlerTerminalApp):
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

    logger.add(log_folder_path / "latest" / "latest.log", rotation="1 MB", enqueue=True, compression="zip", level=LoggingLevels.INFO,
               format=log_format)
    logger.add(log_folder_path / "error" / "error.log", rotation="1 MB", enqueue=True, compression="zip", level=LoggingLevels.ERROR,
               format=log_format, backtrace=True, diagnose=True)
    logger.add(log_folder_path / "trace" / "trace.log", rotation="1 MB", enqueue=True, compression="zip", level=LoggingLevels.TRACE,
               format=log_format, backtrace=True, diagnose=True)
    logger.add(
        # C'est ICI qu'on définit ce qui sera affiché par défaut dans l'interface (grâce à ton StrEnum)
        TextualSink(textual_app, default_ui_level=LoggingLevels.INFO),
        enqueue=True,
        # Loguru doit envoyer TOUS les logs (jusqu'à TRACE) au Sink Textual
        # pour qu'ils soient gardés en mémoire.
        level=LoggingLevels.TRACE,
        format=log_format,
        backtrace=True,
        diagnose=True,
        # colorize=False : le widget Log de Textual ne comprend pas les
        # séquences ANSI, il les affiche comme du texte brut (d'où le "[0m"
        # visible). Pire, l'octet ESC embarqué dans le message est transmis
        # tel quel au vrai terminal en plus des codes générés par Textual,
        # ce qui peut corrompre l'affichage des lignes suivantes (résidus,
        # chevauchement) sans que ça se voie dans un test headless.
        colorize=False,
    )
    logger.info("Logger initialized")
