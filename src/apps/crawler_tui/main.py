import argparse

from dotenv import load_dotenv

from src.crawler.bootstrap import init_sentry, init_cache, validate_environment
from .i18n_setup import init_i18n, load_persisted_locale

load_dotenv()
init_i18n(load_persisted_locale())

from .logging_setup import init_logger  # noqa: E402  (doit suivre init_i18n, voir ci-dessus)
from .tui import CrawlerTerminalApp  # noqa: E402


def parsing_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande pour configurer les options de nettoyage.
    Cette fonction prépare l'espace de noms contenant les indicateurs activés par l'utilisateur.

    Returns:
        argparse.Namespace: L'espace de noms issu de l'analyse de la ligne de commande,
            incluant les options de suppression des journaux, du cache, de la base de
            données ou de l'ensemble des données.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l", "--delete-logs", action="store_true", help="Delete the logs"
    )
    parser.add_argument(
        "-c", "--delete-cache", action="store_true", help="Delete the cache"
    )
    parser.add_argument(
        "-d", "--delete-db", action="store_true", help="Delete the database"
    )
    parser.add_argument(
        "-a",
        "--delete-all",
        action="store_true",
        help="Delete the logs, the database and the cache",
    )
    return parser.parse_args()


def main():
    """Lance l'application TUI de crawler avec la configuration courante.
    Cette fonction orchestre l'initialisation de l'environnement, du cache, du logger et des crawlers.

    Returns:
        None: Cette fonction ne renvoie rien, elle exécute l'application TUI jusqu'à sa fermeture.
    """
    args = parsing_arguments()

    validate_environment()

    init_sentry()

    cache = init_cache(args)
    textual_app = CrawlerTerminalApp(cache)
    init_logger(args, textual_app)
    textual_app.start_crawlers(args)
    textual_app.run()


if __name__ == "__main__":
    main()
