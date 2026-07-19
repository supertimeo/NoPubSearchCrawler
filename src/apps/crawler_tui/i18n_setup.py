from pathlib import Path

from fluent.runtime import FluentLocalization, FluentResourceLoader

LOCALES_DIR = Path(__file__).parent / "locales"
AVAILABLE_LOCALES = ("fr", "en")
DEFAULT_LOCALE = "fr"
RESOURCE_IDS = ["main.ftl"]

_ENV_KEY = "UI_LOCALE"

_loader = FluentResourceLoader(str(LOCALES_DIR / "{locale}"))
_localizations: dict[str, FluentLocalization] = {}
_current_locale = DEFAULT_LOCALE


def init_i18n(locale: str = DEFAULT_LOCALE) -> None:
    """Initialise l'infrastructure de traduction Fluent pour l'interface TUI.
    Cette fonction charge les ressources de toutes les locales disponibles et sélectionne la locale active.

    Args:
        locale (str): La locale à activer au démarrage, ou la locale par défaut si elle n'est pas disponible.
    """
    global _current_locale

    for available_locale in AVAILABLE_LOCALES:
        # Chaque locale a la locale par défaut en repli, pour ne jamais afficher
        # une clé brute si une traduction manque dans une langue non-principale.
        fallback_chain = [available_locale]
        if available_locale != DEFAULT_LOCALE:
            fallback_chain.append(DEFAULT_LOCALE)
        _localizations[available_locale] = FluentLocalization(fallback_chain, RESOURCE_IDS, _loader)

    _current_locale = locale if locale in AVAILABLE_LOCALES else DEFAULT_LOCALE


def t(key: str, **kwargs: object) -> str:
    """Récupère une chaîne traduite pour une clé donnée dans la locale active.
    Cette fonction insère éventuellement des paramètres dans le message localisé.

    Args:
        key (str): La clé de message Fluent à traduire.
        **kwargs (object): Les valeurs de paramètres à injecter dans la chaîne traduite.

    Returns:
        str: La chaîne localisée formatée pour la locale actuellement active.
    """
    return _localizations[_current_locale].format_value(key, kwargs or None)


def get_locale() -> str:
    """Retourne la locale actuellement active pour l'interface TUI.
    Cette fonction permet de connaître la langue utilisée pour la traduction des messages.

    Returns:
        str: Le code de locale actuellement en cours d'utilisation.
    """
    return _current_locale


def set_locale(locale: str) -> None:
    """Change la locale active de l'interface TUI et la rend persistante entre les lancements.
    Cette fonction vérifie que la locale demandée est supportée avant de l'appliquer et de la sauvegarder.

    Args:
        locale (str): Le code de locale à activer, qui doit appartenir aux locales disponibles.
    """
    global _current_locale

    if locale not in AVAILABLE_LOCALES:
        raise ValueError(f"Unknown locale: {locale}")
    _current_locale = locale
    _persist_locale(locale)


def next_locale() -> str:
    """Calcule la prochaine locale disponible dans la liste cyclique des langues supportées.
    Cette fonction permet de faire défiler les locales de l'interface TUI une par une.

    Returns:
        str: Le code de la locale suivante par rapport à la locale actuellement active.
    """
    index = AVAILABLE_LOCALES.index(_current_locale)
    return AVAILABLE_LOCALES[(index + 1) % len(AVAILABLE_LOCALES)]


def load_persisted_locale() -> str:
    """Lit la locale sauvegardée dans le fichier d'environnement de l'application.
    Cette fonction retourne la locale persistée ou la locale par défaut si aucune valeur n'est définie.

    Returns:
        str: Le code de locale trouvé dans la variable d'environnement, ou la locale par défaut en l'absence de valeur.
    """
    import os

    return os.getenv(_ENV_KEY, DEFAULT_LOCALE)


def _persist_locale(locale: str) -> None:
    """Persiste la locale de l'interface TUI dans les variables d'environnement et le fichier .env.
    Cette fonction met à jour ou ajoute la clé de locale afin qu'elle soit réutilisée aux prochains lancements.

    Args:
        locale (str): Le code de locale à enregistrer de manière persistante.
    """
    import os

    os.environ[_ENV_KEY] = locale

    filepath = ".env"
    if not os.path.exists(filepath):
        with open(filepath, "w", encoding="utf-8"):
            pass

    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    updated = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(f"{_ENV_KEY}=") or stripped.startswith(f"{_ENV_KEY} ="):
            lines[i] = f"{_ENV_KEY}={locale}\n"
            updated = True
            break

    if not updated:
        if lines and not lines[-1].endswith("\n"):
            lines.append("\n")
        lines.append(f"{_ENV_KEY}={locale}\n")

    with open(filepath, "w", encoding="utf-8") as f:
        f.writelines(lines)
