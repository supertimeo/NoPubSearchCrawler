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
    return _localizations[_current_locale].format_value(key, kwargs or None)


def get_locale() -> str:
    return _current_locale


def set_locale(locale: str) -> None:
    global _current_locale

    if locale not in AVAILABLE_LOCALES:
        raise ValueError(f"Unknown locale: {locale}")
    _current_locale = locale
    _persist_locale(locale)


def next_locale() -> str:
    index = AVAILABLE_LOCALES.index(_current_locale)
    return AVAILABLE_LOCALES[(index + 1) % len(AVAILABLE_LOCALES)]


def load_persisted_locale() -> str:
    """Lit la locale sauvegardée dans le .env, ou la locale par défaut si absente."""
    import os

    return os.getenv(_ENV_KEY, DEFAULT_LOCALE)


def _persist_locale(locale: str) -> None:
    """Sauvegarde la locale choisie dans le .env pour la retrouver au prochain lancement."""
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
