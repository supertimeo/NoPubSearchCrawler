# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**NoPubSearch** is aimed at becoming a full search engine (crawling, indexing, querying/ranking). Only the **crawler** domain is implemented so far: a multi-threaded web crawler for indexing URLs and extracting page data. The application uses PostgreSQL for persistence, Pydantic for configuration, and SQLAlchemy for ORM.

## Development Commands

All commands use `uv` (Python package manager).

```bash
# Install dependencies
uv sync

# Lint and format code
uv run ruff check . --fix          # Fix linting issues
uv run ruff format .               # Format code

# Run the crawler app (TUI)
uv run python -m src.apps.crawler_tui.main

# Run tests (test suite exists but is empty)
uv run pytest

# Check for unused dependencies
uv run deptry check --fail-exit-code 0
```

## Architecture

The codebase is organized in three layers, with dependencies flowing one way: **apps → domains → common/database/configs**. A domain must never import another domain directly; if two domains need to share data, they go through `src/database/model.py`.

```
src/
  common/            # shared kernel: no dependency on any domain
    paths.py         # root_folder_path, log_folder_path, cache_folder_path, backup_folder_path, assets_folder_path
    errors.py        # DatabaseError, InitializationError, ConfigurationError, MissingEnvironmentVariableError
    base_config.py   # BaseConfig (Pydantic, loads from YAML)

  configs/           # one Pydantic schema per module
    crawler_config.py  # CrawlerConfig, CrawlerNetworkConfig

  database/
    model.py         # SQLAlchemy ORM models (URL, Page, Link, WaitingURL, CrawledURL)
    session.py        # build_db_url, create_db_engine, create_session_factory, init_schema

  crawler/           # domain: fetching & storing raw pages
    engine.py         # Crawler, QueueRecharger, ConfigFileEventHandler, CrawlResult, FixedList
    managers.py        # NetworkManager, RobotsTxtManager
    bootstrap.py       # validate_environment, build_dependencies, launch_crawler
    errors.py           # CrawlError, RobotsError, NetworkError
    log_levels.py       # LoggingLevels StrEnum

  apps/
    crawler_tui/       # interface that wires the crawler domain to a Textual UI
      main.py           # entry point: parses CLI args, wires everything, runs the app
      tui.py             # Textual widgets (DashboardPage, LogsPage, CrawlerTerminalApp, TextualSink)
      logging_setup.py   # loguru configuration (format, sinks, patcher)
```

Future domains (`indexer/`, `search/`) and interfaces (e.g. a search API under `apps/`) should follow the same pattern: a domain package with its own `errors.py`/`bootstrap.py`, a config schema in `src/configs/`, and any new shared exception/path/helper promoted to `src/common/` instead of being duplicated.

### Core Components

```
Entry Point: src/apps/crawler_tui/main.py::main()
    ├─ validate_environment()                # fail fast on missing/invalid DB env vars
    ├─ init_cache()                           # diskcache for robots.txt
    ├─ CrawlerTerminalApp (Textual)           # starts launch_crawler() in a background thread
    │   └─ launch_crawler() (src/crawler/bootstrap.py)
    │        ├─ build_dependencies()          # DB engine/session, bloom filter, queue
    │        ├─ Crawler threads               # N worker threads (config: num_crawlers)
    │        │   └─ Fetches URLs → HTML parsing → Extract links
    │        ├─ QueueRecharger thread         # maintains URL queue from database
    │        └─ ConfigFileEventHandler        # hot-reload of crawler_config.yaml
    └─ init_logger()                          # wires loguru to files + the TUI log panel
```

### Key Classes

**`Crawler` (threading.Thread)** — `src/crawler/engine.py`
- Worker thread that processes URLs from a priority queue
- Fetches HTML, extracts links and page content (see `crawl()`)
- Respects `robots.txt` via `RobotsTxtManager`
- Tracks crawled URLs using a Bloom filter (memory-efficient, false-positive rate only)
- Stores results in database models: `Page`, `Link`, `URL`, `WaitingURL`, `CrawledURL`

**`QueueRecharger` (threading.Thread)** — `src/crawler/engine.py`
- Keeps the priority queue populated from `WaitingURL` database table
- Implements per-domain crawl delays (`domain_crawl_time`)
- Respects `max_waiting_delay` from config before re-queueing

**`NetworkManager`** — `src/crawler/managers.py`
- Handles all HTTP requests with retry logic for transient errors
- DNS resolution caching via `@lru_cache` on `is_resolvable()`
- Distinguishes retryable errors (timeout, 429, 5xx) from fatal ones (404, 403)

**`RobotsTxtManager`** — `src/crawler/managers.py`
- Caches `robots.txt` parsing results per domain
- Enforces crawl delays specified in `robots.txt`

**`CrawlerConfig` → `CrawlerNetworkConfig`** — `src/configs/crawler_config.py`
- Pydantic-based configuration loaded from `config_files/crawler_config.yaml`
- Validators ensure `default_waiting_delay ≤ max_waiting_delay`, `min_queue_size ≤ max_queue_size`
- Live reload: config file changes trigger crawler reconfiguration

### Database Schema

All tables use SQLAlchemy ORM (see `src/database/model.py`):

- **`urls`** — All discovered URLs (unique constraint on `url` field)
- **`pages`** — Crawled pages with extracted title and content
  - 1:1 relationship with `urls` (one page per URL)
  - 1:N relationship with `links` (outbound links from this page)
- **`links`** — Edge table: references from one page to another URL
  - Unique constraint `(page_id, url_id)` prevents duplicate edges
- **`waiting_list`** — URLs queued for crawling
  - Indexed on `domain_crawled_at` for priority ordering
- **`crawled_urls`** — Bloom filter fallback: URLs we've already processed

`src/database/session.py` owns engine/session creation (`build_db_url`, `create_db_engine`, `create_session_factory`, `init_schema`) — any future domain that needs DB access should reuse it rather than building its own connection.

### URL Processing Pipeline

1. **Initialization**: Load `WaitingURL` entries into priority queue, sorted by `domain_crawled_at`
2. **Crawler Worker** (`Crawler.run()` / `Crawler.crawl()` in `engine.py`):
   - Pop URL from queue → check Bloom filter (skip if already crawled)
   - Fetch page via `NetworkManager` → parse HTML with selectolax
   - Extract `<title>`, text content (via `extract_main_content()`)
   - Extract all `<a href>` links, normalize URLs (`get_pure_url()`: `urljoin`, remove tracking params)
   - Store in `Page`, `Link`, `URL` tables
   - Mark as crawled in Bloom filter + `CrawledURL` table
3. **Queue Recharger**: Periodically refill queue from `WaitingURL`

### Error Handling

Errors are split between the shared kernel and the crawler domain:

- **`src/common/errors.py`** (generic, usable by any future domain)
  - `DatabaseError` — ORM/persistence failures
  - `InitializationError` → `ConfigurationError` → `MissingEnvironmentVariableError`
- **`src/crawler/errors.py`** (crawler-specific)
  - `CrawlError` — base for crawler-specific failures
    - `RobotsError` — robots.txt violations
    - `NetworkError` — HTTP issues (has `retryable` flag for transient errors)

`src/crawler/bootstrap.py::launch_crawler` uses `@logger.catch()` with fatal-level handlers to exit on initialization failures.

## Configuration

### File: `config_files/crawler_config.yaml`

```yaml
num_crawlers: 15                    # Number of worker threads
min_queue_size: 200                 # Trigger recharge if queue drops below this
max_queue_size: 1000                # Stop recharging if queue exceeds this

network:
  timeout: 5                        # HTTP request timeout (seconds)
  allow_redirects: false            # Don't follow 3xx redirects
  max_waiting_delay: 60             # Max per-domain delay before re-crawl
  default_waiting_delay: 5          # Default per-domain delay
```

File changes are monitored; crawlers are reconfigured on save. The path is defined in `src/common/paths.py::crawler_config_file_path`. A future `indexer`/`search` domain should get its own `config_files/<domain>_config.yaml` + `src/configs/<domain>_config.py` schema, following the same pattern.

### Environment

The `.env` file is loaded via `python-dotenv`:
- Database URL (if not provided, defaults are used) — validated at startup by `src/crawler/bootstrap.py::validate_environment()` (`DB_USERNAME`, `DB_PASSWORD`, `DB_NAME`, `DB_HOST`, `DB_PORT`)
- Any domain-specific crawl settings

## Common Tasks

### Increase Crawl Rate
Reduce `num_crawlers` parallelism or `default_waiting_delay` in the config.

### Reduce Database Pressure
Increase `max_queue_size` (hold more URLs in memory) or `max_waiting_delay` (spread domain crawls further apart).

### Add New Link Extraction Logic
Edit the `crawl()` method (link extraction) or `get_pure_url()` (URL normalization) in the `Crawler` class in `src/crawler/engine.py`.

### Debug Specific URLs
Check logs in `logs/` (structured via loguru, configured in `src/apps/crawler_tui/logging_setup.py`). Add the URL to `WaitingURL` table directly and observe the Crawler threads.

## Testing

No test suite exists yet. Tests should cover:
- Config validation (validators in `CrawlerConfig`)
- `NetworkManager.fetch_page()` with mocked responses
- URL normalization and link extraction
- `QueueRecharger` timing logic

Use `pytest` with fixtures for database (in-memory SQLite for unit tests, real PostgreSQL for integration tests).

## Dependencies

- **Database**: `sqlalchemy`, `psycopg[binary]` (PostgreSQL dialect)
- **Config**: `pydantic`, `pyyaml`
- **Crawling**: `requests`, `selectolax` (HTML parsing), `protego` (robots.txt parser)
- **Caching**: `diskcache`, `rbloom` (Bloom filter)
- **Observability**: `loguru` (structured logging)
- **Monitoring**: `watchdog` (file system events for config reload)
- **UI**: `textual` (crawler TUI)
- **Dev**: `ruff`, `pytest`, `deptry`

## Code Style

- Python 3.14+ (PEP 673 `Self` type, PEP 695 type hints)
- Linting: Ruff (no manual configuration needed beyond defaults)
- Naming: Snake case for functions/variables, PascalCase for classes, UPPER_CASE for module-level constants

## Known Patterns

- **Layered architecture**: `apps → domains → common/database/configs`, one-directional. Domains never import each other.
- **Bloom filter for URL dedup**: `crawled_urls_bf` prevents redundant fetches without storing full URLs in memory
- **Per-domain rate limiting**: `domain_crawl_time` dict + lock prevents hammering single domains
- **Priority queue**: `queue.PriorityQueue` orders by `domain_crawled_at` (earliest first)
- **Thread-safe database**: `scoped_session` from SQLAlchemy ensures each thread has its own session
- **Graceful shutdown**: `stop_event` threading.Event is set on KeyboardInterrupt; threads poll it in loops
