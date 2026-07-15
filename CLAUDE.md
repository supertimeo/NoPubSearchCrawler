# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**NoPubSearch** is a multi-threaded web crawler for indexing URLs and extracting page data. The application uses PostgreSQL for persistence, Pydantic for configuration, and SQLAlchemy for ORM.

## Development Commands

All commands use `uv` (Python package manager).

```bash
# Install dependencies
uv sync

# Lint and format code
uv run ruff check . --fix          # Fix linting issues
uv run ruff format .               # Format code

# Run the application
uv run python -m src.crawlers.crawlers

# Run tests (test suite exists but is empty)
uv run pytest

# Check for unused dependencies
uv run deptry check --fail-exit-code 0
```

## Architecture

### Core Components

The crawler is built around **multi-threaded URL crawling** with database persistence and intelligent URL queueing.

```
Entry Point: src/crawlers/crawlers.py::main()
    ├─ init()                          # Setup: DB engine, cache, bloom filter, queue
    ├─ Crawler threads                 # N worker threads (config: num_crawlers)
    │   └─ Fetches URLs → HTML parsing → Extract links
    ├─ QueueRecharger thread           # Maintains URL queue from database
    └─ ConfigFileEventHandler          # Hot-reload of crawler_config.yaml
```

### Key Classes

**`Crawler` (threading.Thread)**
- Worker thread that processes URLs from a priority queue
- Fetches HTML, extracts links and page content
- Respects `robots.txt` via RobotsTxtManager
- Tracks crawled URLs using a Bloom filter (memory-efficient, false-positive rate only)
- Stores results in database models: `Page`, `Link`, `URL`, `WaitingURL`, `CrawledURL`

**`QueueRecharger` (threading.Thread)**
- Keeps the priority queue populated from `WaitingURL` database table
- Implements per-domain crawl delays (`domain_crawl_time`)
- Respects `max_waiting_delay` from config before re-queueing

**`NetworkManager`**
- Handles all HTTP requests with retry logic for transient errors
- DNS resolution caching via `@lru_cache` on `is_resolvable()`
- Distinguishes retryable errors (timeout, 429, 5xx) from fatal ones (404, 403)

**`RobotsTxtManager`**
- Caches `robots.txt` parsing results per domain
- Enforces crawl delays specified in `robots.txt`

**`CrawlerConfig` → `CrawlerNetworkConfig`**
- Pydantic-based configuration loaded from `configs/crawler_config.yaml`
- Validators ensure `default_waiting_delay ≤ max_waiting_delay`, `min_queue_size ≤ max_queue_size`
- Live reload: Config file changes trigger crawler reconfiguration

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

### URL Processing Pipeline

1. **Initialization**: Load `WaitingURL` entries into priority queue, sorted by `domain_crawled_at`
2. **Crawler Worker**:
   - Pop URL from queue → check Bloom filter (skip if already crawled)
   - Fetch page via NetworkManager → parse HTML with selectolax
   - Extract `<title>`, text content (via `sanitize()`)
   - Extract all `<a href>` links, normalize URLs (`urljoin`, remove tracking params)
   - Store in `Page`, `Link`, `URL` tables
   - Mark as crawled in Bloom filter + `CrawledURL` table
3. **Queue Recharger**: Periodically refill queue from `WaitingURL`

### Error Handling

Custom exception hierarchy in `src/crawlers/errors.py`:

- **`CrawlError`** — Base for crawler-specific failures
  - `RobotsError` — robots.txt violations
  - `NetworkError` — HTTP issues (has `retryable` flag for transient errors)
- **`DatabaseError`** — ORM/persistence failures
- **`InitializationError`** → `ConfigurationError`, `MissingEnvironmentVariableError`

The main loop uses `@logger.catch()` with fatal-level handlers to exit on initialization failures.

## Configuration

### File: `configs/crawler_config.yaml`

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

File changes are monitored; crawlers are reconfigured on save.

### Environment

The `.env` file is loaded via `python-dotenv`:
- Database URL (if not provided, defaults are used)
- Any domain-specific crawl settings

## Common Tasks

### Increase Crawl Rate
Reduce `num_crawlers` parallelism or `default_waiting_delay` in the config.

### Reduce Database Pressure
Increase `max_queue_size` (hold more URLs in memory) or `max_waiting_delay` (spread domain crawls further apart).

### Add New Link Extraction Logic
Edit the `extract_links()` method in the `Crawler` class (around line 500 in `crawlers.py`). The method already normalizes URLs and filters tracking parameters.

### Debug Specific URLs
Check logs in `logs/` (structured via loguru). Add the URL to `WaitingURL` table directly and observe the Crawler threads.

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
- **Dev**: `ruff`, `pytest`, `deptry`

## Code Style

- Python 3.14+ (PEP 673 `Self` type, PEP 695 type hints)
- Linting: Ruff (no manual configuration needed beyond defaults)
- Naming: Snake case for functions/variables, PascalCase for classes, UPPER_CASE for module-level constants

## Known Patterns

- **Bloom filter for URL dedup**: `crawled_urls_bf` prevents redundant fetches without storing full URLs in memory
- **Per-domain rate limiting**: `domain_crawl_time` dict + lock prevents hammering single domains
- **Priority queue**: `queue.PriorityQueue` orders by `domain_crawled_at` (earliest first)
- **Thread-safe database**: `scoped_session` from SQLAlchemy ensures each thread has its own session
- **Graceful shutdown**: `stop_event` threading.Event is set on KeyboardInterrupt; threads poll it in loops
