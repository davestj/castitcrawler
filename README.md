# CastitCrawler

[![Build Status][build-badge]][build-url]
[![License][license-badge]][license-url]
[![Python Version][python-badge]][python-url]
[![Open Issues][issues-badge]][issues-url]

## Overview

CastitCrawler is a discovery tool that scans the Icecast `yp.xml` directory and collects
information about available streaming stations. It performs connectivity tests,
checks for status pages, and stores valid stations in a MySQL database for
further processing.

### Features

* Parallel processing with configurable worker threads.
* Connection validation via socket and HTTP tests.
* Automatic handling of redirects and SSL certificates.
* Global TLD support and progress tracking.
* Comprehensive statistics and reporting.

## Installation

1. Ensure Python 3.11 or later is installed.
2. Install required dependencies:
   ```bash
   pip install requests mysql-connector-python pyyaml beautifulsoup4
   ```

## Configuration

Create a `config.yaml` file in the project root with database credentials:

```yaml
database:
  user: DBUSERNAME
  password: DBUSERPASS
  host: DBHOST
  database: DBNAME
```

## Usage

### Dry Run
```bash
./castitcrawler.py --dry-run
```

### Full Crawl
```bash
./castitcrawler.py --live-run --fetch-status --threads 20
```

### Background Execution
```bash
nohup ./castitcrawler.py --live-run --fetch-status > ./icecast-crawl-$(date +%Y%m%d).log 2>&1 &
```

## Sandbox testing

See `SANDBOX_TESTING.md` for a step-by-step Docker-based checklist that spins up MySQL, builds a lightweight crawler image, and exercises dry-run and limited live-run paths without touching production resources.

## Data Sanitization & Verification

Use the sanitizer script to clean and verify data prior to database insertion:

```bash
./castitcrawler.sanitize.py
```

`castitcrawler.verified.py` can be used to re-check stored stations.

## Database Schema

A sample schema is provided in `castitcrawler_db.schema.sql` for setting up the
MySQL database.

## Contributing

1. Fork the repository and create your feature branch.
2. Commit your changes with clear messages.
3. Open a pull request describing your work.

## License

This project is licensed under the [MIT License](LICENSE).

[build-badge]: https://img.shields.io/github/actions/workflow/status/OWNER/castitcrawler/ci.yml?branch=main
[build-url]: https://github.com/OWNER/castitcrawler/actions
[license-badge]: https://img.shields.io/badge/license-MIT-green
[license-url]: LICENSE
[python-badge]: https://img.shields.io/badge/python-3.11%2B-blue
[python-url]: https://www.python.org/downloads/
[issues-badge]: https://img.shields.io/github/issues/OWNER/castitcrawler
[issues-url]: https://github.com/OWNER/castitcrawler/issues
