# Sandbox Testing with Docker

Use this checklist to spin up a disposable sandbox that runs CastitCrawler alongside a MySQL database. The steps assume Docker and Docker Compose are available on your host.

## 1. Prepare environment
- Clone the repository and change into it.
- Copy `config.yaml` to `config.local.yaml` and update credentials to match the MySQL container values below.

## 2. Start MySQL
- Launch MySQL in Docker:
  ```bash
  docker run -d --name castitcrawler-mysql \
    -e MYSQL_ROOT_PASSWORD=rootpass \
    -e MYSQL_DATABASE=castitcrawler \
    -e MYSQL_USER=castit \
    -e MYSQL_PASSWORD=castitpass \
    -p 3306:3306 mysql:8
  ```
- Wait for MySQL to become healthy:
  ```bash
  docker logs -f castitcrawler-mysql
  ```
- Apply the schema from this repo:
  ```bash
  docker cp castitcrawler_db.schema.sql castitcrawler-mysql:/tmp/schema.sql
  docker exec -i castitcrawler-mysql mysql -ucastit -pcastitpass castitcrawler < castitcrawler_db.schema.sql
  ```

## 3. Build the crawler container
- Create the Python image with required dependencies:
  ```bash
  docker build -t castitcrawler-app -f - . <<'DOCKERFILE'
  FROM python:3.11-slim
  WORKDIR /app
  COPY . /app
  RUN pip install --no-cache-dir requests mysql-connector-python pyyaml beautifulsoup4
  CMD ["bash"]
  DOCKERFILE
  ```
- Start a container connected to MySQL:
  ```bash
  docker run -it --rm --name castitcrawler-app --network host -v "$(pwd)":/app castitcrawler-app bash
  ```

## 4. Run sandbox checks
Inside the `castitcrawler-app` container:
- Verify connectivity to MySQL:
  ```bash
  python - <<'PY'
  import mysql.connector
  conn = mysql.connector.connect(host="127.0.0.1", user="castit", password="castitpass", database="castitcrawler")
  print(conn.is_connected())
  conn.close()
  PY
  ```
- Execute a dry crawl to validate runtime dependencies:
  ```bash
  ./castitcrawler.py --dry-run --threads 4
  ```
- Optionally run a small live crawl and status fetch to exercise inserts:
  ```bash
  ./castitcrawler.py --live-run --fetch-status --threads 2 --limit 20
  ```
- Clean up containers when done:
  ```bash
  docker rm -f castitcrawler-app castitcrawler-mysql
  ```

## Notes
- The `--network host` flag simplifies connectivity for local testing; adjust to a user-defined network if host mode is unavailable.
- Use `config.local.yaml` to avoid committing credentials. Point `config_loader.py` to this file when invoking scripts if you need separate sandbox settings.
