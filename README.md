# DHAP-42 — Local CSV to MinIO Parquet Pipeline

An Airflow-based ingestion project that reuses the email dataset from **DHAP-34** and targets **MinIO object storage** instead of a relational database. The intended pipeline reads a local CSV, enforces a schema contract, transforms the records, and writes partitioned Parquet objects to MinIO.

**Current status: in progress.** Dataset metadata and the schema contract are defined. The local Airflow and MinIO environment, shared network, automatic bucket creation, and Airflow S3 connection are configured. Bucket creation has been confirmed during setup. The ingestion DAG and runtime schema enforcement remain to be implemented.

## Project progress

| Story | Scope | Current state |
| --- | --- | --- |
| 1 — Dataset manifest and schema contract | Describe the dataset and declare acceptable input | Both YAML files are populated; the manifest remains in `draft` status. Runtime enforcement belongs to Story 3. |
| 2 — Dockerized Airflow and MinIO | Start the local services, create the target bucket, and configure access | Compose services, Makefile commands, shared networking, bucket initialization, `.env.example`, and an environment-defined S3 connection are configured. Verification commands are documented below. |
| 3 — CSV to Parquet ingestion | Read, validate, transform, and load through an Airflow DAG | ETL scaffolding exists. No ingestion DAG or completed CSV-to-Parquet run is available yet. |

This project uses **Apache Airflow 3.3.0**, an accepted change from the original task brief's Airflow 2.x prerequisite. The documentation describes the current local development environment.

## Architecture

### Local environment

Airflow and MinIO are maintained in separate Compose files and joined to the same external Docker network, `dhap42`. The Makefile creates this network when needed and starts both Compose projects.

| Component | Purpose |
| --- | --- |
| Airflow API server | Serves the Airflow UI and API on port `8080`. |
| Airflow scheduler and DAG processor | Schedule work and parse DAG definitions. |
| Airflow worker | Executes tasks using `CeleryExecutor`. |
| Airflow triggerer | Supports deferred tasks. |
| PostgreSQL 16 | Stores Airflow metadata and the Celery result backend. |
| Redis 7.2 | Provides the Celery message broker. |
| `airflow-init` | Initializes the Airflow database, local account, and mounted directories. |
| MinIO AIStor | Provides the S3-compatible API on port `9000` and console on port `9001`. |
| `minio_bucket-init` | Waits for MinIO health, creates the configured bucket if needed, and exits. |

The Airflow Compose file also provides optional `debug` and `flower` profiles. They are not started by the default `make up` command.

### Intended ingestion flow — Story 3

```text
dataset/email_thread_details.csv
              |
              v
          Read CSV
              |
              v
     Validate schema contract ---- mismatch ----> Fail the DAG
              |
              v
       Transform records
              |
              v
    Write partitioned Parquet
              |
              v
       MinIO target bucket
```

The partition column and object layout will be finalized during Story 3. No Parquet output is produced by the current infrastructure setup alone.

## Repository layout

```text
DHAP-42/
├── .env.example                 # Shareable environment template
├── .env                         # Local values; ignored by Git
├── .gitignore
├── Makefile                     # Start, stop, validate, and create the network
├── README.md
├── manifest.yaml                # Dataset identity, source, and file references
├── schema_contract.yaml         # Declared input validation rules
├── requirements.txt             # Dependencies installed in the Airflow image
├── dataset/
│   └── email_thread_details.csv  # Dataset reused from DHAP-34
├── etl/
│   ├── extract.py               # Initial file-presence checks
│   ├── validate.py              # Placeholder
│   ├── transform.py             # Placeholder
│   └── load.py                  # Placeholder
├── utils/
│   ├── __init__.py
│   └── logging_config.py        # Initial application logging configuration
└── containers/
    ├── airflow/
    │   ├── Dockerfile
    │   ├── docker-compose.yaml
    │   ├── dags/                # Current host mount for DAG definitions
    │   ├── logs/                # Runtime files
    │   ├── config/              # Runtime files
    │   └── plugins/             # Runtime directory
    └── minio/
        ├── docker-compose.yaml
        ├── minio.license        # Supplied locally; ignored by Git
        └── minio_data/          # Persistent object data; ignored by Git
```

Runtime directories may be created on first startup and may not be present in a fresh checkout. All commands below assume the working directory contains this README and the Makefile. Relative paths in this document are relative to that directory unless stated otherwise.

## Dataset and schema contract

The manifest identifies the dataset as `email_thread_summary_dataset`, version `1`, owned by `mfonekpo`. Its original source is SharePoint; the pipeline's current local input is `dataset/email_thread_details.csv`, with `utf-8-sig` encoding.

The schema contract declares the following input columns:

| Column | Declared type | Nullable |
| --- | --- | --- |
| `thread_id` | `integer` | No |
| `subject` | `string` | Yes |
| `timestamp` | `datetime` | No |
| `from` | `string` | No |
| `to` | `string` | No |
| `body` | `string` | Yes |

`allow_extra_columns: false` declares that unexpected columns must be rejected. Every listed column is intended to be required; `nullable: true` permits missing values within that column, not an absent CSV header. A thread can contain multiple messages, so the contract does not require `thread_id` to be unique.

The YAML files describe the intended behavior; they do not enforce it themselves. Story 3 must implement parsing and validation, including failure on missing, renamed, or incorrectly typed columns before any output is written. Null markers, accepted timestamp formats, timezone handling, and transformation policies still need explicit implementation decisions.

## Prerequisites

- Docker Engine or Docker Desktop with Docker Compose v2.
- GNU Make, used by the commands in this README.
- Network access to obtain container images and Python dependencies during the first build.
- Available host ports `8080`, `9000`, and `9001`.
- Sufficient Docker resources. The included Airflow initializer checks for at least 4 GB memory, 2 CPUs, and 10 GB available disk; allow additional space for images and data.
- A valid MinIO AIStor license, supplied separately at `containers/minio/minio.license`. Refer to the [official AIStor container installation guide](https://docs.min.io/aistor/installation/container/install/) for license and installation details.

The existing setup runs Airflow and its dependencies inside Docker. A separate host Python installation is not required for the commands below.

## Initial setup

### 1. Prepare the local environment file

For a new checkout, copy the template without replacing an existing `.env`:

```bash
cp -n .env.example .env
```

Edit `.env` and fill in the required values. The template intentionally leaves the Fernet key and MinIO password blank.

| Variable | Purpose |
| --- | --- |
| `FERNET_KEY` | Valid Fernet key used by Airflow to encrypt secrets stored in its metadata database. |
| `AIRFLOW_UID` | Linux user ID used by Airflow containers to access mounted files. The container group is configured as `0`. |
| `MINIO_ROOT_USER` | MinIO root username used for this local setup. |
| `MINIO_ROOT_PASSWORD` | MinIO root password; use a nonempty password of at least eight characters. |
| `MINIO_BUCKET` | Target bucket. The example and current setup use `dhap42`. |
| `AWS_ACCESS_KEY_ID` | References `MINIO_ROOT_USER` using the credential name recognized by the S3 client. |
| `AWS_SECRET_ACCESS_KEY` | References `MINIO_ROOT_PASSWORD` using the credential name recognized by the S3 client. |
| `AIRFLOW_CONN_MINIO_S3` | JSON definition of the Airflow connection named `minio_s3`. |

On Linux, obtain the value for `AIRFLOW_UID` with:

```bash
id -u
```

Generate a Fernet key for a new installation without first starting the stack:

```bash
docker run --rm --entrypoint python apache/airflow:3.3.0 \
  -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

Copy the generated value into `FERNET_KEY`, including any trailing `=`. Retain the same key across restarts; an established installation should keep its existing key.

The template derives the S3 credentials from the MinIO variables and defines the connection as follows:

```dotenv
AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER}
AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD}
AIRFLOW_CONN_MINIO_S3='{"conn_type":"aws","extra":{"endpoint_url":"http://minio:9000","region_name":"us-east-1","config_kwargs":{"s3":{"addressing_style":"path"}}}}'
```

Keep these credential references after the `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` definitions. The connection uses the standard AWS environment credential chain, a local endpoint, and path-style bucket addressing.

Commit `.env.example` as the configuration template. The working `.env`, actual credentials, and AIStor license remain local and are excluded by the current `.gitignore`.

### 2. Provide the AIStor license

Place the actual license file at:

```text
containers/minio/minio.license
```

The MinIO Compose service mounts this file read-only at `/minio.license`. Ensure the source is a file before startup.

For a fresh deployment, explicitly select the mounted license by adding the following line to the local `.env`:

```dotenv
MINIO_LICENSE=/minio.license
```

This is an additional setup value not yet included in `.env.example`. The existing `env_file` configuration passes it to the MinIO server. See the [AIStor license-path setting](https://docs.min.io/aistor/reference/aistor-server/settings/core/#minio-aistor-license).

### 3. Validate and start the environment

```bash
make config
make up
```

`make config` validates both Compose configurations with `--quiet`, so it does not print resolved credentials. It does not validate the Fernet key's cryptographic format, start services, or prove that a connection works. Fill all required values before proceeding even if this command reports no errors.

`make up`:

1. Creates the external Docker network `dhap42` if it does not exist.
2. Builds and starts the Airflow Compose project.
3. Starts the MinIO Compose project and its bucket initializer.

The first startup can take several minutes. The command runs services in the background; returning to the shell does not guarantee that all services have finished initializing. Use the checks below to confirm readiness.

## Access and verification

### Service addresses

| Service or client | Address |
| --- | --- |
| Airflow UI from the host | [http://localhost:8080](http://localhost:8080) |
| MinIO console from the host | [http://localhost:9001](http://localhost:9001) |
| MinIO S3 API from the host | `http://localhost:9000` |
| MinIO S3 API from an Airflow container | `http://minio:9000` |

The Airflow initializer defaults to a local UI account with username `airflow` and password `airflow`. Its `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` variables can configure the account during initial setup. Use `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` from `.env` to sign in to the MinIO console.

Inside a container, `localhost` refers to that container. Airflow therefore uses the Docker service name `minio` for S3 requests. Port `9001` is for the console; S3 clients use port `9000`.

### Check container status

```bash
docker compose --env-file .env \
  -f containers/airflow/docker-compose.yaml ps -a

docker compose --env-file .env \
  -f containers/minio/docker-compose.yaml ps -a
```

Long-running services should be running, with healthchecks passing where configured. The initialization services are expected to finish: `airflow-init` and `minio_bucket-init` showing `Exited (0)` indicates successful completion.

### Confirm automatic bucket creation

```bash
docker compose --env-file .env \
  -f containers/minio/docker-compose.yaml \
  logs --tail=50 minio_bucket-init
```

With the default bucket name, the output should include:

```text
Bucket ready: dhap42
```

The initializer waits for MinIO's healthcheck, creates a client alias named `storage`, and runs `mc mb --ignore-existing`. An existing bucket is left in place, so initialization can be repeated without recreating or clearing it. The initializer joins the same default network as MinIO automatically.

Open the MinIO console and confirm that the configured bucket is visible. Its initial state is empty unless objects have been added separately.

### Verify Airflow can access the bucket

Run a read-only check from the Airflow worker using the named connection:

```bash
docker compose --env-file .env \
  -f containers/airflow/docker-compose.yaml \
  exec -T airflow-worker python - <<'PY'
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if not os.environ.get("AIRFLOW_CONN_MINIO_S3"):
    raise RuntimeError("AIRFLOW_CONN_MINIO_S3 is missing from the worker environment")

client = S3Hook(aws_conn_id="minio_s3").get_conn()
bucket = os.environ["MINIO_BUCKET"]
client.head_bucket(Bucket=bucket)
print(f"Airflow can access bucket: {bucket}")
PY
```

Expected final output for the default configuration:

```text
Airflow can access bucket: dhap42
```

This checks authenticated access to the bucket. It does not upload an object or demonstrate a completed pipeline run.

Environment-defined Airflow connections are resolved at runtime and do not appear in the Connections UI or `airflow connections list`. Also, the generic AWS connection test uses AWS STS and can fail against an S3-compatible service such as MinIO; use the bucket check above for this setup.

## Daily operation

| Command | Behavior |
| --- | --- |
| `make config` | Validate both Compose configurations without displaying their resolved contents. |
| `make network` | Ensure the shared `dhap42` network exists. |
| `make up` | Build as needed and start or update both Compose projects. |
| `make down` | Stop and remove the projects' containers while retaining their persistent data. |

After editing `.env` or dependencies, run `make config` and `make up` again. Compose recreates containers when their resolved configuration changes; a plain container restart does not reload changed environment variables.

View recent logs when troubleshooting:

```bash
docker compose --env-file .env \
  -f containers/airflow/docker-compose.yaml \
  logs --tail=100 airflow-worker airflow-scheduler

docker compose --env-file .env \
  -f containers/minio/docker-compose.yaml \
  logs --tail=100 minio minio_bucket-init
```

### Persistent storage and mounted files

| Host storage | Container destination or use |
| --- | --- |
| PostgreSQL named volume `postgres-db-volume` | Airflow metadata under `/var/lib/postgresql/data`; Docker prefixes the actual volume name with the Compose project name. |
| `containers/minio/minio_data/` | MinIO object data mounted at `/data`. |
| `containers/minio/minio.license` | AIStor license mounted read-only at `/minio.license`. |
| `containers/airflow/dags/` | DAG definitions mounted at `/opt/airflow/dags`. |
| `containers/airflow/logs/` | Airflow logs mounted at `/opt/airflow/logs`. |
| `containers/airflow/config/` | Airflow configuration mounted at `/opt/airflow/config`. |
| `containers/airflow/plugins/` | Plugins mounted at `/opt/airflow/plugins`. |

`make down` does not request volume deletion. It retains the PostgreSQL volume and MinIO's host data directory. The external `dhap42` network is also retained.

The current Airflow Dockerfile copies the dependency file and installs packages while retaining the base image's Airflow version. It does not copy the root `dataset/`, `etl/`, or `utils/` directories or the YAML files into the image. Making these inputs and modules available to the future DAG is part of Story 3.

The application logging scaffold in `utils/logging_config.py` configures console output, `logs/pipeline.log` for INFO and higher, and `logs/monitoring.log` for ERROR and higher. These paths are relative to the process working directory; running from `/opt/airflow` would place them inside the Airflow log mount. The logger creates its files on import; it is not yet integrated into an ingestion DAG.

## Configuration notes and troubleshooting

| Symptom | Check or explanation |
| --- | --- |
| Compose reports an unset variable | Run commands from the project root and use `--env-file .env`, as the Makefile does. Service-level `env_file` populates container variables; Compose interpolation is a separate step. |
| Fernet or container-user errors | Keep `AIRFLOW__CORE__FERNET_KEY: ${FERNET_KEY}` and `user: "${AIRFLOW_UID:-50000}:0"`. Bare names such as `FERNET_KEY` or `AIRFLOW_UID` are literal strings, not variable lookups. |
| The shared network is missing | Run `make network` or `make up`. Both Compose projects declare `dhap42` as external. |
| Airflow cannot resolve or reach MinIO | Confirm both projects use `dhap42`, MinIO is running, and the connection endpoint is `http://minio:9000`. |
| Bucket initialization fails | Inspect `minio` and `minio_bucket-init` logs; check server health, the license file, credentials, and `MINIO_BUCKET`. |
| S3 returns access denied | Check that the derived AWS credential variables match the MinIO credentials loaded by the running server, and that the requested bucket exists. |
| The connection is absent from the Airflow UI | This is expected for a connection supplied through `AIRFLOW_CONN_MINIO_S3`. Use the worker verification command. |
| Changed `.env` values are not taking effect | Run `make up` to apply the changed configuration; restarting an existing container alone retains its original environment. |
| No ingestion DAG appears | The ingestion DAG has not been written yet. The current DAG mount is `containers/airflow/dags/`. |

Relative bind paths are resolved from each Compose file's directory. The Makefile intentionally invokes the two files separately; combining them with multiple `-f` arguments requires reviewing relative paths first, particularly MinIO's data and license mounts.

The current server image uses `quay.io/minio/aistor/minio:latest`; the bucket initializer pins its AIStor client release. The Amazon provider is declared without a version pin in `requirements.txt`. The configuration supports repeatable setup, but it does not yet lock every image and dependency to an immutable version.

## Remaining implementation — Story 3

- Make the CSV, manifest, schema contract, and ETL modules accessible inside the Airflow containers.
- Implement CSV reading using the manifest's path and encoding. The current extraction scaffold only performs working-directory-dependent file checks.
- Implement strict schema validation with clear failure messages for missing, renamed, unexpected, or incorrectly typed columns and invalid required values.
- Define normalization, null handling, timestamp handling, and any duplicate policy.
- Choose a sensible partition column and implement Parquet writing. Declare the required YAML and Parquet libraries explicitly; `PyYAML` and `pyarrow` are not currently listed in `requirements.txt`.
- Upload Parquet objects to the configured MinIO bucket through `minio_s3`.
- Create the Airflow DAG with `catchup=False` and the sequence `read_csv -> validate_contract -> transform -> write_parquet_to_minio`, without heavy I/O at module import time.
- Verify a conforming CSV succeeds and invalid inputs fail before any output is written.
- Confirm the resulting partitioned Parquet objects in the MinIO console and update this README with the DAG ID, partition layout, run procedure, and validation results.

## References

- [Docker Compose environment-variable interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/)
- [Docker Compose networking](https://docs.docker.com/compose/how-tos/networking/)
- [Docker Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/)
- [Airflow connection management](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Airflow Amazon provider connection configuration](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html)
- [Extending the Airflow Docker image](https://airflow.apache.org/docs/docker-stack/build.html)
- [MinIO AIStor container installation](https://docs.min.io/aistor/installation/container/install/)
- [MinIO bucket creation with `mc mb`](https://docs.min.io/aistor/reference/cli/mc-mb/)
