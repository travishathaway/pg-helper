# pg-helper

Small set of commands to help manage a PostgresSQL server.

### Installation

With pip:

```
pip install git+https://github.com/travishathaway/pg-helper.git@main
```

With pixi

```
pixi add pg-helper
```

### Usage

#### CLI

```bash
# Start PostgreSQL (initializes on first run, enables PostGIS automatically)
pg-helper start

# Check status and connection info
pg-helper status

# Open an interactive psql shell
pg-helper shell

# Stop PostgreSQL (data is preserved)
pg-helper stop

# Stop and permanently delete all data (prompts for confirmation)
pg-helper destroy

# Destroy without confirmation prompt
pg-helper destroy --force
```

**Custom port and data directory:**

```bash
# Use a custom port
pg-helper --port 54321 start

# Use a custom data directory
pg-helper start --data-dir /vol/postgres/

# Combine both
pg-helper --port 54321 start --data-dir /vol/postgres/
```

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `PG_HELPER_PORT` | `65432` | PostgreSQL port |
| `PG_HELPER_DATA_DIR` | `.pgdata` (in cwd) | PostgreSQL data directory |

#### Python API

`pg-helper` also exposes a `PostgresCluster` class for use in Python scripts or pytest fixtures:

```python
from pathlib import Path
from pg_helper.postgres import PostgresCluster

# Create a cluster instance
cluster = PostgresCluster(
    data_dir=Path(".pgdata"),
    port=65432,
    user="postgres",
)

# Initialize, start, and create databases (idempotent)
cluster.setup(databases=["mydb"], enable_postgis=True)

# Get connection string
print(cluster.connection_string("mydb"))
# postgresql://postgres@localhost:65432/mydb

# Stop and clean up
cluster.teardown(remove_data=True)
```

**pytest fixture example:**

```python
import pytest
from pathlib import Path
from pg_helper.postgres import PostgresCluster

@pytest.fixture(scope="session")
def postgres():
    cluster = PostgresCluster(Path(".pgdata-test"), port=65433)
    cluster.setup(databases=["testdb"])
    yield cluster
    cluster.teardown(remove_data=True)
```
