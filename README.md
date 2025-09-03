# pgoutput-reader

This is a single file version of [dgea005/pypgoutput: PostgreSQL CDC library using pgoutput and python](https://github.com/dgea005/pypgoutput), with better cli usage support.

Single Python file to read, parse and convert PostgreSQL logical decoding messages to change data capture messages. Built using psycopg2's logical replication support objects, PostgreSQL's pgoutput plugin.

Uses python >= 3.7

Developed on PostgreSQL 12 for now.

**Warning**: this is a prototype and not production tested

## Installation

Just copy file `pgoutputreader.py`, and make sure python venv have [psycopg2](https://www.psycopg.org/docs/) installed.

## How it works

See https://github.com/dgea005/pypgoutput for details.


## Example

First, setup a publication and a logical replication slot in the source database.

```sql
CREATE PUBLICATION test_pub FOR ALL TABLES;
SELECT * FROM pg_create_logical_replication_slot('test_slot', 'pgoutput');
```

Second, run the script to collect the changes:

```shell
python pgoutputreader.py --host $PGHOST --port $PGPORT --database $PGDATABASE --user $PGUSER --password $PGPASSWORD --slot test_slot
```

Generate some change messages

```sql
CREATE TABLE public.readme (id integer primary key, created_at timestamptz default now());

INSERT INTO public.readme (id) SELECT data FROM generate_series(1, 3) AS data;
```

Output:

```json
{
  "op": "I",
  "message_id": "4606b12b-ab41-41e6-9717-7ce92f8a9857",
  "lsn": 23530912,
  "transaction": {
    "tx_id": 499,
    "begin_lsn": 23531416,
    "commit_ts": "2022-01-14T17:22:10.298334+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": true,
        "type_id": 23,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": false,
        "type_id": 1184,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema_name": "public",
    "table": "readme",
    "relation_id": 16403
  },
  "before": null,
  "after": {
    "id": 1,
    "created_at": "2022-01-14T17:22:10.296740+00:00"
  }
}
{
  "op": "I",
  "message_id": "1ede0643-42b6-4bb1-8a98-8e4ca10c7915",
  "lsn": 23531144,
  "transaction": {
    "tx_id": 499,
    "begin_lsn": 23531416,
    "commit_ts": "2022-01-14T17:22:10.298334+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": true,
        "type_id": 23,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": false,
        "type_id": 1184,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema_name": "public",
    "table": "readme",
    "relation_id": 16403
  },
  "before": null,
  "after": {
    "id": 2,
    "created_at": "2022-01-14T17:22:10.296740+00:00"
  }
}
{
  "op": "I",
  "message_id": "fb477de5-8281-4102-96ee-649a838d38f2",
  "lsn": 23531280,
  "transaction": {
    "tx_id": 499,
    "begin_lsn": 23531416,
    "commit_ts": "2022-01-14T17:22:10.298334+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": true,
        "type_id": 23,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": false,
        "type_id": 1184,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema_name": "public",
    "table": "readme",
    "relation_id": 16403
  },
  "before": null,
  "after": {
    "id": 3,
    "created_at": "2022-01-14T17:22:10.296740+00:00"
  }
}
```

## Why use this package?

* Preference to use the built in pgoutput plugin. Some plugins are not available in managed database services such as RDS or cannot be updated to a new version.
* The pgoutput plugin includes useful metadata such as relid.

## Useful links

* logical decoding: <https://www.postgresql.org/docs/12/logicaldecoding.html>
* message specification: <https://www.postgresql.org/docs/12/protocol-logicalrep-message-formats.html>
* postgres publication: <https://www.postgresql.org/docs/12/sql-createpublication.html>
* psycopg2 replication support: <https://www.psycopg.org/docs/extras.html#replication-support-objects>
