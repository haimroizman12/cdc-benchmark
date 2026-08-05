"""Configure Airbyte for Postgres(CDC) -> MSSQL and drive syncs.

Uses the Airbyte **public API** (/api/public/v1) with an OAuth bearer token minted
from the application client id/secret. abctl-installed Airbyte gates the older
internal Configuration API behind session auth, so the public API is the supported
path for scripting here.

Usage:
    python airbyte/configure.py defs      # print resolved connector definition ids
    python airbyte/configure.py setup     # create source, destination, connection
    python airbyte/configure.py sync      # trigger one sync and wait for completion

Env:
    AIRBYTE_URL           public API base (default http://localhost:8010/api/public/v1)
    AIRBYTE_CLIENT_ID     application client id   (abctl local credentials)
    AIRBYTE_CLIENT_SECRET application client secret
    POSTGRES_* / MSSQL_*  DB creds (same as the rest of the benchmark)

The connection id is stored in airbyte/.connection_id.
Postgres CDC prerequisites (publication 'airbyte_pub', slot 'airbyte_slot') must
exist first — `make airbyte-up` creates them.
"""
from __future__ import annotations
import json
import os
import sys
import time
import urllib.error
import urllib.request

PUBLIC_BASE = os.environ.get("AIRBYTE_URL", "http://localhost:8010/api/public/v1")
CONFIG_BASE = PUBLIC_BASE.replace("/api/public/v1", "/api/v1")
CID = os.environ.get("AIRBYTE_CLIENT_ID", "")
CSEC = os.environ.get("AIRBYTE_CLIENT_SECRET", "")

_TOKEN = None


def _raw(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {url} -> {e.code}: {e.read().decode()[:600]}")


def _token():
    global _TOKEN
    if _TOKEN is None:
        r = _raw("POST", PUBLIC_BASE + "/applications/token",
                 {"client_id": CID, "client_secret": CSEC},
                 {"Content-Type": "application/json"})
        _TOKEN = r["access_token"]
    return _TOKEN


def _h():
    return {"Content-Type": "application/json", "Authorization": "Bearer " + _token()}


def _pub(method, path, body=None):
    return _raw(method, PUBLIC_BASE + path, body, _h())


def _workspace_id():
    return _pub("GET", "/workspaces")["data"][0]["workspaceId"]


def _def_id(kind, repo):
    """Resolve a connector definitionId by docker repo via the internal list_latest
    endpoint (instance-level, accepts the app bearer token)."""
    key = f"{kind}Definitions"
    idk = f"{kind}DefinitionId"
    data = _raw("POST", f"{CONFIG_BASE}/{kind}_definitions/list_latest", {}, _h())[key]
    for c in data:
        if c["dockerRepository"] == repo:
            return c[idk]
    return None


def defs():
    print("postgres source def:", _def_id("source", "airbyte/source-postgres"))
    print("mssql destination def:", _def_id("destination", "airbyte/destination-mssql"))


def setup():
    ws = _workspace_id()
    src_def = _def_id("source", "airbyte/source-postgres")
    dst_def = _def_id("destination", "airbyte/destination-mssql")
    if not src_def or not dst_def:
        raise SystemExit("Could not resolve connector definition ids.")

    # Airbyte's connectors run as pods inside abctl's kind cluster, a separate network
    # from the DB rig, so the compose service names don't resolve there. AB_SRC_HOST /
    # AB_DST_HOST carry addresses reachable from the cluster (the DBs' kind-network IPs;
    # make airbyte-up wires this up).
    src_host = os.environ.get("AB_SRC_HOST", "postgres")
    src_port = int(os.environ.get("AB_SRC_PORT", "5432"))
    dst_host = os.environ.get("AB_DST_HOST", "mssql")
    dst_port = int(os.environ.get("AB_DST_PORT", "1433"))

    src = _pub("POST", "/sources", {
        "name": "pg-cdc", "workspaceId": ws, "definitionId": src_def,
        "configuration": {
            "host": src_host, "port": src_port, "database": os.environ["POSTGRES_DB"],
            "username": os.environ["POSTGRES_USER"], "password": os.environ["POSTGRES_PASSWORD"],
            "schemas": ["public"],
            "ssl_mode": {"mode": "disable"},
            "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
            "replication_method": {
                "method": "CDC", "publication": "airbyte_pub",
                # 120 is the valid minimum; the connector returns early once it detects
                # data, so under continuous load syncs don't wait the full window.
                "replication_slot": "airbyte_slot", "initial_waiting_seconds": 120,
            },
        }})["sourceId"]

    dst = _pub("POST", "/destinations", {
        "name": "mssql", "workspaceId": ws, "definitionId": dst_def,
        "configuration": {
            "host": dst_host, "port": dst_port, "database": os.environ.get("MSSQL_DB", "target_db"),
            "schema": "dbo", "user": "sa", "password": os.environ["MSSQL_SA_PASSWORD"],
            "ssl_method": {"name": "unencrypted"},
            "load_type": {"load_type": "INSERT"},
        }})["destinationId"]

    conn = _pub("POST", "/connections", {
        "name": "pg-mssql", "sourceId": src, "destinationId": dst,
        "schedule": {"scheduleType": "manual"},
        "namespaceDefinition": "destination",
        "configurations": {"streams": [
            {"name": "source_events", "syncMode": "incremental_append"},
        ]},
    })["connectionId"]

    with open("airbyte/.connection_id", "w") as f:
        f.write(conn)
    print("source:", src)
    print("destination:", dst)
    print("connection:", conn)


def sync():
    conn = open("airbyte/.connection_id").read().strip()
    job = _pub("POST", "/jobs", {"connectionId": conn, "jobType": "sync"})["jobId"]
    # "incomplete" is transient (Airbyte auto-retries a failed attempt, after which the
    # job can still reach "succeeded"), so only succeeded/failed/cancelled are terminal.
    # Bounded so a stuck job can't hang the sync loop.
    deadline = time.time() + 600
    while time.time() < deadline:
        st = _pub("GET", f"/jobs/{job}")["status"]
        if st in ("succeeded", "failed", "cancelled"):
            print("sync", job, st)
            return 0 if st == "succeeded" else 1
        time.sleep(2)
    print("sync", job, "timeout")
    return 1


if __name__ == "__main__":
    {"defs": defs, "setup": setup, "sync": sync}[sys.argv[1]]()
