#!/usr/bin/env python3
"""
Configures the NiFi Phase 2 ingestion and transformation flow via the NiFi REST API.

Flow:
  GetFile (/app/data/transactions.csv)
    → SplitText (one line per flowfile)
    → RouteOnContent: skip CSV header
    → RouteOnContent: detect malformed rows (INVALID_AMT, empty fields, ERR currency)
        ├── malformed → LogMessage (error bulletin)
        └── valid → ReplaceText (CSV line → JSON)
                      → PublishKafka (transactions-topic)

Usage:
  python3 scripts/setup-nifi-flow.py

Prerequisites:
  pip install requests
  NiFi must be running on https://localhost:8161
"""

import json
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_BASE = "https://localhost:8161/nifi-api"
USERNAME = "admin"
PASSWORD = "admin123456789"

KAFKA_BOOTSTRAP = "kafka:29092"
OUTPUT_TOPIC = "transactions-topic"
CSV_DIR = "/app/data"
CSV_FILTER = "transactions\\.csv"

# Converts a single CSV data line to JSON.
# Input:  550e8400-...,GIG-USR-001,150.00,EUR,2024-03-25T10:00:00Z,SUCCESS
# Output: {"transaction_id":"...","account_id":"...","amount":"...","currency":"...","timestamp":"...","status":"..."}
CSV_TO_JSON_REGEX = r"^([^,\r\n]+),([^,\r\n]+),([^,\r\n]+),([^,\r\n]+),([^,\r\n]+),([^,\r\n\s]+)\s*$"
CSV_TO_JSON_REPLACEMENT = '{"transaction_id":"$1","account_id":"$2","amount":"$3","currency":"$4","timestamp":"$5","status":"$6"}'

# Matches malformed rows: empty transaction_id/account_id, INVALID_AMT, currency=ERR
MALFORMED_PATTERN = r"(^,|,,|INVALID_AMT|,ERR,)"

# Matches the header line
HEADER_PATTERN = r"^transaction_id,"


def wait_for_nifi(timeout=120):
    print("[0/8] Waiting for NiFi to be ready...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.post(
                f"{NIFI_BASE}/access/token",
                data={"username": USERNAME, "password": PASSWORD},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                verify=False, timeout=5
            )
            if resp.status_code == 201:
                print(" ready.")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(5)
    print()
    raise TimeoutError(f"NiFi did not become ready within {timeout}s")


def get_token():
    resp = requests.post(
        f"{NIFI_BASE}/access/token",
        data={"username": USERNAME, "password": PASSWORD},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False
    )
    resp.raise_for_status()
    return resp.text


def h(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def get_root_id(token):
    resp = requests.get(f"{NIFI_BASE}/process-groups/root", headers=h(token), verify=False)
    resp.raise_for_status()
    return resp.json()["id"]


def clear_existing_flow(token, root_id):
    requests.put(
        f"{NIFI_BASE}/flow/process-groups/{root_id}",
        headers=h(token),
        json={"id": root_id, "state": "STOPPED", "disconnectedNodeAcknowledged": False},
        verify=False
    )
    time.sleep(2)

    conns = requests.get(f"{NIFI_BASE}/process-groups/{root_id}/connections", headers=h(token), verify=False).json()
    for conn in conns.get("connections", []):
        cid = conn["id"]
        queued = conn.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", 0)
        if queued > 0:
            requests.post(f"{NIFI_BASE}/flowfile-queues/{cid}/drop-requests", headers=h(token), verify=False)
            time.sleep(2)
        requests.delete(
            f"{NIFI_BASE}/connections/{cid}?version={conn['revision']['version']}",
            headers=h(token), verify=False
        )

    procs = requests.get(f"{NIFI_BASE}/process-groups/{root_id}/processors", headers=h(token), verify=False).json()
    for proc in procs.get("processors", []):
        requests.delete(
            f"{NIFI_BASE}/processors/{proc['id']}?version={proc['revision']['version']}",
            headers=h(token), verify=False
        )

    svcs = requests.get(f"{NIFI_BASE}/flow/process-groups/{root_id}/controller-services", headers=h(token), verify=False).json()
    for svc in svcs.get("controllerServices", []):
        requests.put(
            f"{NIFI_BASE}/controller-services/{svc['id']}/run-status",
            headers=h(token),
            json={"revision": svc["revision"], "state": "DISABLED", "disconnectedNodeAcknowledged": False},
            verify=False
        )
    time.sleep(1)
    for svc in svcs.get("controllerServices", []):
        detail = requests.get(f"{NIFI_BASE}/controller-services/{svc['id']}", headers=h(token), verify=False).json()
        requests.delete(
            f"{NIFI_BASE}/controller-services/{svc['id']}?version={detail['revision']['version']}",
            headers=h(token), verify=False
        )


def create_controller_service(token, root_id, svc_type, name, props):
    body = {"revision": {"version": 0}, "component": {"name": name, "type": svc_type, "properties": props}}
    resp = requests.post(f"{NIFI_BASE}/process-groups/{root_id}/controller-services",
                         headers=h(token), json=body, verify=False)
    resp.raise_for_status()
    return resp.json()


def enable_controller_service(token, svc_id, version):
    requests.put(
        f"{NIFI_BASE}/controller-services/{svc_id}/run-status",
        headers=h(token),
        json={"revision": {"version": version}, "state": "ENABLED", "disconnectedNodeAcknowledged": False},
        verify=False
    ).raise_for_status()


def create_processor(token, root_id, proc_type, name, props, extra_config=None, x=0, y=0):
    config = {"properties": props}
    if extra_config:
        config.update(extra_config)
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name, "type": proc_type,
            "position": {"x": x, "y": y},
            "config": config
        }
    }
    resp = requests.post(f"{NIFI_BASE}/process-groups/{root_id}/processors",
                         headers=h(token), json=body, verify=False)
    resp.raise_for_status()
    return resp.json()


def get_relationships(token, proc_id):
    resp = requests.get(f"{NIFI_BASE}/processors/{proc_id}", headers=h(token), verify=False)
    return [r["name"] for r in resp.json().get("component", {}).get("relationships", [])]


def set_auto_terminate(token, proc_id, relationships):
    resp = requests.get(f"{NIFI_BASE}/processors/{proc_id}", headers=h(token), verify=False)
    proc = resp.json()
    proc["component"]["config"]["autoTerminatedRelationships"] = relationships
    requests.put(f"{NIFI_BASE}/processors/{proc_id}", headers=h(token), json=proc, verify=False).raise_for_status()


def connect(token, root_id, src_id, dst_id, relationship):
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": root_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": root_id, "type": "PROCESSOR"},
            "selectedRelationships": [relationship], "bends": []
        }
    }
    requests.post(f"{NIFI_BASE}/process-groups/{root_id}/connections",
                  headers=h(token), json=body, verify=False).raise_for_status()


def start_processor(token, proc_id):
    resp = requests.get(f"{NIFI_BASE}/processors/{proc_id}", headers=h(token), verify=False)
    version = resp.json()["revision"]["version"]
    requests.put(
        f"{NIFI_BASE}/processors/{proc_id}/run-status",
        headers=h(token),
        json={"revision": {"version": version}, "state": "RUNNING", "disconnectedNodeAcknowledged": False},
        verify=False
    )


def main():
    print("Setting up NiFi Phase 2 ingestion flow...")
    print()

    wait_for_nifi()

    print("[1/8] Authenticating...")
    token = get_token()
    root_id = get_root_id(token)
    print(f"      Root process group: {root_id}")

    print("[2/8] Clearing existing flow...")
    clear_existing_flow(token, root_id)

    print("[3/8] Creating Kafka3ConnectionService...")
    kafka_svc = create_controller_service(
        token, root_id,
        "org.apache.nifi.kafka.service.Kafka3ConnectionService",
        "Kafka Connection",
        {"bootstrap.servers": KAFKA_BOOTSTRAP}
    )
    kafka_svc_id = kafka_svc["id"]
    enable_controller_service(token, kafka_svc_id, kafka_svc["revision"]["version"])
    time.sleep(3)
    print(f"      Service ID: {kafka_svc_id}")

    print("[4/8] Creating processors...")

    get_file = create_processor(
        token, root_id,
        "org.apache.nifi.processors.standard.GetFile",
        "Read CSV file",
        {
            "Input Directory": CSV_DIR,
            "File Filter": CSV_FILTER,
            "Keep Source File": "true",
            "Polling Interval": "3600 sec",
        },
        x=100, y=100
    )

    split = create_processor(
        token, root_id,
        "org.apache.nifi.processors.standard.SplitText",
        "Split into lines",
        {
            "Line Split Count": "1",
            "Header Line Count": "0",
            "Remove Trailing Newlines": "true",
        },
        x=400, y=100
    )

    route_header = create_processor(
        token, root_id,
        "org.apache.nifi.processors.standard.RouteOnContent",
        "Skip header",
        {
            "Match Requirement": "content must match exactly",
            "header": HEADER_PATTERN,
        },
        x=700, y=100
    )

    route_validate = create_processor(
        token, root_id,
        "org.apache.nifi.processors.standard.RouteOnContent",
        "Detect malformed rows",
        {
            "Match Requirement": "content must contain match",
            "malformed": MALFORMED_PATTERN,
        },
        x=700, y=300
    )

    log_invalid = create_processor(
        token, root_id,
        "org.apache.nifi.processors.standard.LogMessage",
        "Log invalid rows",
        {
            "Log Level": "warn",
            "Log Message": "Malformed/header row skipped: ${filename}",
        },
        x=1000, y=200
    )

    replace = create_processor(
        token, root_id,
        "org.apache.nifi.processors.standard.ReplaceText",
        "CSV line to JSON",
        {
            "Search Value": CSV_TO_JSON_REGEX,
            "Replacement Value": CSV_TO_JSON_REPLACEMENT,
            "Evaluation Mode": "Entire text",
            "Line-by-Line Evaluation Mode": "All",
        },
        x=700, y=500
    )

    publish = create_processor(
        token, root_id,
        "org.apache.nifi.kafka.processors.PublishKafka",
        "Publish to transactions-topic",
        {
            "Kafka Connection Service": kafka_svc_id,
            "Topic Name": OUTPUT_TOPIC,
        },
        x=1000, y=500
    )

    print(f"      GetFile:           {get_file['id']}")
    print(f"      SplitText:         {split['id']}")
    print(f"      RouteOnContent(h): {route_header['id']}")
    print(f"      RouteOnContent(v): {route_validate['id']}")
    print(f"      LogMessage:        {log_invalid['id']}")
    print(f"      ReplaceText:       {replace['id']}")
    print(f"      PublishKafka:      {publish['id']}")

    print("[5/8] Connecting processors...")
    connect(token, root_id, get_file["id"],       split["id"],          "success")
    connect(token, root_id, split["id"],           route_header["id"],   "splits")
    connect(token, root_id, route_header["id"],    log_invalid["id"],    "header")
    connect(token, root_id, route_header["id"],    route_validate["id"], "unmatched")
    connect(token, root_id, route_validate["id"],  log_invalid["id"],    "malformed")
    connect(token, root_id, route_validate["id"],  replace["id"],        "unmatched")
    connect(token, root_id, replace["id"],         publish["id"],        "success")
    connect(token, root_id, replace["id"],         log_invalid["id"],    "failure")

    print("[6/8] Configuring auto-terminate relationships...")

    split_rels = get_relationships(token, split["id"])
    set_auto_terminate(token, split["id"], [r for r in split_rels if r not in ("splits",)])

    log_rels = get_relationships(token, log_invalid["id"])
    set_auto_terminate(token, log_invalid["id"], log_rels)

    publish_rels = get_relationships(token, publish["id"])
    set_auto_terminate(token, publish["id"], publish_rels)

    print("[7/8] Starting processors...")
    for proc, name in [
        (get_file, "GetFile"), (split, "SplitText"),
        (route_header, "RouteOnContent(header)"), (route_validate, "RouteOnContent(validate)"),
        (log_invalid, "LogMessage"), (replace, "ReplaceText"), (publish, "PublishKafka")
    ]:
        start_processor(token, proc["id"])
        print(f"      Started {name}")

    print("[8/8] Done.")
    print()
    print("=" * 60)
    print("NiFi flow ready!")
    print()
    print("  GetFile → SplitText → RouteOnContent(header skip)")
    print("                      → RouteOnContent(malformed detect)")
    print("                          ├── malformed → LogMessage")
    print("                          └── valid → ReplaceText → PublishKafka")
    print()
    print(f"  CSV input : {CSV_DIR}/transactions.csv")
    print(f"  Kafka out : {OUTPUT_TOPIC}")
    print()
    print(f"  NiFi UI   : https://localhost:8161/nifi")
    print(f"  Username  : {USERNAME}  /  Password: {PASSWORD}")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"\nHTTP error {e.response.status_code}: {e.response.text[:300]}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
