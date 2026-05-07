#!/usr/bin/env python3
"""
Configures the NiFi Phase 2 transformation flow via the NiFi REST API.

Flow:
  ConsumeKafka (transactions-topic)
    → JoltTransformJSON (user_id → account_id, transaction_date → timestamp)
    → PublishKafka (transactions-processed)

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
INPUT_TOPIC = "transactions-topic"
OUTPUT_TOPIC = "transactions-processed"
CONSUMER_GROUP = "nifi-kafka-group"

# Rename user_id → account_id and transaction_date → timestamp
JOLT_SPEC = json.dumps([{
    "operation": "shift",
    "spec": {
        "transaction_id": "transaction_id",
        "user_id": "account_id",
        "amount": "amount",
        "currency": "currency",
        "transaction_date": "timestamp",
        "status": "status",
        "description": "description"
    }
}])


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
    """Remove all processors and connections from the root group."""
    # Stop all processors first
    requests.put(
        f"{NIFI_BASE}/flow/process-groups/{root_id}",
        headers=h(token),
        json={"id": root_id, "state": "STOPPED", "disconnectedNodeAcknowledged": False},
        verify=False
    )
    time.sleep(2)

    # Delete connections
    conns = requests.get(f"{NIFI_BASE}/process-groups/{root_id}/connections", headers=h(token), verify=False).json()
    for conn in conns.get("connections", []):
        requests.delete(
            f"{NIFI_BASE}/connections/{conn['id']}?version={conn['revision']['version']}",
            headers=h(token), verify=False
        )

    # Delete processors
    procs = requests.get(f"{NIFI_BASE}/process-groups/{root_id}/processors", headers=h(token), verify=False).json()
    for proc in procs.get("processors", []):
        requests.delete(
            f"{NIFI_BASE}/processors/{proc['id']}?version={proc['revision']['version']}",
            headers=h(token), verify=False
        )

    # Delete controller services
    svcs = requests.get(f"{NIFI_BASE}/flow/process-groups/{root_id}/controller-services", headers=h(token), verify=False).json()
    for svc in svcs.get("controllerServices", []):
        # Disable first
        requests.put(
            f"{NIFI_BASE}/controller-services/{svc['id']}/run-status",
            headers=h(token),
            json={"revision": svc["revision"], "state": "DISABLED", "disconnectedNodeAcknowledged": False},
            verify=False
        )
    time.sleep(1)
    for svc in svcs.get("controllerServices", []):
        svc_detail = requests.get(f"{NIFI_BASE}/controller-services/{svc['id']}", headers=h(token), verify=False).json()
        requests.delete(
            f"{NIFI_BASE}/controller-services/{svc['id']}?version={svc_detail['revision']['version']}",
            headers=h(token), verify=False
        )


def create_controller_service(token, root_id, svc_type, name, props):
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": svc_type,
            "properties": props
        }
    }
    resp = requests.post(
        f"{NIFI_BASE}/process-groups/{root_id}/controller-services",
        headers=h(token), json=body, verify=False
    )
    resp.raise_for_status()
    return resp.json()


def enable_controller_service(token, svc_id, version):
    body = {
        "revision": {"version": version},
        "state": "ENABLED",
        "disconnectedNodeAcknowledged": False
    }
    resp = requests.put(
        f"{NIFI_BASE}/controller-services/{svc_id}/run-status",
        headers=h(token), json=body, verify=False
    )
    resp.raise_for_status()


def create_processor(token, root_id, proc_type, name, props, x, y):
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": proc_type,
            "position": {"x": x, "y": y},
            "config": {
                "properties": props
            }
        }
    }
    resp = requests.post(
        f"{NIFI_BASE}/process-groups/{root_id}/processors",
        headers=h(token), json=body, verify=False
    )
    resp.raise_for_status()
    return resp.json()


def set_auto_terminate(token, proc_id, relationships):
    resp = requests.get(f"{NIFI_BASE}/processors/{proc_id}", headers=h(token), verify=False)
    resp.raise_for_status()
    proc = resp.json()
    proc["component"]["config"]["autoTerminatedRelationships"] = relationships
    resp = requests.put(
        f"{NIFI_BASE}/processors/{proc_id}",
        headers=h(token), json=proc, verify=False
    )
    resp.raise_for_status()
    return resp.json()


def connect_processors(token, root_id, src_id, dst_id, relationship):
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": root_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": root_id, "type": "PROCESSOR"},
            "selectedRelationships": [relationship],
            "bends": []
        }
    }
    resp = requests.post(
        f"{NIFI_BASE}/process-groups/{root_id}/connections",
        headers=h(token), json=body, verify=False
    )
    resp.raise_for_status()
    return resp.json()


def start_processor(token, proc_id):
    resp = requests.get(f"{NIFI_BASE}/processors/{proc_id}", headers=h(token), verify=False)
    version = resp.json()["revision"]["version"]
    requests.put(
        f"{NIFI_BASE}/processors/{proc_id}/run-status",
        headers=h(token),
        json={"revision": {"version": version}, "state": "RUNNING", "disconnectedNodeAcknowledged": False},
        verify=False
    )


def get_relationships(token, proc_id):
    resp = requests.get(f"{NIFI_BASE}/processors/{proc_id}", headers=h(token), verify=False)
    return [r["name"] for r in resp.json().get("component", {}).get("relationships", [])]


def wait_for_nifi(timeout=120):
    print("[0/7] Waiting for NiFi to be ready...", end="", flush=True)
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


def main():
    print("Setting up NiFi Phase 2 transformation flow...")
    print()

    wait_for_nifi()

    print("[1/7] Authenticating...")
    token = get_token()

    root_id = get_root_id(token)
    print(f"      Root process group: {root_id}")

    print("[2/7] Clearing existing flow...")
    clear_existing_flow(token, root_id)

    print("[3/7] Creating Kafka3ConnectionService...")
    kafka_svc = create_controller_service(
        token, root_id,
        "org.apache.nifi.kafka.service.Kafka3ConnectionService",
        "Kafka Connection",
        {"bootstrap.servers": KAFKA_BOOTSTRAP}
    )
    kafka_svc_id = kafka_svc["id"]
    print(f"      Service ID: {kafka_svc_id}")

    print("[4/7] Enabling Kafka connection service...")
    enable_controller_service(token, kafka_svc_id, kafka_svc["revision"]["version"])
    time.sleep(3)

    print("[5/7] Creating processors...")

    consume = create_processor(
        token, root_id,
        "org.apache.nifi.kafka.processors.ConsumeKafka",
        "Consume transactions-topic",
        {
            "kafka.connection.service": kafka_svc_id,
            "topic.names": INPUT_TOPIC,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
        },
        x=100, y=300
    )
    consume_id = consume["id"]
    print(f"      ConsumeKafka:       {consume_id}")

    jolt = create_processor(
        token, root_id,
        "org.apache.nifi.processors.jolt.JoltTransformJSON",
        "Rename fields",
        {
            "jolt-transform": "jolt-transform-shift",
            "jolt-spec": JOLT_SPEC,
        },
        x=450, y=300
    )
    jolt_id = jolt["id"]
    print(f"      JoltTransformJSON:  {jolt_id}")

    publish = create_processor(
        token, root_id,
        "org.apache.nifi.kafka.processors.PublishKafka",
        "Publish transactions-processed",
        {
            "kafka.connection.service": kafka_svc_id,
            "topic.name": OUTPUT_TOPIC,
        },
        x=800, y=300
    )
    publish_id = publish["id"]
    print(f"      PublishKafka:       {publish_id}")

    print("[6/7] Connecting processors and configuring relationships...")
    connect_processors(token, root_id, consume_id, jolt_id, "success")
    connect_processors(token, root_id, jolt_id, publish_id, "success")

    # Auto-terminate unused relationships so processors can start
    jolt_rels = get_relationships(token, jolt_id)
    set_auto_terminate(token, jolt_id, [r for r in jolt_rels if r != "success"])

    publish_rels = get_relationships(token, publish_id)
    set_auto_terminate(token, publish_id, publish_rels)  # PublishKafka auto-terminates all (fire & forget)

    print("[7/7] Starting processors...")
    for proc_id, name in [(consume_id, "ConsumeKafka"), (jolt_id, "JoltTransformJSON"), (publish_id, "PublishKafka")]:
        start_processor(token, proc_id)
        print(f"      Started {name}")

    print()
    print("=" * 55)
    print("NiFi flow ready!")
    print(f"  {INPUT_TOPIC}  →  JoltTransformJSON  →  {OUTPUT_TOPIC}")
    print()
    print(f"  NiFi UI  : https://localhost:8161/nifi")
    print(f"  Username : {USERNAME}")
    print(f"  Password : {PASSWORD}")
    print("=" * 55)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"\nHTTP error: {e.response.status_code} — {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
