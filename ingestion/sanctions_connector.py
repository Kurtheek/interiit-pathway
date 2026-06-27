# ingestion/sanctions_connector.py
# P4 — External Data & Compliance
import pathway as pw
import urllib.request
import xml.etree.ElementTree as ET
import re
from datetime import datetime, timezone
from schemas.sanctions_schema import SanctionsSchema

OFAC_URL = "https://www.treasury.gov/ofac/downloads/sdn.xml"
NAMESPACE = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML"
NS = {"ns": NAMESPACE}

def normalize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)
    return name.strip()

def audit_log(event: str, details: str) -> None:
    """Write an audit trail entry to data/audit_log.jsonl"""
    import json, os
    log_path = os.path.join(os.path.dirname(__file__), "..", "data", "audit_log.jsonl")
    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event": event,
        "details": details,
    }
    with open(log_path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def validate_row(row: dict) -> bool:
    """Basic data quality check — reject rows missing critical fields."""
    if not row.get("sdn_uid"):
        audit_log("VALIDATION_FAIL", f"Missing sdn_uid for entity: {row.get('entity_name')}")
        return False
    if not row.get("entity_name"):
        audit_log("VALIDATION_FAIL", f"Missing entity_name for uid: {row.get('sdn_uid')}")
        return False
    return True

def fetch_sdn_entries() -> list[dict]:
    last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with urllib.request.urlopen(OFAC_URL) as response:
        raw = response.read()
    root = ET.fromstring(raw)
    pub_date_el = root.find("ns:publshInformation/ns:Publish_Date", NS)
    list_date = pub_date_el.text.strip() if pub_date_el is not None else ""
    if list_date:
        parts = list_date.split("/")
        list_date = f"{parts[2]}-{parts[0]}-{parts[1]}"
    rows = []
    for entry in root.findall("ns:sdnEntry", NS):
        uid = (entry.findtext("ns:uid", default="", namespaces=NS) or "").strip()
        name = (entry.findtext("ns:lastName", default="", namespaces=NS) or "").strip()
        sdn_type = (entry.findtext("ns:sdnType", default="", namespaces=NS) or "").strip().lower()
        programs = [p.text.strip() for p in entry.findall("ns:programList/ns:program", NS) if p.text]
        if not programs:
            programs = [""]
        aliases = [aka.findtext("ns:lastName", default="", namespaces=NS).strip() for aka in entry.findall("ns:akaList/ns:aka", NS)]
        alias_str = "|".join(a for a in aliases if a)
        first_address = entry.find("ns:addressList/ns:address", NS)
        country = ""
        if first_address is not None:
            country = (first_address.findtext("ns:country", default="", namespaces=NS) or "").strip()
        remarks = (entry.findtext("ns:remarks", default="", namespaces=NS) or "").strip()
        for program in programs:
            row = {"sdn_uid": uid, "entity_name": name, "entity_name_normalized": normalize_name(name), "entity_type": sdn_type, "program": program, "country": country, "alias": alias_str, "list_date": list_date, "last_updated": last_updated, "remarks": remarks}
            if validate_row(row):
                rows.append(row)
    audit_log("FETCH_SUCCESS", f"Fetched {len(rows)} SDN entries from OFAC. list_date={list_date}")
    return rows

class SanctionsConnector(pw.io.python.ConnectorSubject):
    def __init__(self, poll_interval_seconds: int = 3600):
        super().__init__()
        self.poll_interval = poll_interval_seconds

    def run(self):
        import time
        while True:
            try:
                print("[sanctions_connector] Fetching OFAC SDN list...")
                rows = fetch_sdn_entries()
                for row in rows:
                    self.next(**row)
                print(f"[sanctions_connector] Emitted {len(rows)} rows.")
            except Exception as e:
                print(f"[sanctions_connector] ERROR: {e}")
            time.sleep(self.poll_interval)

def get_sanctions_stream(poll_interval_seconds: int = 3600) -> pw.Table:
    connector = SanctionsConnector(poll_interval_seconds=poll_interval_seconds)
    return pw.io.python.read(connector, schema=SanctionsSchema)