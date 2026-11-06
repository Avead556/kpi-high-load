import argparse
import json
import urllib.error
import urllib.request

LOT = {
    "lot_number": "LOT-2609-V018",
    "vaccine": "Vaxigrip Tetra",
    "doses": 600,
    "temperature_regime": "+2..+8",
    "manufactured_on": "2026-09-01",
    "expires_on": "2027-06-30",
}


def call(base, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(base + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as err:
        status, raw = err.code, err.read()
    payload = json.loads(raw) if raw else None
    shown = payload
    if isinstance(payload, dict) and "items" in payload:
        shown = dict(payload, items=f"<{len(payload['items'])} items, ids {[i['id'] for i in payload['items']]}>")
    elif isinstance(payload, dict) and "created_at" in payload:
        shown = {k: v for k, v in payload.items() if k not in ("created_at", "updated_at")}
    print(f"$ {method} {path}" + (f" {json.dumps(body, ensure_ascii=False)}" if body is not None else ""))
    print(f"  -> {status} {json.dumps(shown, ensure_ascii=False) if shown is not None else '(empty body)'}")
    return status, payload


def short(item):
    keep = ("id", "lot_number", "vaccine", "doses", "manufactured_on", "expires_on")
    return {k: item[k] for k in keep}


def main():
    parser = argparse.ArgumentParser(description="full CRUD cycle against the running stack")
    parser.add_argument("--url", default="http://127.0.0.1:8018")
    args = parser.parse_args()
    base = args.url
    status, created = call(base, "POST", "/vaccine-lots", LOT)
    assert status == 201
    lot_id = created["id"]
    status, page = call(base, "GET", "/vaccine-lots?vaccine=Vaxigrip%20Tetra&limit=100")
    assert status == 200 and any(item["id"] == lot_id for item in page["items"])
    print(f"  found id {lot_id} in the list: {[short(i) for i in page['items'] if i['id'] == lot_id]}")
    status, _ = call(base, "GET", f"/vaccine-lots/{lot_id}")
    assert status == 200
    replacement = dict(LOT, doses=480, expires_on="2027-03-31")
    status, _ = call(base, "PUT", f"/vaccine-lots/{lot_id}", replacement)
    assert status == 200
    status, _ = call(base, "POST", "/vaccine-lots", dict(LOT, lot_number="LOT-BAD", doses=0, expires_on="2026-08-01"))
    assert status == 400
    status, _ = call(base, "PUT", f"/vaccine-lots/{lot_id}", {"lot_number": "LOT-BAD"})
    assert status == 400
    status, _ = call(base, "POST", "/vaccine-lots", dict(LOT, doses=10))
    assert status == 409
    status, _ = call(base, "DELETE", f"/vaccine-lots/{lot_id}")
    assert status == 204
    status, _ = call(base, "GET", f"/vaccine-lots/{lot_id}")
    assert status == 404
    status, _ = call(base, "PUT", f"/vaccine-lots/{lot_id}", replacement)
    assert status == 404
    status, _ = call(base, "DELETE", f"/vaccine-lots/{lot_id}")
    assert status == 404
    print("cycle ok")


if __name__ == "__main__":
    main()
