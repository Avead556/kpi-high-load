import argparse
import json
import random
import urllib.request
from datetime import date, timedelta

VACCINES = ["Comirnaty", "Spikevax", "Vaxigrip Tetra", "Priorix", "Engerix-B", "Infanrix hexa", "Varivax", "BCG"]
REGIMES = {"Comirnaty": "-90..-60", "Spikevax": "-25..-15"}


def existing_lots(url):
    found, offset = {}, 0
    while True:
        with urllib.request.urlopen(f"{url}/vaccine-lots?limit=100&offset={offset}") as response:
            page = json.load(response)
        for item in page["items"]:
            found[item["lot_number"]] = item["id"]
        offset += len(page["items"])
        if not page["items"] or offset >= page["total"]:
            return found


def main():
    parser = argparse.ArgumentParser(description="create vaccine lots through the API")
    parser.add_argument("--url", default="http://127.0.0.1:8018")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--out", default="results/ids.txt")
    args = parser.parse_args()
    rng = random.Random(18)
    existing = existing_lots(args.url)
    ids = []
    created = 0
    for i in range(args.count):
        vaccine = rng.choice(VACCINES)
        made = date(2026, 1, 1) + timedelta(days=rng.randint(0, 250))
        body = {
            "lot_number": f"LOT-{made:%y%m}-{i + 1:04d}",
            "vaccine": vaccine,
            "doses": rng.choice([100, 200, 500, 1000, 1170, 2000]),
            "temperature_regime": REGIMES.get(vaccine, "+2..+8"),
            "manufactured_on": made.isoformat(),
            "expires_on": (made + timedelta(days=rng.choice([180, 270, 365, 730]))).isoformat(),
        }
        if body["lot_number"] in existing:
            ids.append(existing[body["lot_number"]])
            continue
        request = urllib.request.Request(
            f"{args.url}/vaccine-lots", data=json.dumps(body).encode(), method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request) as response:
            ids.append(json.load(response)["id"])
        created += 1
    with open(args.out, "w") as f:
        f.write(",".join(map(str, ids)) + "\n")
    print(f"created {created} lots, {len(ids) - created} already present, ids {min(ids)}..{max(ids)}")


if __name__ == "__main__":
    main()
