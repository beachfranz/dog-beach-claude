"""
flag_review_beaches.py
----------------------
Adds a review_status and review_notes column to ca_beaches_geocoded.csv.
Records with data quality issues are flagged as "Needs Review".
"""

import csv
from pathlib import Path

INPUT_CSV  = Path(r"C:\Users\beach\Documents\dog-beach-claude\share\Dog_Beaches\ca_beaches_geocoded.csv")
OUTPUT_CSV = INPUT_CSV.with_stem("ca_beaches_flagged")

# Unincorporated communities in CA that Google returns as "city" but have no
# municipal government — rules come from county or special district instead.
UNINCORPORATED = {
    "leucadia", "olivenhain", "cardiff-by-the-sea", "del mar heights",
    "malibu colony", "topanga", "east los angeles", "west hollywood",
    "marina del rey", "playa del rey", "el segundo",
    "two harbors", "pescadero", "montara", "el granada",
    "moss beach", "princeton-by-the-sea", "bodega bay", "dillon beach",
    "tomales", "inverness", "olema", "stinson beach", "muir beach",
    "bolinas", "pebble beach", "carmel highlands", "big sur",
    "cambria", "cayucos", "harmony", "baywood-los osos",
    "avila beach", "shell beach", "oceano",
}

def flag(row: dict) -> tuple[str, str]:
    """Return (review_status, review_notes) for a row."""
    notes = []

    if row.get("geocode_status") != "OK":
        notes.append(f"geocode failed: {row.get('geocode_status')}")

    if not row.get("state"):
        notes.append("no state returned — likely offshore or remote (Plus Code address)")

    if not row.get("city"):
        notes.append("no city returned")

    if not row.get("county"):
        notes.append("no county returned")

    city_lower = row.get("city", "").lower()
    if city_lower in UNINCORPORATED:
        notes.append(f"'{row['city']}' is unincorporated — governing body is county, not city")
        # Fix governing fields
        row["governing_jurisdiction"] = "county"
        row["governing_body"] = row.get("county", "")

    if notes:
        return "Needs Review", "; ".join(notes)
    return "OK", ""


def main():
    with open(INPUT_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    fieldnames = list(rows[0].keys()) + ["review_status", "review_notes"]

    needs_review = 0
    for row in rows:
        status, notes = flag(row)
        row["review_status"] = status
        row["review_notes"]  = notes
        if status == "Needs Review":
            needs_review += 1

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Total records : {len(rows)}")
    print(f"OK            : {len(rows) - needs_review}")
    print(f"Needs Review  : {needs_review}")
    print(f"Output        : {OUTPUT_CSV}")
    print()
    print("Needs Review breakdown:")
    for row in rows:
        if row["review_status"] == "Needs Review":
            print(f"  {row['name']:<40} {row['review_notes']}")

if __name__ == "__main__":
    main()
