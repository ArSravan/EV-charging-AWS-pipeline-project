from pathlib import Path
import pandas as pd
import re

INPUT_FILE = Path("data/raw/Ladesaeulenregister_BNetzA_2026-02-27.csv")
OUTPUT_FILE = Path("data/real_source/ev_stations.csv")

def fix_mojibake(text):
    if pd.isna(text):
        return text
    try:
        return text.encode("latin1").decode("utf-8")
    except Exception:
        return text

def canonicalize_city(city):
    if pd.isna(city):
        return None

    c = city.lower().strip()
    c = re.sub(r"\s+", " ", c)

    if " bei " in c:
        return None
    if c.startswith("berlin"):
        return "Berlin"
    if c.startswith("hamburg"):
        return "Hamburg"
    if c.startswith(("münchen", "munchen", "munich")):
        return "Munich"
    if c.startswith(("frankfurt am main", "frankfurt")):
        return "Frankfurt"
    if c.startswith(("köln", "koln", "cologne")):
        return "Cologne"
    if c.startswith("stuttgart"):
        return "Stuttgart"
    if c.startswith(("düsseldorf", "dusseldorf")):
        return "Düsseldorf"
    return None

def fix_coord(val):
    if pd.isna(val):
        return None

    digits = re.sub(r"\D", "", str(val))
    if len(digits) < 5:
        return None

    if digits.startswith(("47", "48", "49", "50", "51", "52", "53", "54", "55")):
        return float(digits[:2] + "." + digits[2:])
    return float(digits[:1] + "." + digits[1:])

def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    data = pd.read_csv(
        INPUT_FILE,
        sep=";",
        encoding="latin1",
        header=10
    )

    data = data[
        [
            "Ladeeinrichtungs-ID",
            "Betreiber",
            "Ort",
            "Postleitzahl",
            "Breitengrad",
            "LÃ¤ngengrad",
            "Anzahl Ladepunkte",
            "Nennleistung Ladeeinrichtung [kW]",
            "Art der Ladeeinrichtung",
        ]
    ]

    data = data.rename(columns={
        "Ladeeinrichtungs-ID": "station_id",
        "Betreiber": "operator_name",
        "Ort": "city",
        "Postleitzahl": "postcode",
        "Breitengrad": "latitude",
        "LÃ¤ngengrad": "longitude",
        "Anzahl Ladepunkte": "connector_count",
        "Nennleistung Ladeeinrichtung [kW]": "power_kw",
        "Art der Ladeeinrichtung": "charging_type",
    })

    df = data.copy()

    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].apply(fix_mojibake).str.strip()

    df["canonical_city"] = df["city"].apply(canonicalize_city)
    df = df[df["canonical_city"].notna()].copy()

    df = df.rename(columns={"city": "raw_city"})
    df["latitude"] = df["latitude"].apply(fix_coord)
    df["longitude"] = df["longitude"].apply(fix_coord)

    df["postcode"] = pd.to_numeric(df["postcode"], errors="coerce")
    df["connector_count"] = pd.to_numeric(df["connector_count"], errors="coerce")
    df["power_kw"] = pd.to_numeric(df["power_kw"], errors="coerce")

    charging_map = {
        "Normalladeeinrichtung": "Normal Charger",
        "Schnellladeeinrichtung": "Fast Charger",
    }
    df["charging_type"] = df["charging_type"].map(charging_map).fillna(df["charging_type"])

    df = df.drop_duplicates(subset=["station_id"])
    df = df.dropna(subset=["latitude", "longitude", "power_kw"])

    df = df.rename(columns={"canonical_city": "city"})
    df = df.drop(columns=["raw_city", "Unnamed: 0"], errors="ignore")

    df["postcode"] = df["postcode"].astype(str)

    cols = list(df.columns)
    cols.remove("city")
    cols.insert(1, "city")
    df = df[cols].reset_index(drop=True)

    df.to_csv(OUTPUT_FILE, index=False)

    print(df.columns.tolist())
    print(df.shape)
    print(f"Saved cleaned station file to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()