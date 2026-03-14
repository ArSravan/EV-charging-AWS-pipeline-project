from __future__ import annotations

import argparse
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Tuple

import numpy as np
import pandas as pd


CITY_DEMAND_FACTOR = {
    "Berlin": 1.18,
    "Hamburg": 1.08,
    "Munich": 1.12,
    "Frankfurt": 1.10,
    "Cologne": 1.00,
    "Stuttgart": 1.03,
    "Düsseldorf": 1.00,
}

VEHICLE_TYPE_PROBS = {
    "Fast Charger": {
        "compact_ev": 0.30,
        "suv_ev": 0.32,
        "premium_ev": 0.18,
        "fleet_van": 0.10,
        "phev": 0.10,
    },
    "Normal Charger": {
        "compact_ev": 0.38,
        "suv_ev": 0.22,
        "premium_ev": 0.08,
        "fleet_van": 0.05,
        "phev": 0.27,
    },
}

PAYMENT_TYPE_PROBS = {
    "Fast Charger": {
        "app": 0.46,
        "card": 0.34,
        "roaming": 0.20,
    },
    "Normal Charger": {
        "app": 0.42,
        "subscription": 0.38,
        "rfid": 0.12,
        "card": 0.08,
    },
}

SEASONAL_MONTH_FACTOR = {
    1: 1.08,
    2: 1.05,
    3: 1.00,
    4: 0.98,
    5: 0.97,
    6: 0.96,
    7: 0.95,
    8: 0.96,
    9: 1.00,
    10: 1.02,
    11: 1.04,
    12: 1.07,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic EV charging sessions from a real station master file.")
    parser.add_argument("--stations-file", type=str, required=True, help="Path to ev_stations_final.csv")
    parser.add_argument("--output-dir", type=str, default="data/local_source")
    parser.add_argument("--start-month", type=str, default="2025-01", help="Format: YYYY-MM")
    parser.add_argument("--months", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--station-sample-frac", type=float, default=1.0, help="Use <1.0 for smaller datasets during testing")
    return parser.parse_args()


def month_starts(start_month: str, months: int) -> List[date]:
    year, month = map(int, start_month.split("-"))
    starts: List[date] = []
    y, m = year, month
    for _ in range(months):
        starts.append(date(y, m, 1))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return starts


def date_iter(start_day: date, end_day: date) -> Iterable[date]:
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def prepare_station_master(path: str, sample_frac: float, rng: np.random.Generator) -> pd.DataFrame:
    df = pd.read_csv(path)

    df = df.drop(columns=["Unnamed: 0"], errors="ignore")

    required_cols = {
        "station_id",
        "city",
        "operator_name",
        "postcode",
        "latitude",
        "longitude",
        "connector_count",
        "power_kw",
        "charging_type",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df["connector_count"] = pd.to_numeric(df["connector_count"], errors="coerce")
    df["power_kw"] = pd.to_numeric(df["power_kw"], errors="coerce")
    df["postcode"] = pd.to_numeric(df["postcode"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df = df.dropna(subset=["station_id", "city", "connector_count", "power_kw", "charging_type"]).copy()
    df["connector_count"] = df["connector_count"].astype(int)
    df["station_id"] = df["station_id"].astype(str)
    df["city"] = df["city"].astype(str).str.strip()
    df["charging_type"] = df["charging_type"].astype(str).str.strip()

    if sample_frac < 1.0:
        sample_frac = max(0.01, min(sample_frac, 1.0))
        df = df.sample(frac=sample_frac, random_state=int(rng.integers(0, 1_000_000)))

    return df.reset_index(drop=True)


def weekday_factor(charging_type: str, weekday_idx: int) -> float:
    is_weekend = weekday_idx >= 5

    if charging_type == "Fast Charger":
        return 1.00 if not is_weekend else 1.10

    # Normal Charger
    return 1.05 if not is_weekend else 0.88


def hour_weights(charging_type: str, weekday_idx: int) -> np.ndarray:
    is_weekend = weekday_idx >= 5
    w = np.ones(24, dtype=float)

    if charging_type == "Fast Charger":
        w[:] = 0.55
        w[6:10] += 1.50
        w[11:15] += 0.70
        w[16:21] += 1.70
        if is_weekend:
            w[10:18] += 0.50
    else:
        w[:] = 0.20
        w[7:10] += 1.10
        w[10:16] += 0.90
        w[17:23] += 1.90
        if is_weekend:
            w[10:22] += 0.40

    return w / w.sum()


def base_daily_sessions(charging_type: str, power_kw: float, connector_count: int) -> float:
    connector_factor = 0.75 + min(connector_count / 6.0, 0.50)
    power_factor = 0.75 + min(power_kw / 300.0, 0.80)

    if charging_type == "Fast Charger":
        base = 1.40
    else:
        base = 0.23

    return base * connector_factor * power_factor


def sample_vehicle_type(charging_type: str, rng: np.random.Generator) -> str:
    probs = VEHICLE_TYPE_PROBS[charging_type]
    values = list(probs.keys())
    weights = np.array(list(probs.values()), dtype=float)
    return str(rng.choice(values, p=weights / weights.sum()))


def sample_payment_type(charging_type: str, rng: np.random.Generator) -> str:
    probs = PAYMENT_TYPE_PROBS[charging_type]
    values = list(probs.keys())
    weights = np.array(list(probs.values()), dtype=float)
    return str(rng.choice(values, p=weights / weights.sum()))


def sample_duration_minutes(charging_type: str, power_kw: float, rng: np.random.Generator) -> int:
    if charging_type == "Fast Charger":
        if power_kw >= 250:
            mean, std, low, high = 24, 8, 8, 60
        elif power_kw >= 100:
            mean, std, low, high = 32, 10, 10, 75
        else:
            mean, std, low, high = 42, 12, 12, 90
    else:
        if power_kw >= 44:
            mean, std, low, high = 70, 25, 20, 200
        elif power_kw >= 22:
            mean, std, low, high = 105, 35, 30, 300
        else:
            mean, std, low, high = 150, 50, 40, 480

    value = int(rng.normal(mean, std))
    return max(low, min(value, high))


def energy_multiplier_for_vehicle(vehicle_type: str) -> float:
    return {
        "compact_ev": 0.78,
        "suv_ev": 0.88,
        "premium_ev": 0.95,
        "fleet_van": 1.00,
        "phev": 0.45,
    }[vehicle_type]


def sample_energy_kwh(
    charging_type: str,
    power_kw: float,
    duration_min: int,
    vehicle_type: str,
    status: str,
    rng: np.random.Generator,
) -> float:
    if charging_type == "Fast Charger":
        utilization = rng.uniform(0.45, 0.78)
    else:
        utilization = rng.uniform(0.18, 0.45)

    energy = power_kw * (duration_min / 60.0) * utilization * energy_multiplier_for_vehicle(vehicle_type)
    energy *= rng.uniform(0.93, 1.07)

    if status == "failed":
        energy *= rng.uniform(0.00, 0.30)
    elif status == "aborted":
        energy *= rng.uniform(0.10, 0.55)

    return round(max(0.5, min(energy, 220.0)), 2)


def sample_queue_wait(
    charging_type: str,
    hour: int,
    connector_count: int,
    daily_sessions: int,
    rng: np.random.Generator,
) -> int:
    if charging_type == "Fast Charger":
        peak = hour in range(7, 10) or hour in range(16, 21)
    else:
        peak = hour in range(17, 23) or hour in range(8, 10)

    load_ratio = daily_sessions / max(connector_count * 2.5, 1)

    if not peak:
        if rng.random() < 0.86:
            return 0
        return int(min(rng.poisson(2), 12))

    base = max(0.0, (load_ratio - 0.85) * 6.0)
    if connector_count <= 2:
        base += 2.0
    if charging_type == "Fast Charger":
        base += 1.0

    return int(min(rng.poisson(max(0.8, base)), 45))


def sample_status(queue_wait: int, charging_type: str, rng: np.random.Generator) -> str:
    completed = 0.952
    failed = 0.020
    aborted = 0.028

    if queue_wait >= 10:
        completed -= 0.05
        failed += 0.02
        aborted += 0.03

    if charging_type == "Fast Charger":
        completed -= 0.01
        failed += 0.005
        aborted += 0.005

    probs = np.array([completed, failed, aborted], dtype=float)
    probs = probs / probs.sum()
    return str(rng.choice(["completed", "failed", "aborted"], p=probs))


def price_per_kwh(charging_type: str, hour: int, power_kw: float) -> float:
    if charging_type == "Fast Charger":
        base = 0.64
        if power_kw >= 250:
            base += 0.08
        elif power_kw >= 100:
            base += 0.04
    else:
        base = 0.38
        if power_kw <= 11:
            base -= 0.05

    if hour in [7, 8, 9, 17, 18, 19, 20]:
        base += 0.06
    elif hour in [0, 1, 2, 3, 4, 5]:
        base -= 0.04

    return round(max(0.19, base), 2)


def generate_sessions_for_month(
    month_start: date,
    stations_df: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    last_day = monthrange(month_start.year, month_start.month)[1]
    month_end = date(month_start.year, month_start.month, last_day)

    rows: List[dict] = []
    seq = 1

    for current_day in date_iter(month_start, month_end):
        weekday_idx = current_day.weekday()
        seasonal_factor = SEASONAL_MONTH_FACTOR.get(current_day.month, 1.0)

        for station in stations_df.itertuples(index=False):
            city = station.city
            charging_type = station.charging_type
            power_kw = float(station.power_kw)
            connector_count = int(max(station.connector_count, 1))

            lam = (
                base_daily_sessions(charging_type, power_kw, connector_count)
                * CITY_DEMAND_FACTOR.get(city, 1.0)
                * weekday_factor(charging_type, weekday_idx)
                * seasonal_factor
            )

            daily_sessions = int(min(rng.poisson(max(lam, 0.05)), 30))
            if daily_sessions == 0:
                continue

            hour_prob = hour_weights(charging_type, weekday_idx)

            for _ in range(daily_sessions):
                hour = int(rng.choice(np.arange(24), p=hour_prob))
                minute = int(rng.integers(0, 60))
                second = int(rng.integers(0, 60))

                start_ts = datetime(
                    current_day.year,
                    current_day.month,
                    current_day.day,
                    hour,
                    minute,
                    second,
                )

                duration_min = sample_duration_minutes(charging_type, power_kw, rng)
                queue_wait_min = sample_queue_wait(charging_type, hour, connector_count, daily_sessions, rng)
                status = sample_status(queue_wait_min, charging_type, rng)
                vehicle_type = sample_vehicle_type(charging_type, rng)
                payment_type = sample_payment_type(charging_type, rng)

                adjusted_duration = duration_min
                if status == "failed":
                    adjusted_duration = max(5, int(duration_min * rng.uniform(0.12, 0.45)))
                elif status == "aborted":
                    adjusted_duration = max(8, int(duration_min * rng.uniform(0.20, 0.65)))

                end_ts = start_ts + timedelta(minutes=adjusted_duration)

                energy_kwh = sample_energy_kwh(
                    charging_type=charging_type,
                    power_kw=power_kw,
                    duration_min=adjusted_duration,
                    vehicle_type=vehicle_type,
                    status=status,
                    rng=rng,
                )

                unit_price = price_per_kwh(charging_type, hour, power_kw)
                estimated_revenue = round(energy_kwh * unit_price, 2)

                rows.append(
                    {
                        "session_id": f"SES-{month_start.strftime('%Y%m')}-{seq:08d}",
                        "session_start_ts": start_ts.isoformat(sep=" "),
                        "session_end_ts": end_ts.isoformat(sep=" "),
                        "station_id": str(station.station_id),
                        "vehicle_type": vehicle_type,
                        "energy_kwh": energy_kwh,
                        "session_status": status,
                        "payment_type": payment_type,
                        "queue_wait_min": queue_wait_min,
                        "price_per_kwh": unit_price,
                        "estimated_revenue_eur": estimated_revenue,
                    }
                )
                seq += 1

    if not rows:
        return pd.DataFrame(
            columns=[
                "session_id",
                "session_start_ts",
                "session_end_ts",
                "station_id",
                "vehicle_type",
                "energy_kwh",
                "session_status",
                "payment_type",
                "queue_wait_min",
                "price_per_kwh",
                "estimated_revenue_eur",
            ]
        )

    return pd.DataFrame(rows).sort_values(["session_start_ts", "station_id"]).reset_index(drop=True)


def main() -> None:
    args = parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)
    stations_df = prepare_station_master(args.stations_file, args.station_sample_frac, rng)

    starts = month_starts(args.start_month, args.months)
    monthly_counts: List[Tuple[str, int]] = []

    for month_start in starts:
        sessions_df = generate_sessions_for_month(month_start, stations_df, rng)
        file_name = f"charge_sessions_{month_start.strftime('%Y_%m')}.csv"
        sessions_df.to_csv(output_dir / file_name, index=False)
        monthly_counts.append((file_name, len(sessions_df)))

    print(f"Using stations: {len(stations_df)}")
    print(f"Output directory: {output_dir.resolve()}")
    for file_name, row_count in monthly_counts:
        print(f"{file_name}: {row_count} rows")


if __name__ == "__main__":
    main()