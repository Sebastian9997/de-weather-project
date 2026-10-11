"""Pure helpers shared by the weather DAGs and their unit tests."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterable, Mapping, Any

import pandas as pd


EXPECTED_CITIES = (
    "Warsaw",
    "Berlin",
    "Paris",
    "London",
    "Madrid",
    "Rome",
    "Prague",
    "Vienna",
)


def source_date_from_context(conf: Mapping[str, Any] | None, fallback: str) -> str:
    """Return a validated YYYYMMDD processing date."""
    source_date = str((conf or {}).get("source_ds_nodash", fallback))
    if not re.fullmatch(r"\d{8}", source_date):
        raise ValueError(f"Invalid source date: {source_date!r}")
    return source_date


def bronze_object_key(city: str, logical_hour: datetime) -> str:
    """Build an idempotent object key for a city and an Airflow logical hour."""
    if logical_hour.tzinfo is None:
        logical_hour = logical_hour.replace(tzinfo=timezone.utc)
    timestamp = logical_hour.astimezone(timezone.utc).strftime("%Y%m%dT%H0000Z")
    city_slug = city.lower().replace(" ", "_")
    return f"bronze/{city_slug}/{city_slug}_{timestamp}.json"


def payload_to_record(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and flatten one OpenWeather payload."""
    city = payload.get("city")
    current = payload.get("current")
    if not city or not isinstance(current, Mapping):
        raise ValueError("Payload is missing city or current weather data")

    weather = current.get("weather")
    if not isinstance(weather, list) or not weather or not isinstance(weather[0], Mapping):
        raise ValueError(f"Payload for {city} is missing weather description")

    required_current = ("temp", "humidity", "wind_speed", "dt")
    missing = [field for field in required_current if current.get(field) is None]
    if missing:
        raise ValueError(f"Payload for {city} is missing fields: {', '.join(missing)}")

    if payload.get("lat") is None or payload.get("lon") is None:
        raise ValueError(f"Payload for {city} is missing coordinates")

    extraction_time = pd.to_datetime(current["dt"], unit="s", utc=True).tz_localize(None)
    return {
        "city": str(city),
        "temp": current["temp"],
        "humidity": current["humidity"],
        "wind_speed": current["wind_speed"],
        "weather_desc": weather[0].get("description"),
        "lat": payload["lat"],
        "lon": payload["lon"],
        "extraction_time": extraction_time,
    }


def build_daily_frame(records: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """Create a complete, deterministic daily frame from flattened records."""
    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No valid weather records were produced")

    missing_cities = sorted(set(EXPECTED_CITIES) - set(frame["city"].dropna()))
    if missing_cities:
        raise ValueError(f"Daily data is missing cities: {', '.join(missing_cities)}")

    frame = (
        frame.drop_duplicates(subset=["city", "extraction_time"], keep="last")
        .sort_values(["extraction_time", "city"])
        .reset_index(drop=True)
    )
    frame["extraction_time"] = frame["extraction_time"].astype("datetime64[us]")
    return frame

