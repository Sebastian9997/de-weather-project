import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from weather_utils import (  # noqa: E402
    EXPECTED_CITIES,
    bronze_object_key,
    build_daily_frame,
    payload_to_record,
    source_date_from_context,
)


def sample_payload(city: str, timestamp: int = 1_700_000_000) -> dict:
    return {
        "city": city,
        "lat": 52.0,
        "lon": 21.0,
        "current": {
            "temp": 20.5,
            "humidity": 55,
            "wind_speed": 3.2,
            "dt": timestamp,
            "weather": [{"description": "clear sky"}],
        },
    }


class WeatherUtilsTests(unittest.TestCase):
    def test_bronze_key_is_stable_and_uses_utc_hour(self):
        logical_hour = datetime(2026, 8, 22, 14, 37, tzinfo=timezone.utc)
        self.assertEqual(
            bronze_object_key("Warsaw", logical_hour),
            "bronze/warsaw/warsaw_20260822T140000Z.json",
        )

    def test_source_date_prefers_trigger_configuration(self):
        self.assertEqual(
            source_date_from_context({"source_ds_nodash": "20260822"}, "20260821"),
            "20260822",
        )

    def test_source_date_rejects_invalid_values(self):
        with self.assertRaisesRegex(ValueError, "Invalid source date"):
            source_date_from_context({"source_ds_nodash": "2026-08-22"}, "20260821")

    def test_payload_validation_rejects_missing_current_data(self):
        with self.assertRaisesRegex(ValueError, "missing city or current"):
            payload_to_record({"city": "Warsaw"})

    def test_daily_frame_requires_every_city_and_deduplicates(self):
        records = [payload_to_record(sample_payload(city)) for city in EXPECTED_CITIES]
        records.append(payload_to_record(sample_payload("Warsaw")))
        frame = build_daily_frame(records)
        self.assertEqual(len(frame), len(EXPECTED_CITIES))
        self.assertEqual(set(frame["city"]), set(EXPECTED_CITIES))
        self.assertEqual(str(frame["extraction_time"].dtype), "datetime64[us]")

    def test_daily_frame_rejects_partial_snapshot(self):
        records = [payload_to_record(sample_payload("Warsaw"))]
        with self.assertRaisesRegex(ValueError, "missing cities"):
            build_daily_frame(records)


if __name__ == "__main__":
    unittest.main()

