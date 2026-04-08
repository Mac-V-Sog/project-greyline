from pathlib import Path

from app.profiler import profile_bytes


def test_profile_csv_detects_fields_and_fingerprint():
    data = Path("examples/telecom_cdr.csv").read_bytes()
    profile = profile_bytes(data, "telecom_cdr.csv", "telecom_x")
    assert profile.file_format == "csv"
    assert profile.row_count_sampled == 3
    assert len(profile.fields) == 5
    assert profile.shape_fingerprint
