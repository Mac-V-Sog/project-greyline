from io import BytesIO

from app.profiler import profile_fileobj


def test_profile_fileobj_csv_stops_after_sample_rows():
    header = b"A_NO,B_NO,START,DUR,CGI\n"
    row = b"447700900111,447700900222,2026-04-02 14:03:11,62,234-15-90123\n"
    data = header + row * 2000
    profile = profile_fileobj(BytesIO(data), "telecom_cdr.csv", "telecom_x", sample_rows=50)
    assert profile.file_format == "csv"
    assert profile.row_count_sampled == 50
    assert len(profile.fields) == 5
    assert profile.delimiter == ","


def test_profile_fileobj_jsonl_stops_after_sample_rows():
    lines = (b'{"device":"abc","lat":58.97,"lon":5.73,"captured_at":"2026-03-11T14:22:41Z"}\n') * 500
    profile = profile_fileobj(BytesIO(lines), "geo.jsonl", "geo_src", sample_rows=25)
    assert profile.file_format == "jsonl"
    assert profile.row_count_sampled == 25
    assert any(field.name == "lat" for field in profile.fields)
