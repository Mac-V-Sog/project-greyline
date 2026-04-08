from app.patterns import looks_like_phone, looks_like_datetime, PHONE_RE, CELL_ID_RE, DATETIME_FORMATS


def test_phone_re_matches_international():
    assert PHONE_RE.match("+4712345678")


def test_phone_re_matches_plain_digits():
    assert PHONE_RE.match("4712345678")


def test_phone_re_rejects_short():
    assert not PHONE_RE.match("123")


def test_phone_re_rejects_alpha():
    assert not PHONE_RE.match("47123456abc")


def test_cell_id_re_matches_alphanumeric():
    assert CELL_ID_RE.match("234-15-90123")


def test_cell_id_re_rejects_short():
    assert not CELL_ID_RE.match("ab")


def test_cell_id_re_rejects_special_chars():
    assert not CELL_ID_RE.match("abc!@#")


def test_datetime_formats_is_list():
    assert isinstance(DATETIME_FORMATS, list)
    assert len(DATETIME_FORMATS) >= 5


def test_looks_like_phone_with_spaces():
    assert looks_like_phone("+47 123 45 678")


def test_looks_like_phone_with_parens():
    assert looks_like_phone("(471) 234-5678")


def test_looks_like_phone_plain():
    assert looks_like_phone("4712345678")


def test_looks_like_phone_rejects_text():
    assert not looks_like_phone("hello")


def test_looks_like_datetime_iso():
    assert looks_like_datetime("2026-04-02 14:03:11")


def test_looks_like_datetime_iso_t():
    assert looks_like_datetime("2026-04-02T14:03:11Z")


def test_looks_like_datetime_eu():
    assert looks_like_datetime("02/04/2026 14:03")


def test_looks_like_datetime_unix_ts():
    assert looks_like_datetime("1743596591")


def test_looks_like_datetime_rejects_text():
    assert not looks_like_datetime("banana")


def test_looks_like_datetime_rejects_short_digit():
    assert not looks_like_datetime("12345")
