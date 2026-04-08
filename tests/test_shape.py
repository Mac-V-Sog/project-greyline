from app.shape import fingerprint_fields


def test_fingerprint_deterministic():
    items = [("A_NO", ("str",)), ("B_NO", ("str",))]
    fp1 = fingerprint_fields(items)
    fp2 = fingerprint_fields(items)
    assert fp1 == fp2


def test_fingerprint_differs_for_different_inputs():
    items1 = [("A_NO", ("str",)), ("B_NO", ("str",))]
    items2 = [("A_NO", ("str",)), ("C_NO", ("str",))]
    assert fingerprint_fields(items1) != fingerprint_fields(items2)


def test_fingerprint_ignores_type_order():
    items1 = [("A_NO", ("str", "int"))]
    items2 = [("A_NO", ("int", "str"))]
    assert fingerprint_fields(items1) == fingerprint_fields(items2)


def test_fingerprint_length():
    items = [("A", ("str",))]
    fp = fingerprint_fields(items)
    assert len(fp) == 16  # first 16 hex chars of sha256


def test_fingerprint_empty():
    fp = fingerprint_fields([])
    assert isinstance(fp, str)
    assert len(fp) == 16
