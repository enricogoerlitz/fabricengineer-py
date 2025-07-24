from fabricengineer import hello


def test_init():
    assert isinstance(hello(), str)
