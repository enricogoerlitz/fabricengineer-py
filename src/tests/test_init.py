from fabricengineer import hello, print_spark_version


def test_init():
    assert isinstance(hello(), str)
