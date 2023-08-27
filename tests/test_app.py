from src.Module.app import App
from src.constants.constants import FIFA23_OFFICIAL_DATA


def test_do():
    with App() as app:
        assert app.do() is None


def test_get_top_5_countries():
    with App() as app:
        fifa_official = app.read_csv(FIFA23_OFFICIAL_DATA)
        df = app.get_top_5_countries(fifa_official)
    expected_list = [('England', 1531), ('Germany', 1038), ('Spain', 990), ('France', 864), ('Argentina', 843)]
    actual_list = df.rdd.map(tuple).collect()
    assert expected_list == actual_list
