import livedocs


def test_one():
    livedocs.query("select * from livedocs.livedocs.test_table limit 10", "bigquery")
