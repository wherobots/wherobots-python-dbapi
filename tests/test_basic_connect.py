from wherobots.db import Connection


def test_basic_connect(wbc):
    conn: Connection = wbc.conn_or_skip()

    with conn.cursor() as curr:
        curr.execute("SHOW SCHEMAS IN wherobots_open_data")
        all_schemas = curr.fetchall()
        assert len(all_schemas) > 0
