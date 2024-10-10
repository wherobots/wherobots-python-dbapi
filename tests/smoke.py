# A simple smoke test for the DB driver.

import argparse
import functools
import logging
import sys

import pandas
from rich.console import Console
from rich.table import Table

from wherobots.db import connect, connect_direct
from wherobots.db.constants import DEFAULT_ENDPOINT
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key-file", help="File containing the API key")
    parser.add_argument("--token-file", help="File containing the token")
    parser.add_argument(
        "--debug",
        help="Enable debug logging",
        action="store_const",
        const=logging.DEBUG,
        default=logging.INFO,
    )
    parser.add_argument(
        "--api-endpoint",
        help="Wherobots API endpoint to request a SQL session from",
        default=DEFAULT_ENDPOINT,
    )
    parser.add_argument("--ws-url", help="Direct URL to connect to")
    parser.add_argument(
        "--shutdown-after-inactive-seconds",
        help="Request a specific SQL Session expiration timeout (in seconds)",
    )
    parser.add_argument(
        "--wide", help="Enable wide output", action="store_const", const=80, default=30
    )
    parser.add_argument("sql", nargs="+", help="SQL query to execute")
    args = parser.parse_args()

    logging.basicConfig(
        stream=sys.stdout,
        level=args.debug,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)20s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("websockets.protocol").setLevel(args.debug)

    api_key = None
    token = None
    headers = None

    if args.api_key_file:
        with open(args.api_key_file) as f:
            api_key = f.read().strip()
        headers = {"X-API-Key": api_key}

    if args.token_file:
        with open(args.token_file) as f:
            token = f.read().strip()
        headers = {"Authorization": f"Bearer {token}"}

    if args.ws_url:
        conn_func = functools.partial(connect_direct, uri=args.ws_url, headers=headers)
    else:
        conn_func = functools.partial(
            connect,
            host=args.api_endpoint,
            token=token,
            api_key=api_key,
            shutdown_after_inactive_seconds=args.shutdown_after_inactive_seconds,
            wait_timeout=900,
        )

    def render(results: pandas.DataFrame):
        table = Table()
        table.add_column("#")
        for column in results.columns:
            table.add_column(column, max_width=args.wide, no_wrap=True)
        for row in results.itertuples(name=None):
            r = [str(x) for x in row]
            table.add_row(*r)
        Console().print(table)

    with conn_func() as conn:
        for sql in args.sql:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                render(cursor.fetchall())
