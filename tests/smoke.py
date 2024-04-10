# A simple smoke test for the DB driver.

import argparse
import functools
import logging
from contextlib import closing

import shapely
import sys
import tabulate

from wherobots.db import connect, connect_direct
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region


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
    parser.add_argument("--ws-url", help="Direct URL to connect to")
    parser.add_argument("sql", help="SQL query to execute")
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=args.debug)
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
            host="api.staging.wherobots.services",
            token=token,
            api_key=api_key,
            runtime=Runtime.SEDONA,
            region=Region.AWS_US_WEST_2,
            wait_timeout_seconds=900,
        )

    with conn_func() as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(args.sql)
            results = cursor.fetchall()

    for row in results:
        for key, value in row.items():
            if "geometry" in key:
                row[key] = shapely.to_geojson(shapely.from_wkt(value))

    print(tabulate.tabulate(results, headers="keys", tablefmt="rounded_outline"))
