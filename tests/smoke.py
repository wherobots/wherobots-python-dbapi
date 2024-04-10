# A simple smoke test for the DB driver.

import argparse
import logging
import shapely
import sys
import yaml

from wherobots.db import connect, connect_direct
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region


def execute(conn):
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
             categories.main AS category,
             names.common[0].value AS name,
             ST_AsEWKT(geometry) AS wkt_geometry
           FROM
             wherobots_open_data.overture.places_place
        """
    )
    results = cursor.fetchall()
    for row in results:
        for key, value in row.items():
            if key == "wkt_geometry":
                row[key] = shapely.to_geojson(shapely.from_wkt(value))
    print(yaml.dump(results, indent=2, allow_unicode=True))


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
    parser.add_argument("direct_url", help="Direct URL to connect to", nargs="?")
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=args.debug)
    logging.getLogger("websockets.protocol").setLevel(args.debug)

    api_key = None
    if args.api_key_file:
        with open(args.api_key_file) as f:
            api_key = f.read().strip()

    token = None
    if args.token_file:
        with open(args.token_file) as f:
            token = f.read().strip()

    if args.direct_url:
        headers = (
            {"Authorization": f"Bearer {token}"} if token else {"X-API-Key": api_key}
        )
        with connect_direct(uri=args.direct_url, headers=headers) as conn:
            execute(conn)
    else:
        with connect(
            host="api.staging.wherobots.services",
            token=token,
            api_key=api_key,
            runtime=Runtime.SEDONA,
            region=Region.AWS_US_WEST_2,
            wait_timeout_seconds=900,
        ) as conn:
            execute(conn)
