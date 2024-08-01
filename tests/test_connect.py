from wherobots.db import connect
from wherobots.db.constants import STAGING_ENDPOINT
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime
import logging

logging.basicConfig(level=logging.DEBUG)

with connect(
        host=STAGING_ENDPOINT,
        api_key=staging_dbapi_key,
        # api_key=dbapi_key,
        runtime=Runtime.SAN_FRANCISCO,
        region=Region.AWS_US_WEST_2,
        # ws_url='wss://wbc.cloud.wherobots.com/sql/nnla3muxj9/ujwk78awwu949l',
        # ws_url='wss://wbc.staging.wherobots.com/sql/hkny39iypq/6knlwynt3kdddm'
) as conn:
    curr = conn.cursor()

    print("\n\nExecuting SHOW SCHEMAS IN wherobots_open_data")
    curr.execute("SHOW SCHEMAS IN wherobots_open_data")
    # curr.execute("SELECT categories AS categories, COUNT(*) AS count FROM overture.places_place GROUP BY categories LIMIT 10")
    results = curr.fetchall()
    print("\n\n\n")
    print(results)
    print("\n\n\n")
    #
    curr.execute("SHOW TABLES IN wherobots_open_data.overture_2024_05_16")
    # curr.execute("SELECT categories AS categories, COUNT(*) AS count FROM overture.places_place GROUP BY categories LIMIT 10")
    results = curr.fetchall()
    print("\n\n\n")
    print(results)
    print("\n\n\n")

    # curr.execute("SELECT * FROM wherobots_open_data.overture_2024_07_22.buildings_building")
    # curr.execute("SELECT geojson2 AS geojson2 FROM ( SELECT ST_ASGEOJSON(geometry, 'featureCollection') AS geojson2, geometry FROM wherobots_open_data.overture_2024_07_22.buildings_building ) AS virtual_table  LIMIT 5")
    results = curr.fetchall()
    print("\n\n\n")
    for row in results:
        print(row)
    # print(results)
    print("\n\n\n")