from wherobots.db import connect
from wherobots.db.constants import STAGING_ENDPOINT
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime
import logging

logging.basicConfig(level=logging.DEBUG)

dbapi_key = '629324d9-418c-4c4b-bfde-7a130cffb512'
staging_dbapi_key = 'f64f972f-c4eb-4b2d-bb3e-96838ebba4d9'

with connect(
        host=STAGING_ENDPOINT,
        api_key=staging_dbapi_key,
        runtime=Runtime.SAN_FRANCISCO,
        region=Region.AWS_US_WEST_2,
        # ws_url='wss://wbc.cloud.wherobots.com/sql/nnla3muxj9/ujwk78awwu949l',
        ws_url='wss://wbc.staging.wherobots.com/sql/hkny39iypq/ahxb5l893vbub2'
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