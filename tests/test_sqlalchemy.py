import os
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import URL

from wherobots.db.wherobots_sqlalchemy import WherobotsDialect
from sqlalchemy.dialects import registry


# Get all registered dialects
dialects = registry.impls

print("Registered SQLAlchemy Dialects:")
for name, dialect in dialects.items():
    print(f"{name}: {dialect}")


# Define connection details
host = 'localhost'
runtime = 'SAN_FRANCISCO'
region = 'AWS_US_WEST_2'
ws_url = 'wss://wbc.cloud.wherobots.com/sql/nnla3muxj9/dv7gcfijtwa3r1'
staging_ws_url='wss://wbc.staging.wherobots.com/sql/hkny39iypq/dqdiqr7kje5qdf'
catalog = "wherobots_pro_data"

# Construct the database URL
db_url = URL.create(
    drivername="Wherobots",
    username=staging_dbapi_key,
    host=host,
    query={
        "runtime": runtime,
        "region": region,
        "ws_url": staging_ws_url,
        # "catalog": catalog
    }

)
print(db_url)

# Create an engine
engine = create_engine(db_url, echo=True)

# Test the connection
with engine.connect() as connection:
    result = connection.execute('SHOW TABLES IN wherobots_open_data.overture')
    # result = connection.execute('SELECT categories AS categories, COUNT(*) AS count FROM wherobots_open_data.overture.places_place GROUP BY categories LIMIT 10')
    for row in result:
        print("\n\n\n", row)

    # result = connection.execute('SHOW TABLES IN overture')
    # result = connection.execute("SELECT geojson2 AS geojson2 FROM ( SELECT ST_ASGEOJSON(geometry) AS geojson, ST_ASGEOJSON(geometry, 'featureCollection') AS geojson2, geometry FROM wherobots_open_data.overture_2024_07_22.buildings_building ) AS virtual_table LIMIT 5")
    # print("\n\n\nresult - ", result)
    # for row in result:
    #     print("\n\n\n", row)

    # dialect = WherobotsDialect()
    # schemas = dialect.get_schema_names(connection)
    # print("\n\n\nSchemas:", schemas)

import re

# normalized_query = "SELECT geojson2 AS geojson2 FROM ( SELECT ST_ASGEOJSON(geometry) AS geojson, ST_ASGEOJSON(geometry, 'featureCollection') AS geojson2, geometry FROM wherobots_open_data.overture_2024_07_22.buildings_building ) AS virtual_table GROUP BY geojson2 LIMIT 50"

# print("\nnormalized_query: %s\n", normalized_query)
#
# # Extract aliases for ST_AsGeoJSON with any parameters in the SELECT statement
# select_clause = re.search(r"SELECT (.*) FROM", normalized_query, re.IGNORECASE)
# if select_clause:
#     select_fields = select_clause.group(1)
#     geojson_aliases = re.findall(r"ST_AsGeoJSON\([^\)]*\)(?:\s+AS\s+(\w+))?", select_fields, re.IGNORECASE)
#     print("\nFound geojson aliases: %s\n", geojson_aliases)
#
#     # Add the original function name with any parameters to the list of aliases
#     geojson_aliases.append(r"ST_AsGeoJSON\([^\)]*\)")
#
#     # Construct regex to remove GROUP BY clauses for these aliases
#     for alias in geojson_aliases:
#         if alias:
#             # Ensure we match the alias exactly, followed by space or end of string
#             regex = re.compile(rf"GROUP BY\s+{alias}(?=\s|$)", re.IGNORECASE)
#             normalized_query = re.sub(regex, "", normalized_query)
#
# print("\nModified query after removing GROUP BY on ST_AsGeoJSON aliases: %s\n", normalized_query)



print("\n\n",db_url)