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
api_key = '629324d9-418c-4c4b-bfde-7a130cffb512'
staging_dbapi_key = 'f64f972f-c4eb-4b2d-bb3e-96838ebba4d9'
runtime = 'SAN_FRANCISCO'
region = 'AWS_US_WEST_2'
ws_url = 'wss://wbc.cloud.wherobots.com/sql/nnla3muxj9/dv7gcfijtwa3r1'
staging_ws_url='wss://wbc.staging.wherobots.com/sql/hkny39iypq/ahxb5l893vbub2'
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
    result = connection.execute("SELECT COUNT(*) AS `COUNT(*)` FROM wherobots_open_data.overture.places_place LIMIT 10")
    for row in result:
        print("\n\n\n", row)

    dialect = WherobotsDialect()
    schemas = dialect.get_schema_names(connection)
    print("\n\n\nSchemas:", schemas)

print(db_url)