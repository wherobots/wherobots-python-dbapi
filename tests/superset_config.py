# import sys
# import os
# from sqlalchemy import create_engine, text
# from sqlalchemy.engine.url import URL
# from flask_appbuilder.security.manager import AUTH_DB

print("\n\n\n     Running superset.config.py\n\n\n")

# # SQLite configuration for Superset metadata storage
# SQLALCHEMY_DATABASE_URI = 'sqlite:////Users/pranavtoggi/Documents/test-dbapi/superset/superset.db?check_same_thread=false'
#
# # Define a function to create a connection to WherobotsDB for testing
# def get_wherobots_connection():
#     wherobots_db_uri = URL.create(
#         drivername='wherobots1',
#         username='70f7e686-66c9-4da2-b54b-ff053b3119c9',
#         host='localhost',
#         database='default',
#         query={
#             'runtime': 'SEDONA',
#             'region': 'AWS_US_WEST_2',
#             'connect_timeout': '300',
#         }
#     )
#     engine = create_engine(wherobots_db_uri)
#     return engine.connect()
#
# # Secret key for Superset
# SECRET_KEY = 'v6Ch5QH1FHVBxuH6ghiKlsh68S4BWwr0WMQZ9e7f3oCtwaHm4DfCJSP9'
#
# # Flask AppBuilder configuration
# AUTH_TYPE = AUTH_DB
#
# # Additional configurations
# SQLALCHEMY_TRACK_MODIFICATIONS = True
# SUPERSET_WEBSERVER_TIMEOUT = 300
# BASE_URL = 'http://localhost:8088'
# WTF_CSRF_ENABLED = True
# WTF_CSRF_EXEMPT_LIST = []
# CACHE_CONFIG = {
#     'CACHE_TYPE': 'SimpleCache',
#     'CACHE_DEFAULT_TIMEOUT': 300
# }

# Optional: Define a separate Superset DB URI for the wherobots connection to test
WHEROBOTS_DATABASE_URI = 'wherobots://70f7e686-66c9-4da2-b54b-ff053b3119c9@localhost/default?region=AWS_US_WEST_2&runtime=SEDONA'

# # Ensure the connection works before starting Superset
# try:
#     with get_wherobots_connection() as conn:
#         print("Connection to WherobotsDB successful!")
# except Exception as e:
#     print(f"Failed to connect to WherobotsDB: {e}")
#
# # Setting the SQLAlchemy engine options for longer wait times
# SQLALCHEMY_ENGINE_OPTIONS = {
#     "pool_pre_ping": True,
#     "pool_recycle": 1800,  # 30 minutes
#     "connect_args": {
#         "connect_timeout": 300
#     }
# }

import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from flask_appbuilder.security.manager import AUTH_DB

print("\n\n\n     Running superset.config.py\n\n\n")

# SQLite configuration for Superset metadata storage
SQLALCHEMY_DATABASE_URI = 'sqlite:////Users/pranavtoggi/Documents/test-dbapi/superset/superset.db?check_same_thread=false'

# Define a function to create a connection to WherobotsDB for testing
def get_wherobots_connection():
    wherobots_db_uri = URL.create(
        drivername='wherobots',
        username='70f7e686-66c9-4da2-b54b-ff053b3119c9',
        host='localhost',
        database='default',
        query={
            'runtime': 'SEDONA',
            'region': 'AWS_US_WEST_2',
        }
    )
    engine = create_engine(wherobots_db_uri)
    return engine.connect()

# Secret key for Superset
SECRET_KEY = 'v6Ch5QH1FHVBxuH6ghiKlsh68S4BWwr0WMQZ9e7f3oCtwaHm4DfCJSP9'

# Flask AppBuilder configuration
AUTH_TYPE = AUTH_DB

DEBUG = True

# Additional configurations
SQLALCHEMY_TRACK_MODIFICATIONS = True
SUPERSET_WEBSERVER_TIMEOUT = 300
BASE_URL = 'http://localhost:8088'
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# Ensure the connection works before starting Superset
# try:
#     with get_wherobots_connection() as conn:
#         print("Connection to WherobotsDB successful!")
# except Exception as e:
#     print(f"Failed to connect to WherobotsDB: {e}")

# Setting the SQLAlchemy engine options for longer wait times
SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_recycle": 1800,  # 30 minutes
}
