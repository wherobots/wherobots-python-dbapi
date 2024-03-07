# A simple smoke test for the DB driver.

import logging
import sys

from wherobots.db import connect
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write(f"usage: {sys.argv[0]} <api-key-file>\n")
        sys.exit(1)

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    with open(sys.argv[1]) as f:
        api_key = f.read().strip()

    with connect(
        api_key=api_key, runtime=Runtime.NEW_YORK, region=Region.AWS_US_WEST_2
    ) as conn:
        pass
