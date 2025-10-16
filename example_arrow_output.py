#!/usr/bin/env python3
"""
Example script demonstrating Arrow table output functionality.

This script shows how to use the new output_format parameter to return 
PyArrow tables instead of pandas DataFrames, which is particularly useful
for working with GeoArrow geometries.
"""

from wherobots.db import connect
from wherobots.db.constants import ResultsFormat, OutputFormat, GeometryRepresentation
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region

def example_arrow_usage():
    """
    Example of how to use the new Arrow output functionality.
    
    Note: This is a code example only - it would need valid credentials
    to actually run against a Wherobots DB instance.
    """
    
    # Example 1: Return Arrow tables instead of pandas DataFrames
    with connect(
        host="api.cloud.wherobots.com",
        api_key="your_api_key",
        runtime=Runtime.TINY,
        results_format=ResultsFormat.ARROW,  # Efficient wire format
        output_format=OutputFormat.ARROW,    # Return Arrow tables
        geometry_representation=GeometryRepresentation.WKB,
        region=Region.AWS_US_WEST_2
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM buildings LIMIT 1000")
        results = cursor.fetchall()
        
        # results is now a pyarrow.Table instead of pandas.DataFrame
        print(f"Result type: {type(results)}")
        print(f"Schema: {results.schema}")
        print(f"Row count: {len(results)}")
        
        # Work with Arrow table directly (great for GeoArrow!)
        # Convert to pandas only when needed:
        # df = results.to_pandas()
    
    # Example 2: Default behavior (backwards compatible)
    with connect(
        host="api.cloud.wherobots.com",
        api_key="your_api_key",
        runtime=Runtime.TINY,
        results_format=ResultsFormat.ARROW,
        # output_format defaults to OutputFormat.PANDAS
        geometry_representation=GeometryRepresentation.WKB,
        region=Region.AWS_US_WEST_2
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM buildings LIMIT 1000")
        results = cursor.fetchall()
        
        # results is a pandas.DataFrame (existing behavior)
        print(f"Result type: {type(results)}")

if __name__ == "__main__":
    print("Arrow Table Output Example")
    print("=" * 30)
    print("This example shows how to use the new output_format parameter.")
    print("Uncomment and provide valid credentials to run against Wherobots DB.")
    print()
    print("Key benefits of Arrow output:")
    print("- More efficient for large datasets")
    print("- Native support for GeoArrow geometries")
    print("- Better interoperability with Arrow ecosystem")
    print("- Zero-copy operations when possible")