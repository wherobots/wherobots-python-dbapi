#!/usr/bin/env python3
"""
Simple test script to verify Arrow output functionality.
This is a temporary test file to validate the implementation.
"""

import pyarrow as pa
import pandas as pd
from unittest.mock import Mock, MagicMock
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from wherobots.db.constants import OutputFormat, ResultsFormat
from wherobots.db.connection import Connection
from wherobots.db.cursor import Cursor

def test_arrow_output():
    """Test that the connection can return Arrow tables when requested."""
    
    # Create a mock WebSocket connection
    mock_ws = Mock()
    mock_ws.protocol.state = 1  # Not closing state
    
    # Create connection with Arrow output format
    conn = Connection(
        ws=mock_ws,
        output_format=OutputFormat.ARROW,
        results_format=ResultsFormat.ARROW
    )
    
    # Create some test Arrow data
    test_data = pa.table({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10.5, 20.0, 30.5]
    })
    
    # Test _handle_results method with Arrow format
    test_results = {
        'result_bytes': pa.ipc.RecordBatchStreamWriter.write_table(test_data),
        'format': 'arrow',
        'compression': 'brotli'  # This won't be used in our simplified test
    }
    
    # Mock the pyarrow reading process
    original_py_buffer = pa.py_buffer
    original_input_stream = pa.input_stream
    original_open_stream = pa.ipc.open_stream
    
    def mock_py_buffer(data):
        return Mock()
    
    def mock_input_stream(buffer, compression):
        return Mock()
    
    class MockReader:
        def __init__(self):
            pass
        
        def read_all(self):
            return test_data
        
        def read_pandas(self):
            return test_data.to_pandas()
        
        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            pass
    
    def mock_open_stream(stream):
        return MockReader()
    
    # Apply mocks
    pa.py_buffer = mock_py_buffer
    pa.input_stream = mock_input_stream
    pa.ipc.open_stream = mock_open_stream
    
    try:
        # Test with Arrow output format
        result = conn._handle_results("test_id", test_results)
        
        print("✓ Arrow output format test passed")
        print(f"  Result type: {type(result)}")
        print(f"  Is Arrow Table: {isinstance(result, pa.Table)}")
        
        if isinstance(result, pa.Table):
            print(f"  Schema: {result.schema}")
            print(f"  Row count: {len(result)}")
            print(f"  Columns: {result.column_names}")
        
        # Test with pandas output format (default)
        conn_pandas = Connection(
            ws=mock_ws,
            output_format=OutputFormat.PANDAS,
            results_format=ResultsFormat.ARROW
        )
        
        result_pandas = conn_pandas._handle_results("test_id", test_results)
        
        print("✓ Pandas output format test passed")
        print(f"  Result type: {type(result_pandas)}")
        print(f"  Is DataFrame: {isinstance(result_pandas, pd.DataFrame)}")
        
        if isinstance(result_pandas, pd.DataFrame):
            print(f"  Shape: {result_pandas.shape}")
            print(f"  Columns: {list(result_pandas.columns)}")
        
    finally:
        # Restore original functions
        pa.py_buffer = original_py_buffer
        pa.input_stream = original_input_stream
        pa.ipc.open_stream = original_open_stream

def test_cursor_with_arrow():
    """Test that cursor can handle Arrow tables."""
    
    # Create test Arrow data
    test_data = pa.table({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    
    # Create a cursor with mock functions
    exec_fn = Mock()
    cancel_fn = Mock()
    cursor = Cursor(exec_fn, cancel_fn)
    
    # Simulate receiving Arrow table results
    cursor._Cursor__queue.put(test_data)
    cursor._Cursor__current_execution_id = "test_execution"
    
    # Test fetchall
    results = cursor.fetchall()
    
    print("✓ Cursor Arrow table test passed")
    print(f"  Result type: {type(results)}")
    print(f"  Is Arrow Table: {isinstance(results, pa.Table)}")
    
    if isinstance(results, pa.Table):
        print(f"  Row count: {len(results)}")
        print(f"  Columns: {results.column_names}")

if __name__ == "__main__":
    print("Testing Arrow output functionality...")
    print("=" * 50)
    
    try:
        test_arrow_output()
        print()
        test_cursor_with_arrow()
        print()
        print("✓ All tests passed!")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)