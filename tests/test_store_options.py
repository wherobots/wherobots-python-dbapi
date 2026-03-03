"""Tests for Store options support."""

import json

from wherobots.db.models import Store
from wherobots.db.types import StorageFormat


class TestStoreOptions:
    """Tests for the options field on Store."""

    def test_default_options_is_none(self):
        store = Store(format=StorageFormat.PARQUET)
        assert store.options is None

    def test_options_set(self):
        store = Store(
            format=StorageFormat.CSV,
            options={"header": "false", "delimiter": "|"},
        )
        assert store.options == {"header": "false", "delimiter": "|"}

    def test_empty_options_normalized_to_none(self):
        store = Store(format=StorageFormat.PARQUET, options={})
        assert store.options is None

    def test_none_options_stays_none(self):
        store = Store(format=StorageFormat.PARQUET, options=None)
        assert store.options is None

    def test_options_defensively_copied(self):
        original = {"header": "false"}
        store = Store(format=StorageFormat.CSV, options=original)
        # Mutating the original should not affect the store
        original["delimiter"] = "|"
        assert "delimiter" not in store.options


class TestStoreForDownloadWithOptions:
    """Tests for Store.for_download() with options parameter."""

    def test_for_download_default_no_options(self):
        store = Store.for_download()
        assert store.options is None

    def test_for_download_with_options(self):
        store = Store.for_download(options={"header": "false"})
        assert store.options == {"header": "false"}
        assert store.single is True
        assert store.generate_presigned_url is True

    def test_for_download_with_format_and_options(self):
        store = Store.for_download(
            format=StorageFormat.CSV,
            options={"header": "false", "delimiter": "|"},
        )
        assert store.format == StorageFormat.CSV
        assert store.options == {"header": "false", "delimiter": "|"}

    def test_for_download_empty_options_normalized(self):
        store = Store.for_download(options={})
        assert store.options is None


class TestStoreSerializationWithOptions:
    """Tests for Store.to_dict() serialization."""

    def test_serialize_without_options(self):
        store = Store.for_download(format=StorageFormat.GEOJSON)
        d = store.to_dict()
        assert d == {
            "format": "geojson",
            "single": "true",
            "generate_presigned_url": "true",
        }
        assert "options" not in d

    def test_serialize_with_options(self):
        store = Store.for_download(
            format=StorageFormat.CSV,
            options={"header": "false", "delimiter": "|"},
        )
        d = store.to_dict()
        assert d == {
            "format": "csv",
            "single": "true",
            "generate_presigned_url": "true",
            "options": {"header": "false", "delimiter": "|"},
        }

    def test_serialize_empty_options_omitted(self):
        store = Store(format=StorageFormat.PARQUET, options={})
        d = store.to_dict()
        assert "options" not in d

    def test_json_roundtrip_with_options(self):
        store = Store.for_download(
            format=StorageFormat.GEOJSON,
            options={"ignoreNullFields": "false"},
        )
        d = store.to_dict()
        payload = json.dumps(d)
        parsed = json.loads(payload)
        assert parsed["options"] == {"ignoreNullFields": "false"}

    def test_full_request_shape(self):
        """Verify the full execute_sql request dict shape with store options."""
        store = Store.for_download(
            format=StorageFormat.CSV,
            options={"header": "false"},
        )
        request = {
            "kind": "execute_sql",
            "execution_id": "test-id",
            "statement": "SELECT 1",
        }
        store_dict = store.to_dict()
        request["store"] = store_dict

        assert request == {
            "kind": "execute_sql",
            "execution_id": "test-id",
            "statement": "SELECT 1",
            "store": {
                "format": "csv",
                "single": "true",
                "generate_presigned_url": "true",
                "options": {"header": "false"},
            },
        }
