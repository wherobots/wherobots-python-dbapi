"""Tests for result_store module: Store dataclass and StorageFormat enum."""

import json
import pytest

from wherobots.db.result_store import Store, StorageFormat, DEFAULT_STORAGE_FORMAT


class TestStorageFormat:
    def test_values(self):
        assert StorageFormat.PARQUET.value == "parquet"
        assert StorageFormat.CSV.value == "csv"
        assert StorageFormat.GEOJSON.value == "geojson"

    def test_default_format(self):
        assert DEFAULT_STORAGE_FORMAT == StorageFormat.PARQUET


class TestStore:
    def test_default_construction(self):
        store = Store()
        assert store.format == StorageFormat.PARQUET
        assert store.single is False
        assert store.generate_presigned_url is False
        assert store.options is None

    def test_with_format(self):
        store = Store(format=StorageFormat.CSV)
        assert store.format == StorageFormat.CSV
        assert store.options is None

    def test_with_options(self):
        store = Store(
            format=StorageFormat.GEOJSON,
            options={"ignoreNullFields": "false"},
        )
        assert store.options == {"ignoreNullFields": "false"}

    def test_with_multiple_options(self):
        opts = {"header": "false", "delimiter": "|", "quote": '"'}
        store = Store(format=StorageFormat.CSV, options=opts)
        assert store.options == opts

    def test_empty_options_normalized_to_none(self):
        store = Store(options={})
        assert store.options is None

    def test_none_options(self):
        store = Store(options=None)
        assert store.options is None

    def test_options_defensively_copied(self):
        original = {"key": "value"}
        store = Store(options=original)
        # Mutating the original should not affect the store
        original["key"] = "changed"
        assert store.options == {"key": "value"}

    def test_frozen_dataclass(self):
        store = Store()
        with pytest.raises(AttributeError):
            store.format = StorageFormat.CSV

    def test_presigned_url_requires_single(self):
        with pytest.raises(ValueError, match="single=True"):
            Store(generate_presigned_url=True, single=False)

    def test_presigned_url_with_single(self):
        store = Store(single=True, generate_presigned_url=True)
        assert store.single is True
        assert store.generate_presigned_url is True


class TestStoreForDownload:
    def test_default(self):
        store = Store.for_download()
        assert store.format == StorageFormat.PARQUET
        assert store.single is True
        assert store.generate_presigned_url is True
        assert store.options is None

    def test_with_format(self):
        store = Store.for_download(format=StorageFormat.CSV)
        assert store.format == StorageFormat.CSV
        assert store.single is True
        assert store.generate_presigned_url is True

    def test_with_options(self):
        store = Store.for_download(
            format=StorageFormat.GEOJSON,
            options={"ignoreNullFields": "false"},
        )
        assert store.format == StorageFormat.GEOJSON
        assert store.options == {"ignoreNullFields": "false"}


class TestStoreToDict:
    def test_without_options(self):
        store = Store(format=StorageFormat.PARQUET, single=True)
        d = store.to_dict()
        assert d == {
            "format": "parquet",
            "single": True,
            "generate_presigned_url": False,
        }
        assert "options" not in d

    def test_with_options(self):
        store = Store(
            format=StorageFormat.GEOJSON,
            single=True,
            generate_presigned_url=True,
            options={"ignoreNullFields": "false"},
        )
        d = store.to_dict()
        assert d == {
            "format": "geojson",
            "single": True,
            "generate_presigned_url": True,
            "options": {"ignoreNullFields": "false"},
        }

    def test_serializable_to_json(self):
        store = Store.for_download(
            format=StorageFormat.CSV,
            options={"header": "false"},
        )
        serialized = json.dumps(store.to_dict())
        deserialized = json.loads(serialized)
        assert deserialized["format"] == "csv"
        assert deserialized["single"] is True
        assert deserialized["generate_presigned_url"] is True
        assert deserialized["options"] == {"header": "false"}

    def test_to_dict_returns_copy(self):
        """Mutating the returned dict should not affect the Store."""
        store = Store(options={"key": "value"})
        d = store.to_dict()
        d["options"]["key"] = "changed"
        assert store.options == {"key": "value"}

    def test_full_execute_sql_request_shape(self):
        """Verify the dict integrates correctly into an execute_sql request."""
        store = Store.for_download(
            format=StorageFormat.GEOJSON,
            options={"ignoreNullFields": "false"},
        )
        request = {
            "kind": "execute_sql",
            "execution_id": "test-id",
            "statement": "SELECT 1",
            "store": store.to_dict(),
        }
        serialized = json.dumps(request)
        parsed = json.loads(serialized)
        assert parsed["store"]["format"] == "geojson"
        assert parsed["store"]["single"] is True
        assert parsed["store"]["generate_presigned_url"] is True
        assert parsed["store"]["options"] == {"ignoreNullFields": "false"}

    def test_request_without_store(self):
        """Without a store, the request should not have a store key."""
        request = {
            "kind": "execute_sql",
            "execution_id": "test-id",
            "statement": "SELECT 1",
        }
        serialized = json.dumps(request)
        parsed = json.loads(serialized)
        assert "store" not in parsed
