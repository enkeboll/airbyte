#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import unittest
from unittest.mock import Mock

from airbyte_cdk.models.airbyte_protocol import AirbyteStream, DestinationSyncMode, SyncMode

from destination_chroma.config import ChromaIndexingConfigModel
from destination_chroma.indexer import ChromaIndexer



class TestChromaIndexer(unittest.TestCase):
    def setUp(self):
        self.mock_config = ChromaIndexingConfigModel(
            **{
                "collection_name": "dummy-collection",
                "auth_method": {
                    "mode": "persistent_client",
                    "path": "./path",
                },
            }
        )
        self.chroma_indexer = ChromaIndexer(self.mock_config)
        self.chroma_indexer._get_client = Mock()
        self.mock_client = self.chroma_indexer._get_client()
        self.mock_client.get_or_create_collection = Mock()
        self.mock_collection = self.mock_client.get_or_create_collection()
        self.chroma_indexer.client = self.mock_client
        self.mock_client.get_collection = Mock()

    def test_valid_collection_name(self):

        test_configs = [
            ({"collection_name": "dummy-collection", "auth_method": {"mode": "persistent_client", "path": "./path"}}, 'dummy-collection'),
            ({"collection_name": "du", "auth_method": {"mode": "persistent_client", "path": "./path"}}, 'dux'),
            ({"collection_name": "dummy-collectionxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "auth_method": {"mode": "persistent_client", "path": "./path"}}, 'dummy-collectionxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'),
            ({"collection_name": "1dummy-colle..ction4", "auth_method": {"mode": "persistent_client", "path": "./path"}}, '1dummy-collection4'),
            ({"collection_name": "Dummy-coll...ectioN", "auth_method": {"mode": "persistent_client", "path": "./path"}}, 'dummy-coll.ection'),
            ({"collection_name": "-dum?my-collection-", "auth_method": {"mode": "persistent_client", "path": "./path"}}, 'x-dummy-collection-x'),
            ({"collection_name": "123.34...54.2", "auth_method": {"mode": "persistent_client", "path": "./path"}}, '12334542'),
            ({"collection_name": "345.4.23.12", "auth_method": {"mode": "persistent_client", "path": "./path"}}, '345.4.23.12')
            ]
        
        for config, collection_name in test_configs:
            mock_config = ChromaIndexingConfigModel(**config)
            chroma_indexer = ChromaIndexer(mock_config)

            self.assertEqual(chroma_indexer.collection_name, collection_name)

    def test_check_returns_expected_result(self):
        check_result = self.chroma_indexer.check()

        self.assertIsNone(check_result)

        self.chroma_indexer._get_client.assert_called()
        self.mock_client.heartbeat.assert_called()
        self.mock_client.get_or_create_collection.assert_called()
        self.mock_client.get_or_create_collection().count.assert_called()

    def test_check_handles_failure_conditions(self):
        # Test 1: client heartbeat returns error
        self.mock_client.heartbeat.side_effect = Exception("Random exception")
        result = self.chroma_indexer.check()
        self.assertTrue("Random exception" in result)

        # Test 2: client server is not alive
        self.mock_client.heartbeat.side_effect = None
        self.mock_client.heartbeat.return_value = None
        result = self.chroma_indexer.check()
        self.assertEqual(result, "Chroma client server is not alive")

        # Test 3: unable to get collection
        self.mock_client.heartbeat.return_value = 45465
        self.mock_collection.count.return_value = None
        result = self.chroma_indexer.check()
        self.assertEqual(result, f"unable to get or create collection with name {self.chroma_indexer.collection_name}")

    def test_pre_sync_calls_delete(self):
        self.chroma_indexer.pre_sync(
            Mock(
                streams=[
                    Mock(
                        destination_sync_mode=DestinationSyncMode.overwrite,
                        stream=AirbyteStream(name="some_stream", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]),
                    )
                ]
            )
        )

        self.mock_client.get_collection().delete.assert_called_with(where={'_ab_stream': {"$in": ['some_stream']}})

    def test_pre_sync_does_not_call_delete(self):
        self.chroma_indexer.pre_sync(
            Mock(streams=[Mock(destination_sync_mode=DestinationSyncMode.append, stream=Mock(name="some_stream"))])
        )

        self.mock_client.get_collection().delete.assert_not_called()

    def test_index_calls_insert(self):
        self.chroma_indexer.index(
            [Mock(metadata={"key": "value"}, page_content="some content", embedding=[1,2,3])], []
            )

        self.mock_client.get_collection().add.assert_called_once()

    def test_index_calls_delete(self):
        self.chroma_indexer.index([], ["some_id"])

        self.mock_client.get_collection().delete.assert_called_with(where={'_ab_record_id': {"$in": ['some_id']}})
