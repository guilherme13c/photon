import json
import pytest
from unittest.mock import MagicMock
from src.service.processor import EmbeddingProcessorService

@pytest.fixture
def mock_vector_store():
    return MagicMock()

@pytest.fixture
def mock_object_store():
    return MagicMock()

@pytest.fixture
def mock_producer():
    return MagicMock()

# We will mock the SentenceTransformer class itself
def test_process_message_valid_json(mocker, mock_vector_store, mock_object_store, mock_producer):
    mock_model_class = mocker.patch("src.service.processor.SentenceTransformer")
    mock_model_instance = MagicMock()
    mock_model_instance.encode.return_value = [0.1, 0.2, 0.3]
    mock_model_class.return_value = mock_model_instance

    processor = EmbeddingProcessorService("dummy-model", mock_vector_store, mock_producer)

    message = json.dumps({
        "url": "http://example.com",
        "title": "Example",
        "text": "This is an example document.",
        "s3_key": "dummy.html"
    }).encode('utf-8')

    processor.process_message(message)

    mock_model_instance.encode.assert_called_once_with(
        ["Example\nThis is an example document."], batch_size=32, show_progress_bar=False
    )
    mock_vector_store.insert_batch.assert_called_once()
    mock_producer.publish_cleanup_requests.assert_called_once_with(["dummy.html"])

def test_process_message_missing_text(mocker, mock_vector_store, mock_object_store, mock_producer):
    mock_model_class = mocker.patch("src.service.processor.SentenceTransformer")
    processor = EmbeddingProcessorService("dummy-model", mock_vector_store, mock_producer)

    message = json.dumps({
        "url": "http://example.com",
        "title": "Example",
        "text": ""
    }).encode('utf-8')

    processor.process_message(message)

    # Should not process if text is missing
    mock_vector_store.insert.assert_not_called()
    mock_producer.publish_cleanup_requests.assert_not_called()

def test_process_message_invalid_json(mocker, mock_vector_store, mock_object_store, mock_producer):
    mock_model_class = mocker.patch("src.service.processor.SentenceTransformer")
    processor = EmbeddingProcessorService("dummy-model", mock_vector_store, mock_producer)

    message = b"invalid-json"

    processor.process_message(message)

    mock_vector_store.insert.assert_not_called()
    mock_object_store.delete.assert_not_called()
    mock_producer.publish_dead_letter.assert_called_once()
