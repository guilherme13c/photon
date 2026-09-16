import pytest

from src.service.chunking import chunk_text, clean_text, is_meaningful


def test_clean_text_removes_markup_and_normalizes_whitespace():
    assert clean_text("<p>Hello&nbsp;world</p>\n\nagain") == "Hello world again"


def test_tiny_markup_fragment_is_not_meaningful():
    assert not is_meaningful('me\\" />')
    assert is_meaningful("A useful document contains enough words to retrieve reliably.")


def test_short_document_stays_as_one_chunk():
    assert chunk_text("Heading\nA short paragraph.", max_tokens=20, overlap_tokens=2) == [
        "Heading A short paragraph."
    ]


def test_long_document_is_bounded_and_overlaps():
    chunks = chunk_text("\n".join(f"paragraph {index}" for index in range(20)), max_tokens=8, overlap_tokens=2)

    assert len(chunks) > 1
    assert all(len(chunk.split()) <= 8 for chunk in chunks)
    assert set(chunks[0].split()[-2:]).intersection(chunks[1].split()[:2])


def test_invalid_chunk_limits_are_rejected():
    with pytest.raises(ValueError):
        chunk_text("text", max_tokens=4, overlap_tokens=4)
