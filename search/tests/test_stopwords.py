from src.stopwords import remove_stop_words

def test_query_and_document_normalization_is_deterministic():
    assert remove_stop_words("The Photon system is fast") == "photon system fast"
