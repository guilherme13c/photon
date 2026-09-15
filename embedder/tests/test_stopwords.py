from src.service.stopwords import remove_stop_words

def test_removes_common_words_and_normalizes_whitespace():
    assert remove_stop_words("The Photon system is fast, and reliable.") == "photon system fast reliable"
