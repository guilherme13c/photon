"""Structure-aware, dependency-free text cleaning and chunking."""

import html
import re


_TAG_RE = re.compile(r"<[^>]{1,512}>")
_SPACE_RE = re.compile(r"\s+")
_MEANINGFUL_RE = re.compile(r"[\wÀ-ÖØ-öø-ÿ]", re.UNICODE)


def clean_text(text: str) -> str:
    """Remove residual markup and normalize whitespace before embedding."""
    text = html.unescape(text)
    text = _TAG_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def is_meaningful(text: str, min_chars: int = 20, min_words: int = 4) -> bool:
    """Reject markup fragments and tiny chunks with little retrieval value."""
    cleaned = clean_text(text)
    return len(cleaned) >= min_chars and len(cleaned.split()) >= min_words and bool(_MEANINGFUL_RE.search(cleaned))


def _words(text: str) -> list[str]:
    return text.split()


def chunk_text(text: str, max_tokens: int = 320, overlap_tokens: int = 48) -> list[str]:
    """Split text on blank lines while bounding approximate token count.

    Word count is used as a stable approximation here; the model tokenizer is
    still the final authority on its own input limit.
    """
    if max_tokens <= 0 or overlap_tokens < 0 or overlap_tokens >= max_tokens:
        raise ValueError("invalid chunk limits")
    words = _words(text)
    if not words:
        return []
    paragraphs = [paragraph.split() for paragraph in text.split("\n") if paragraph.split()]
    chunks: list[str] = []
    current: list[str] = []
    for paragraph in paragraphs:
        while paragraph:
            capacity = max_tokens - len(current)
            if capacity == 0:
                chunks.append(" ".join(current))
                current = current[-overlap_tokens:] if overlap_tokens else []
                capacity = max_tokens - len(current)
            take = min(capacity, len(paragraph))
            current.extend(paragraph[:take])
            paragraph = paragraph[take:]
            if len(current) == max_tokens:
                chunks.append(" ".join(current))
                current = current[-overlap_tokens:] if overlap_tokens else []
        if len(current) >= max_tokens:
            chunks.append(" ".join(current))
            current = current[-overlap_tokens:] if overlap_tokens else []
    if current:
        chunks.append(" ".join(current))
    return chunks
