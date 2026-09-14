"""Structure-aware, dependency-free text chunking for embedding inputs."""


def _words(text: str) -> list[str]:
    return text.split()


def chunk_text(text: str, max_tokens: int = 450, overlap_tokens: int = 60) -> list[str]:
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
