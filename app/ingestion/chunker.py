from dataclasses import dataclass


@dataclass(frozen=True)
class TextChunk:
    text: str
    start_char: int
    end_char: int


def chunk_text(text: str, *, size: int, overlap: int) -> list[TextChunk]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    if overlap < 0 or overlap >= size:
        raise ValueError("chunk overlap must be non-negative and smaller than size")

    chunks: list[TextChunk] = []
    cursor = 0
    while cursor < len(text):
        end = min(len(text), cursor + size)
        if end < len(text):
            boundary = text.rfind(" ", cursor + size // 2, end)
            if boundary > cursor:
                end = boundary

        raw = text[cursor:end]
        leading = len(raw) - len(raw.lstrip())
        trailing = len(raw.rstrip())
        if trailing > leading:
            chunks.append(
                TextChunk(
                    text=raw[leading:trailing],
                    start_char=cursor + leading,
                    end_char=cursor + trailing,
                )
            )

        if end >= len(text):
            break
        next_cursor = end - overlap
        cursor = next_cursor if next_cursor > cursor else end

    return chunks
