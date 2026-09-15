import re

STOP_WORDS = frozenset("a an and are as at be by for from in is it of on or that the this to was were with".split())

def remove_stop_words(text: str) -> str:
    return " ".join(word for word in re.findall(r"\b[\w']+\b", text.lower()) if word not in STOP_WORDS)
