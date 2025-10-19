import re

def is_matching(filename: str, pattern: re.Pattern) -> bool:
    return pattern.match(filename) is not None