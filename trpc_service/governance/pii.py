import re


class PiiRedactor:
    _patterns = (
        ("EMAIL", re.compile(r"(?<![\w.-])[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}(?![\w.-])")),
        ("CN_ID", re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")),
        ("CN_PHONE", re.compile(r"(?<!\d)(?:\+?86[- ]?)?1[3-9]\d{9}(?!\d)")),
        ("BANK_CARD", re.compile(r"(?<!\d)(?:\d[ -]?){15,18}\d(?!\d)")),
    )

    def redact(self, text: str) -> tuple[str, tuple[str, ...]]:
        redacted = text
        found: list[str] = []
        for label, pattern in self._patterns:
            redacted, count = pattern.subn(f"[{label}]", redacted)
            if count:
                found.append(label)
        return redacted, tuple(found)
