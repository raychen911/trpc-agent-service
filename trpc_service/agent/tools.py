"""Stage 1 deterministic tools for the demo agent."""

from __future__ import annotations

import datetime as _dt


def get_current_time() -> str:
    """Return the current local time as a timezone-aware ISO-8601 string.

    The agent instruction tells the model to call this tool whenever a user asks
    for "current time" / "what time is it" / "now". Returning a tz-aware string
    keeps the result unambiguous and trivially parseable by tests.
    """
    return _dt.datetime.now(_dt.timezone.utc).astimezone().isoformat(timespec="seconds")


__all__ = ["get_current_time"]
