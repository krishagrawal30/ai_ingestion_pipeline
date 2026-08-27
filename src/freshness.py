from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime


def parse_date(value: str | None, now: datetime | None = None) -> datetime | None:
    """Parse ISO, RFC 2822, and common relative dates into UTC."""
    if not value:
        return None
    current = now or datetime.now(timezone.utc)
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            match = re.search(r"(\d+)\s*(minute|hour|day)s?\s*ago", text.lower())
            if not match:
                return None
            amount = int(match.group(1))
            parsed = current - timedelta(**{f"{match.group(2)}s": amount})
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_fresh(published_at: datetime | None, now: datetime | None = None, hours: int = 24) -> bool:
    if published_at is None:
        return False
    current = now or datetime.now(timezone.utc)
    age = current - published_at.astimezone(timezone.utc)
    return timedelta(0) <= age <= timedelta(hours=hours)