"""Pagination classes for tap-taboola."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from singer_sdk.pagination import BaseAPIPaginator
from typing_extensions import override


class DayPaginator(BaseAPIPaginator[date]):
    """Day paginator."""

    @override
    def has_more(self, response):
        return self.current_value < datetime.now(tz=timezone.utc).date()

    @override
    def get_next(self, response):
        return self.current_value + timedelta(days=1)

    @override
    def continue_if_empty(self, response):
        return True
