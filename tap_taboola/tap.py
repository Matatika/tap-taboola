"""Taboola tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from typing_extensions import override

from tap_taboola import streams

STREAM_TYPES = [
    streams.AccountStream,
    streams.CampaignStream,
    streams.CampaignItemStream,
    streams.TopCampaignContentDailyReportStream,
    streams.PublisherStream,
]


class TapTaboola(Tap):
    """Taboola tap class."""

    name = "tap-taboola"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            title="Client ID",
            description="Taboola Backstage API client ID",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            title="Client Secret",
            description="Taboola Backstage API client secret",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            title="Start Date",
            description="Initial date to start extracting data from",
        ),
    ).to_dict()

    @override
    def discover_streams(self):
        return [stream_cls(tap=self) for stream_cls in STREAM_TYPES]


if __name__ == "__main__":
    TapTaboola.cli()
