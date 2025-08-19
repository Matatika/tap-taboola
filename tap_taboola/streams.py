"""Stream type classes for tap-taboola."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_taboola.client import TaboolaStream


class AccountStream(TaboolaStream):
    """Define accounts stream."""

    name = "accounts"
    path = "/users/current/allowed-accounts"
    primary_keys = ("id",)

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("account_id", th.StringType),
        th.Property("partner_types", th.ArrayType(th.StringType)),
        th.Property("type", th.StringType),
        th.Property("campaign_types", th.ArrayType(th.StringType)),
        th.Property("currency", th.StringType),
        th.Property("time_zone_name", th.StringType),
        th.Property("default_platform", th.StringType),
        th.Property("is_active", th.BooleanType),
        th.Property("language", th.StringType),
        th.Property("country", th.StringType),
        th.Property("is_fla", th.BooleanType),
        th.Property(
            "account_metadata",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "external_metadata",
            th.NullType(),  # TODO: establish what type this is
        ),
    ).to_dict()
