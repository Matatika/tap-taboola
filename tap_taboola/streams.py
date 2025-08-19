"""Stream type classes for tap-taboola."""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

from singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_taboola.client import TaboolaStream

if TYPE_CHECKING:
    import requests

TargetingType = th.PropertiesList(
    th.Property("type", th.StringType),
    th.Property("value", th.ArrayType(th.StringType)),
    th.Property("href", th.URIType),
)

BidModifierType = th.PropertiesList(
    th.Property(
        "values",
        th.ArrayType(
            th.NullType()  # TODO: establish what type this is
        ),
    ),
)

MultiTargetingType = th.PropertiesList(
    th.Property("state", th.StringType),
    th.Property("value", th.NullType()),  # this is always `null`
    th.Property("href", th.URIType),
)


class _ResumableAPIError(Exception):
    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message)
        self.response = response


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

    @override
    def get_child_context(self, record, context):
        return {"account_id": record["account_id"]}


class CampaignStream(TaboolaStream):
    """Define campaigns stream."""

    parent_stream_type = AccountStream
    name = "campaigns"
    path = "/{account_id}/campaigns"
    primary_keys = ("id", "advertiser_id")

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("advertiser_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("branding_text", th.StringType),
        th.Property("tracking_code", th.StringType),
        th.Property("pricing_model", th.StringType),
        th.Property("cpc", th.NumberType),
        th.Property("safety_rating", th.StringType),
        th.Property("daily_cap", th.NumberType),
        th.Property("daily_ad_delivery_model", th.StringType),
        th.Property("spending_limit", th.NumberType),
        th.Property("spending_limit_model", th.StringType),
        th.Property("cpa_goal", th.NumberType),
        th.Property(
            "min_expected_conversions_for_cpa_goal",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property("country_targeting", TargetingType),
        th.Property("sub_country_targeting", TargetingType),
        th.Property("dma_country_targeting", TargetingType),
        th.Property("region_country_targeting", TargetingType),
        th.Property("city_targeting", TargetingType),
        th.Property("postal_code_targeting", TargetingType),
        th.Property("contextual_targeting", TargetingType),
        th.Property("platform_targeting", TargetingType),
        th.Property("publisher_targeting", TargetingType),
        th.Property("auto_publisher_targeting", TargetingType),
        th.Property("os_targeting", TargetingType),
        th.Property("connection_type_targeting", TargetingType),
        th.Property("app_restriction_targeting", TargetingType),
        th.Property("publisher_bid_modifier", BidModifierType),
        th.Property("platform_bid_modifier", BidModifierType),
        th.Property("publisher_platform_bid_modifier", BidModifierType),
        th.Property(
            "day_time_bid_modifier",
            th.ObjectType(
                *BidModifierType,
                th.Property("time_zone", th.StringType),
            ),
        ),
        th.Property("publisher_bid_strategy_modifiers", BidModifierType),
        th.Property(
            "campaign_profile",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property("comments", th.StringType),
        th.Property("spent", th.NumberType),
        th.Property("bid_type", th.StringType),
        th.Property("bid_strategy", th.StringType),
        th.Property("traffic_allocation_mode", th.StringType),
        th.Property(
            "external_brand_safety",
            th.ObjectType(
                th.Property("type", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            ),
        ),
        th.Property(
            "campaign_groups",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "target_cpa",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property("learning_state", th.StringType),
        th.Property("cvr_learning_status", th.StringType),
        th.Property(
            "target_cpa_learning_status",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "conversion_rules",
            th.ObjectType(
                th.Property(
                    "rules",
                    th.ArrayType(
                        th.NullType()  # TODO: establish what type this is
                    ),
                ),
            ),
        ),
        th.Property(
            "funnel_template",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property("start_date", th.DateType),
        th.Property("end_date", th.DateType),
        th.Property("start_date_in_utc", th.DateTimeType),
        th.Property("end_date_in_utc", th.DateTimeType),
        th.Property("traffic_allocation_ab_test_end_date", th.DateType),
        th.Property("approval_state", th.StringType),
        th.Property("is_active", th.BooleanType),
        th.Property("status", th.StringType),
        th.Property("audience_segments_multi_targeting", MultiTargetingType),
        th.Property("contextual_segments_targeting", MultiTargetingType),
        th.Property("custom_contextual_targeting", MultiTargetingType),
        th.Property("audiences_targeting", MultiTargetingType),
        th.Property("custom_audience_targeting", MultiTargetingType),
        th.Property(
            "segments_targeting",
            th.ObjectType(
                th.Property("GENDER", TargetingType),
                th.Property("AGE", TargetingType),
            ),
        ),
        th.Property("segments_multi_targeting", MultiTargetingType),
        th.Property("marking_label_multi_targeting", MultiTargetingType),
        th.Property("lookalike_audience_targeting", MultiTargetingType),
        th.Property(
            "predefined_targeting_options",
            th.ObjectType(
                th.Property("predefined_supply_targeting", th.StringType),
            ),
        ),
        th.Property("marketing_objective", th.StringType),
        th.Property(
            "verification_pixel",
            th.ObjectType(
                th.Property(
                    "verification_pixel_items",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("url", th.URIType),
                            th.Property("verification_pixel_type", th.StringType),
                        ),
                    ),
                )
            ),
        ),
        th.Property(
            "viewability_tag",
            th.ObjectType(
                th.Property(
                    "values",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("type", th.StringType),
                            th.Property("tag", th.StringType),
                        ),
                    ),
                )
            ),
        ),
        th.Property(
            "activity_schedule",
            th.ObjectType(
                th.Property("mode", th.StringType),
                th.Property(
                    "rules",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("type", th.StringType),
                            th.Property("day", th.StringType),
                            th.Property("from_hour", th.IntegerType),
                            th.Property("until_hour", th.IntegerType),
                        )
                    ),
                ),
                th.Property("time_zone", th.StringType),
            ),
        ),
        th.Property(
            "policy_review",
            th.ObjectType(
                th.Property("reject_reason", th.StringType),
                th.Property("reject_reason_description", th.StringType),
                th.Property("reviewer_notes", th.StringType),
            ),
        ),
        th.Property("browser_targeting", TargetingType),
        th.Property(
            "external_metadata",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property("is_spend_guard_active", th.StringType),
        th.Property(
            "frequency_capping_targeting",
            th.ObjectType(
                th.Property(
                    "threshold",
                    th.NullType(),  # TODO: establish what type this is
                ),
                th.Property("frequency_capping_days", th.StringType),
            ),
        ),
        th.Property("campaign_item_type", th.StringType),
        th.Property(
            "performance_rule_ids",
            th.ArrayType(
                th.NullType()  # TODO: establish what type this is
            ),
        ),
        th.Property(
            "budget_additional_parameters",
            th.ObjectType(additional_properties=True),
        ),
    ).to_dict()

    @override
    def get_records(self, context):
        try:
            yield from super().get_records(context)
        except _ResumableAPIError as e:
            self.logger.warning(e)

    @override
    def validate_response(self, response):
        if response.status_code == HTTPStatus.NOT_FOUND:
            msg = f"Could not get data for account '{self.context['account_id']}'"
            raise _ResumableAPIError(msg, response)

        super().validate_response(response)
