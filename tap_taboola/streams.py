"""Stream type classes for tap-taboola."""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

from singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_taboola.client import TaboolaStream
from tap_taboola.pagination import DayPaginator

if TYPE_CHECKING:
    from datetime import date

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
    def get_records(self, context):
        records = super().get_records(context)

        account_ids = set(self.config["account_ids"])

        if not account_ids:
            yield from records
            return

        for record in records:
            account_id = record["account_id"]

            if account_id in account_ids:
                account_ids.remove(account_id)
                yield record
                continue

            self.logger.info("Account '%s' is not selected; skipping", account_id)

        if account_ids:
            self.logger.warning(
                (
                    "Some accounts IDs are either not accessible or do not exist for"
                    " the authenticated principal: %s"
                ),
                account_ids,
            )

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

    @override
    def get_child_context(self, record, context):
        return context | {"campaign_id": record["id"]}


class CampaignItemStream(TaboolaStream):
    """Define campaign items stream."""

    parent_stream_type = CampaignStream
    name = "campaign_items"
    path = "/{account_id}/campaigns/{campaign_id}/items"
    primary_keys = ("id", "campaign_id")

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("campaign_id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("url", th.URIType),
        th.Property("thumbnail_url", th.URIType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("approval_state", th.StringType),
        th.Property("is_active", th.BooleanType),
        th.Property("status", th.StringType),
        th.Property(
            "policy_review",
            th.ObjectType(
                th.Property("reject_reason", th.StringType),
                th.Property("reject_reason_description", th.StringType),
                th.Property("status_reason", th.StringType),
                th.Property("reviewer_notes", th.StringType),
            ),
        ),
        th.Property(
            "cta",
            th.ObjectType(
                th.Property("cta_type", th.StringType),
            ),
        ),
        th.Property(
            "creative_focus",
            th.ObjectType(
                th.Property("type", th.StringType),
                th.Property(
                    "coordinates",
                    th.ObjectType(
                        th.Property("x", th.IntegerType),
                        th.Property("y", th.IntegerType),
                    ),
                ),
            ),
        ),
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
            "app_install",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "app_install",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "logo",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "disclaimer",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "creative_crop",
            th.ObjectType(
                th.Property(
                    "crop_date",
                    th.ObjectType(
                        th.Property(
                            "ratio",
                            th.ObjectType(
                                th.Property("width", th.IntegerType),
                                th.Property("height", th.IntegerType),
                            ),
                        ),
                        th.Property(
                            "area",
                            th.ObjectType(
                                th.Property("top_left_x", th.IntegerType),
                                th.Property("top_left_y", th.IntegerType),
                                th.Property("width", th.IntegerType),
                                th.Property("height", th.IntegerType),
                            ),
                        ),
                        th.Property("url", th.URIType),
                    ),
                ),
            ),
        ),
        th.Property(
            "external_metadata",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property("creative_type", th.StringType),
        th.Property(
            "performance_video_data",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "display_data",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "hierarchy_rep_item_id",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "hierarchy_data",
            th.NullType(),  # TODO: establish what type this is
        ),
        th.Property(
            "custom_data",
            th.ObjectType(
                th.Property("creative_name", th.StringType),
                th.Property("custom_id", th.StringType),
            ),
        ),
        th.Property("start_date", th.DateType),
        th.Property("end_date", th.DateType),
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
            "orientation",
            th.NullType(),  # TODO: establish what type this is
        ),
    ).to_dict()


class CampaignSummarySiteDailyReport(TaboolaStream):
    """Define campaign summary site daily report stream."""

    parent_stream_type = AccountStream
    name = "campaign_summary_site_daily_report"
    path = (
        "/{account_id}/reports/campaign-summary/dimensions/campaign_site_day_breakdown"
    )
    primary_keys = ("date", "site_id", "campaign")
    replication_key = "date"
    is_timestamp_replication_key = True
    is_sorted = True
    selected_by_default = False

    schema = th.PropertiesList(
        th.Property("date", th.DateTimeType),
        th.Property("site", th.StringType),
        th.Property("site_name", th.StringType),
        th.Property("site_id", th.IntegerType),
        th.Property("campaign", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("clicks", th.IntegerType),
        th.Property("impressions", th.IntegerType),
        th.Property("visible_impressions", th.IntegerType),
        th.Property("spent", th.NumberType),
        th.Property("conversions_value", th.NumberType),
        th.Property("roas", th.NumberType),
        th.Property("roas_clicks", th.NumberType),
        th.Property("roas_views", th.NumberType),
        th.Property("ctr", th.NumberType),
        th.Property("vctr", th.NumberType),
        th.Property("cpm", th.NumberType),
        th.Property("vcpm", th.NumberType),
        th.Property("cpc", th.NumberType),
        th.Property("cpa", th.NumberType),
        th.Property("cpa_clicks", th.NumberType),
        th.Property("cpa_views", th.NumberType),
        th.Property("cpa_actions_num", th.IntegerType),
        th.Property("cpa_conversion_rate_clicks", th.NumberType),
        th.Property("cpa_conversion_rate_views", th.NumberType),
        th.Property("blocking_level", th.StringType),
        th.Property("currency", th.StringType),
    ).to_dict()

    @override
    def get_new_paginator(self):
        start_date = self.get_starting_timestamp(self.context).date()
        return DayPaginator(start_date)

    @override
    def get_url_params(self, context, next_page_token: date):
        self._date = next_page_token

        return {
            "start_date": next_page_token.isoformat(),
            "end_date": next_page_token.isoformat(),
        }

    @override
    def post_process(self, row, context=None):
        date: str = row["date"]
        row["date"] = date.removesuffix(".0")

        return row


class TopCampaignContentDailyReportStream(TaboolaStream):
    """Define top campaign content daily report stream."""

    _date: date

    parent_stream_type = AccountStream
    name = "top_campaign_content_daily_report"
    path = "/{account_id}/reports/top-campaign-content/dimensions/item_breakdown"
    primary_keys = ("date", "item", "content_provider")
    replication_key = "date"
    is_timestamp_replication_key = True
    is_sorted = True
    selected_by_default = False

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("item", th.StringType),
        th.Property("ad_name", th.StringType),
        th.Property("custom_id", th.StringType),
        th.Property("item_name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("thumbnail_url", th.URIType),
        th.Property("url", th.URIType),
        th.Property("campaign", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("content_provider", th.StringType),
        th.Property("content_provider_name", th.StringType),
        th.Property("content_provider_id_name", th.StringType),
        th.Property("impressions", th.IntegerType),
        th.Property("visible_impressions", th.IntegerType),
        th.Property("ctr", th.NumberType),
        th.Property("vctr", th.NumberType),
        th.Property("clicks", th.IntegerType),
        th.Property("cpc", th.NumberType),
        th.Property("cvr", th.NumberType),
        th.Property("cvr_clicks", th.NumberType),
        th.Property("cvr_views", th.NumberType),
        th.Property("cpa", th.NumberType),
        th.Property("cpa_clicks", th.NumberType),
        th.Property("cpa_views", th.NumberType),
        th.Property("actions", th.IntegerType),
        th.Property("actions_num_from_clicks", th.IntegerType),
        th.Property("actions_num_from_views", th.IntegerType),
        th.Property("cpm", th.NumberType),
        th.Property("vcpm", th.NumberType),
        th.Property("spent", th.NumberType),
        th.Property("conversions_value", th.NumberType),
        th.Property("roas", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("create_time", th.DateTimeType),
        th.Property("old_item_version_id", th.StringType),
        th.Property("learning_display_status", th.StringType),
    ).to_dict()

    @override
    def get_new_paginator(self):
        start_date = self.get_starting_timestamp(self.context).date()
        return DayPaginator(start_date)

    @override
    def get_url_params(self, context, next_page_token: date):
        self._date = next_page_token

        return {
            "start_date": next_page_token.isoformat(),
            "end_date": next_page_token.isoformat(),
        }

    @override
    def post_process(self, row, context=None):
        if row["item"] is None:
            self.logger.warning("Ignoring invalid record with null `item` ID: %s", row)
            return None

        row["date"] = self._date.isoformat()
        return row

    @override
    def _finalize_state(self, state=None):
        if state is not None:
            # always update state for the current date, even if no records were returned
            state.setdefault("replication_key", self.replication_key)
            state["replication_key_value"] = self._date.isoformat()

        return super()._finalize_state(state)


class PublisherStream(TaboolaStream):
    """Define publishers stream."""

    parent_stream_type = AccountStream
    name = "publishers"
    path = "/{account_id}/allowed-publishers"
    primary_keys = ("id", "account_id")

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
