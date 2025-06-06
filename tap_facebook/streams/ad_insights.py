"""Stream class for AdInsights."""

from __future__ import annotations

import json
import time
import typing as t
from functools import lru_cache
from urllib.parse import urlencode

import facebook_business.adobjects.user as fb_user
import pendulum
import requests as rq
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsactionstats import AdsActionStats
from facebook_business.adobjects.adshistogramstats import AdsHistogramStats
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from singer_sdk import typing as th
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, Stream
from tap_facebook.streams.base_streams import AdAccountsStream



COLUMN_LIST = [
    "ad_id",
    "account_id",
    "date_start",
    "ad_name",
    "adset_id",
    "campaign_id",
    "campaign_name",
    "clicks",
    "conversions",
    "cpc",
    "cpm",
    "cpp",
    "created_time",
    "date_stop",
    "impressions",
    "reach",
    "spend",
    "updated_time",
    "actions",
    "action_values",
    "conversion_values"
]

SLEEP_TIME_INCREMENT = 5
INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60
USAGE_LIMIT_THRESHOLD = 75
BATCH_SIZE = 30

BACKOFF_MAX_RETRIES = 5
BACKOFF_INITIAL_SLEEP = 60

class AdsInsightStream(Stream):
    name = "adsinsights"
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "date_start"
    parent_stream_type = AdAccountsStream

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize the stream."""
        self._report_definition = kwargs.pop("report_definition")
        kwargs["name"] = f"{self.name}_{self._report_definition['name']}"
        super().__init__(*args, **kwargs)

    @property
    def primary_keys(self) -> list[str] | None:
        return ["date_start", "account_id", "ad_id"] + self._report_definition["breakdowns"]

    @primary_keys.setter
    def primary_keys(self, new_value: list[str] | None) -> None:
        """Set primary key(s) for the stream.

        Args:
            new_value: TODO
        """
        self._primary_keys = new_value

    @staticmethod
    def _get_datatype(field: str) -> th.Type | None:
        d_type = AdsInsights._field_types[field]  # noqa: SLF001
        if d_type == "string":
            return th.StringType()
        if d_type.startswith("list"):
            if "AdsActionStats" in d_type:
                sub_props = [
                    th.Property(field.replace("field_", ""), th.StringType())
                    for field in list(AdsActionStats.Field.__dict__)
                ]
                return th.ArrayType(th.ObjectType(*sub_props))
            if "AdsHistogramStats" in d_type:
                sub_props = []
                for field in list(AdsHistogramStats.Field.__dict__):
                    clean_field = field.replace("field_", "")
                    if AdsHistogramStats._field_types[clean_field] == "string":  # noqa: SLF001
                        sub_props.append(th.Property(clean_field, th.StringType()))
                    else:
                        sub_props.append(
                            th.Property(
                                clean_field,
                                th.ArrayType(th.IntegerType()),
                            ),
                        )
                return th.ArrayType(th.ObjectType(*sub_props))
            return th.ArrayType(th.ObjectType())
        msg = f"Type not found for field: {field}"
        raise RuntimeError(msg)

    @property
    @lru_cache  # noqa: B019
    def schema(self) -> dict:
        properties: th.List[th.Property] = []
        for field in COLUMN_LIST:
            properties.append(th.Property(field, self._get_datatype(field)))
        for breakdown in self._report_definition["breakdowns"]:
            properties.append(th.Property(breakdown, th.StringType()))
        return th.PropertiesList(*properties).to_dict()

    @property
    def oldest_allowed_start_date(self) -> pendulum.Date:
        """
        Facebook stores metrics for a maximum of 37 months. 
        Returns the oldest date that can be fetched from the API.
        """
        return pendulum.today().date().subtract(months=36)
    
    def _initialize_client(self, account_id) -> None:
        FacebookAdsApi.init(
            access_token=self.config["access_token"],
            timeout=300,
            api_version=self.config["api_version"],
        )
        fb_user.User(fbid="me")
        self.account:AdAccount = AdAccount(f"act_{account_id}").api_get()
        if not self.account:
            msg = f"Couldn't find account with id {account_id}"
            raise RuntimeError(msg)

    def _get_selected_columns(self) -> list[str]:
        columns = [
            keys[1] for keys, data in self.metadata.items() if data.selected and len(keys) > 0
        ]
        if not columns and self.name == "adsinsights_default":
            columns = list(self.schema["properties"])
        return columns

    def _get_earliest_record_date(self, account_id: str, sync_end_date: pendulum.Date) -> pendulum.Date | None:
        """
        Make a single Insights API call using sort to determine the oldest date with data.
        Returns None if no data exists. Will retry on 500 errors and other transient failures.
        """
        config_start_date = pendulum.parse(self.config["start_date"]).date()
        start_date = max(config_start_date, self.oldest_allowed_start_date)
        
        params = {
            "level": self._report_definition["level"],
            "fields": ["date_start", "ad_id", "impressions", "date_stop", "created_time"],
            "sort": ["created_time_ascending"],
            "time_range": {
                "since": start_date.format("YYYY-MM-DD"),
                "until": sync_end_date.format("YYYY-MM-DD"),
            },
        }
        api: FacebookAdsApi = FacebookAdsApi.get_default_api()
        url = f"https://graph.facebook.com/{self.config['api_version']}/act_{account_id}/insights"
        try:
            response = api.call("GET", url, params=params)
            data = response.json().get("data", [])
            if data:
                earliest_date = pendulum.parse(data[0]["created_time"]).date()
                self.logger.info(f"Earliest record found: {earliest_date}")
                return earliest_date
            else:
                self.logger.info("No data found for the specified date range")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching earliest record date: {e}")
            raise e



    def _get_start_date(
        self,
        context: dict | None,
    ) -> pendulum.Date:
        lookback_window = self._report_definition["lookback_window"]

        config_start_date = pendulum.parse(self.config["start_date"]).date()
        incremental_start_date = pendulum.parse(
            self.get_starting_replication_key_value(context),
        ).date()
        lookback_start_date = incremental_start_date.subtract(days=lookback_window)

        # Don't use lookback if this is the first sync. Just start where the user requested.
        if config_start_date >= incremental_start_date:
            report_start = config_start_date
            self.logger.info("Using configured start_date as report start filter.")
        else:
            self.logger.info(
                "Incremental sync, applying lookback '%s' to the "
                "bookmark start_date '%s'. Syncing "
                "reports starting on '%s'.",
                lookback_window,
                incremental_start_date,
                lookback_start_date,
            )
            report_start = lookback_start_date

        # Facebook store metrics maximum of 37 months old. Any time range that
        # older that 37 months from current date would result in 400 Bad request
        # HTTP response.
        # https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/#overview
        if report_start < self.oldest_allowed_start_date:
            self.logger.info(
                "Report start date '%s' is older than 37 months. "
                "Using oldest allowed start date '%s' instead.",
                report_start,
                self.oldest_allowed_start_date,
            )
            report_start = self.oldest_allowed_start_date
        return report_start

    def _execute_single_request_with_retries(
        self,
        api: FacebookAdsApi,
        batch_request: dict,
    ) -> dict:
        """
        Execute a single batch request (wrapped in a list) with retry logic.
        Retries individual requests in case of transient errors (like rate limiting or 500 errors).
        """
        attempt = 0
        sleep_time = BACKOFF_INITIAL_SLEEP
        while attempt < BACKOFF_MAX_RETRIES:
            try:
                response = api.call("POST", ["/"], params={"batch": json.dumps([batch_request])})
                resp = response.json()[0]
                if resp.get("code") == 200:
                    return resp
                elif "#80000" in resp.get("body", ""):
                    self.logger.warning(
                        "Rate Limit Reached on individual request. Cooling for %s seconds. Attempt %s/%s",
                        sleep_time,
                        attempt + 1,
                        BACKOFF_MAX_RETRIES,
                    )
                    time.sleep(sleep_time)
                    sleep_time *= 2  # Exponential backoff.
                    attempt += 1
                elif resp.get("code") == 500:
                    self.logger.warning(
                        "500 Server Error on individual request. Retrying in %s seconds. Attempt %s/%s",
                        sleep_time,
                        attempt + 1,
                        BACKOFF_MAX_RETRIES,
                    )
                    time.sleep(sleep_time)
                    sleep_time *= 2  # Exponential backoff.
                    attempt += 1
                elif resp.get("code") == 400 and "too many calls" in resp.get("body", ""):
                    self.logger.warning(
                        "Rate Limit Reached on individual request. Cooling for %s seconds. Attempt %s/%s",
                        sleep_time,
                        attempt + 1,
                        BACKOFF_MAX_RETRIES,
                    )
                    time.sleep(sleep_time)
                    sleep_time *= 2  # Exponential backoff.
                    attempt += 1
                else:
                    raise RuntimeError(f"Individual request failed with non-retryable error: {resp}")
            except Exception as e:
                self.logger.error(
                    "Error during individual request retry: %s. Attempt %s/%s", e, attempt + 1, BACKOFF_MAX_RETRIES
                )
                time.sleep(sleep_time)
                sleep_time *= 2
                attempt += 1
        raise RuntimeError("Max retries exceeded for individual request")
    
    def get_records(
        self,
        context: dict | None,
    ) -> t.Iterable[dict | tuple[dict, dict | None]]:
        account_id = context.get("account_id")
        if not account_id:
            raise RuntimeError("Account ID not found in context.")
        self._initialize_client(account_id)

        time_increment = self._report_definition["time_increment_days"]

        end_date = self.config.get("end_date")
        if end_date:
            sync_end_date = pendulum.parse(end_date).date()
        else:
            sync_end_date = pendulum.today().date()

        # Determine the starting point from our bookmark or config
        report_start_consolidated = self._get_start_date(context)
        # Adjust start date using the sorting approach to find the earliest record.
        earliest_data_date = self._get_earliest_record_date(account_id, sync_end_date)
        
        # If no data exists, exit early
        if earliest_data_date is None:
            self.logger.info("No data exists for this account in the specified date range. Exiting.")
            return
            
        if earliest_data_date > report_start_consolidated:
            self.logger.info(
                "Adjusting report start from %s to earliest available date %s.",
                report_start_consolidated.to_date_string(),
                earliest_data_date.to_date_string(),
            )
            report_start_consolidated = earliest_data_date

        columns = self._get_selected_columns()
        self.logger.info(f"Syncing reports starting from {report_start_consolidated.to_date_string()} to {sync_end_date.to_date_string()}")
        while report_start_consolidated < sync_end_date:
            report_start = report_start_consolidated
            # Prepare batch requests
            batch_requests = []
            batch_final_dates = []
            days_to_fetch = min(sync_end_date.diff(report_start).days+1,BATCH_SIZE)
            for day_offset in range(1, days_to_fetch+1, time_increment):
                batch_params = {
                    "level": self._report_definition["level"],
                    "action_breakdowns": self._report_definition["action_breakdowns"],
                    "action_report_time": self._report_definition["action_report_time"],
                    "breakdowns": self._report_definition["breakdowns"],
                    "fields": columns,
                    "time_increment": time_increment,
                    "limit": 100,
                    "action_attribution_windows": [
                        self._report_definition["action_attribution_windows_view"],
                        self._report_definition["action_attribution_windows_click"],
                    ],
                    "time_range": {
                        "since": (report_start).to_date_string(),
                        "until": (report_start.add(days=(time_increment - 1))).to_date_string(),
                    },
                }
                batch_requests.append({
                    "method": "GET",
                    "relative_url": f"act_{account_id}/insights?{urlencode(batch_params)}",
                })
                report_start = report_start.add(days=time_increment)
                batch_final_dates.append(report_start)
            # Execute the batch
            api:FacebookAdsApi = FacebookAdsApi.get_default_api()
            self.check_limit(account_id)
            batch_response = api.call("POST", ["/"], params={"batch": json.dumps(batch_requests)})

            # Process batch responses
            for final_date, batch_request, response in zip(batch_final_dates, batch_requests, batch_response.json()):
                if response.get("code") == 200:
                    data = json.loads(response["body"])
                else:
                    # Handle rate limits, 500 errors, or other transient failures
                    if "#80000" in response.get("body", ""):
                        self.logger.warning(
                            "Batch request for date %s failed due to rate limiting. Retrying individual request.",
                            final_date.to_date_string(),
                        )
                        response = self._execute_single_request_with_retries(api, batch_request)
                        data = json.loads(response["body"])
                    elif response.get("code") == 500:
                        self.logger.warning(
                            "Batch request for date %s failed with 500 error. Retrying individual request.",
                            final_date.to_date_string(),
                        )
                        response = self._execute_single_request_with_retries(api, batch_request)
                        data = json.loads(response["body"])
                    elif response.get("code") == 400 and "too many calls" in response.get("body", ""):
                        self.logger.warning(
                            "Batch request for date %s failed due to rate limiting. Retrying individual request.",
                            final_date.to_date_string(),
                        )
                        response = self._execute_single_request_with_retries(api, batch_request)
                        data = json.loads(response["body"])
                    else:
                        raise RuntimeError(f"Batch request failed: {response}")
                if data.get("data") and len(data["data"]) > 0:
                    self.logger.info(
                        "%s records fetched for %s",
                        len(data["data"]),
                        final_date.to_date_string(),
                    )
                    for record in data["data"]:
                        yield record
                else:
                    self.logger.info("No records fetched for %s", final_date.to_date_string())
                report_start_consolidated = final_date
                    
        self.logger.info("Syncing reports completed.")

    #Function to find the string between two strings or characters
    def find_between(self, usage, find ) -> str:
        try:
            usage = json.loads(usage)
            for key in usage.keys():
                # Access the list corresponding to the current key and iterate through its elements
                for item in usage[key]:
                    if find in item:
                        return item[find]
        except ValueError:
            return 0

    #Function to check how close you are to the FB Rate Limit
    def get_limit(self, account_id)->float:
        """
        This function makes a GET request to the Facebook Graph API to retrieve
        the usage limit for the Ads Insights endpoint and returns the maximum
        usage among the call count, CPU time, and total time.

        Returns:
            float: The maximum usage among call count, CPU time, and total time.
        """
        try:
            # Make a GET request to the Facebook Graph API to retrieve the usage limit
            check = rq.get(
                'https://graph.facebook.com/' +
                self.config["api_version"] +
                '/act_' +
                account_id +
                '/insights?access_token=' +
                self.config["access_token"]
            )

            # Check if the request was successful
            check.raise_for_status()

            # Parse the usage limit from the headers of the response
            usage_data = check.headers.get("x-business-use-case-usage", "")

            if usage_data:
                # Extract call count, CPU time, and total time
                call = float(self.find_between(usage_data, "call_count"))
                cpu = float(self.find_between(usage_data, "total_cputime"))
                total = float(self.find_between(usage_data, "total_time"))

                # Find the maximum usage among call count, CPU time, and total time
                usage = max(call, cpu, total)
                return usage
            else:
                self.logger.warning("No usage data found in the response headers.")
                return 0.0  # Default value if no data is available

        except rq.exceptions.RequestException as e:
            self.logger.error(f"Request to Facebook API failed: {e}")
            return 0.0  # Default value in case of request failure

        except KeyError as e:
            self.logger.error(f"KeyError: {e} - Check if expected headers are present.")
            return 0.0  # Default value if key is not found

        except ValueError as e:
            self.logger.error(f"ValueError: {e} - Unable to convert usage data to float.")
            return 0.0  # Default value if conversion fails

    def check_limit(self, account_id) -> None:
        #Check if you reached 75% of the limit, if yes then back-off for 5 minutes
        if (self.get_limit(account_id)>USAGE_LIMIT_THRESHOLD):
            #After threshold is reached start throttling all consecutive requests until the limit is reset.
            self.logger.warning(f"{USAGE_LIMIT_THRESHOLD}% Rate Limit Reached. Cooling Time 5 Minutes.")
            time.sleep(300)
