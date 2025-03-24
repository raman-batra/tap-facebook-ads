import typing as t
import json
import pendulum
from tap_facebook.client import FacebookStream
from tap_facebook.streams import AdAccountsStream

class AccountLevelStream(FacebookStream):
    """Account level stream class."""
    parent_stream_type = AdAccountsStream

    @property
    def url_base(self) -> str:
        return super().url_base + "/act_{account_id}"

class IncrementalFacebookStream(AccountLevelStream):
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": 25}
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
            ts = pendulum.parse(self.get_starting_replication_key_value(context))
            params["filtering"] = json.dumps(
                [
                    {
                        "field": f"{self.filter_entity}.{self.replication_key}",
                        "operator": "GREATER_THAN",
                        "value": int(ts.timestamp()),
                    },
                ],
            )

        return params


