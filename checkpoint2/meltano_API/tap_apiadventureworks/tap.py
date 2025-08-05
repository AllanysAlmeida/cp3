"""APIAdventureWorks tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_apiadventureworks import streams


class TapAPIAdventureWorks(Tap):

    name = "tap-apiadventureworks"

    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> list[streams.APIAdventureWorksStream]:
        return [
            #streams.PurchaseOrderDetailStream(self),
            #streams.PurchaseOrderHeaderStream(self),
            streams.SalesOrderHeaderStream(self),
            streams.SalesOrderDetailStream(self)
        ]


if __name__ == "__main__":
    TapAPIAdventureWorks.cli()
