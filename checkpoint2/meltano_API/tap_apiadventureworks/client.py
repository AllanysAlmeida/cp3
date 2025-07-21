"""REST client handling, including APIAdventureWorksStream base class."""

from __future__ import annotations

import decimal
import typing as t
from importlib import resources

from dotenv import load_dotenv
from os import getenv

from requests.auth import HTTPBasicAuth
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

SCHEMAS_DIR = resources.files(__package__) / "schemas"

class NewPaginator(BaseOffsetPaginator):
    def over (self, response):
        data = response.json()
        total = data.get("total", 0)
        offset = data.get("offset", 0)
        limit = data.get("limit", 15000)
        return offset + limit < total

class APIAdventureWorksStream(RESTStream):
    """APIAdventureWorks stream class."""

    records_jsonpath = "$.data[*]"

    next_page_token_jsonpath = "$.next_page"

    @property
    def url_base(self) -> str:
        return "http://18.209.218.63:8080"

    @property
    def authenticator(self) -> HTTPBasicAuth:

        return HTTPBasicAuth(
            username=self.config.get("username", ""),
            password=self.config.get("password", "")
        )

    @property
    def http_headers(self) -> dict:
        return {}

    def get_new_paginator(self):
        limit = self.config.get("limit", 15000)
        return NewPaginator(start_value=0, page_size=limit)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        
        params: dict = {
            "limit": self.config.get("limit", 15000),
            "offset": next_page_token or 0,
        }
    
        return params

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        return None

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
 
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        return row
