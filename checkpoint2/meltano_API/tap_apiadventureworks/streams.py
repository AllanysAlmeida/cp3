from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th 

from tap_apiadventureworks.client import APIAdventureWorksStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"

class SalesOrderDetailStream(APIAdventureWorksStream):

    name = "SalesOrderDetail"
    path = "/SalesOrderDetail"

    schema_filepath = SCHEMAS_DIR / "salesorderdetail.json"  

class SalesOrderHeaderStream(APIAdventureWorksStream):
    
    name = "SalesOrderHeader"
    path = "/SalesOrderHeader"
    schema_filepath = SCHEMAS_DIR / "salesorderheader.json"

class PurchaseOrderDetailStream(APIAdventureWorksStream):
    
    name = "PurchaseOrderDetail"
    path = "/PurchaseOrderDetail"
    schema_filepath = SCHEMAS_DIR / "purchaseorderdetail.json"

class PurchaseOrderHeaderStream(APIAdventureWorksStream):
    
    name = "PurchaseOrderHeader"
    path = "/PurchaseOrderHeader"
    schema_filepath = SCHEMAS_DIR / "purchaseorderheader.json"
