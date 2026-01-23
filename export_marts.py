import os
import json
import csv
from datetime import datetime, date
from typing import Any, Dict, List

from pymongo import MongoClient
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId


# config env vars
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:adminpass@localhost:27018/?authSource=admin")
DB_NAME = os.getenv("MONGO_DB", "pet_analytics")
COLLECTION = os.getenv("MONGO_COLLECTION", "raw_invoices")

OUT_DIR = os.getenv("OUT_DIR", "out")

# debug
LIMIT = os.getenv("LIMIT")  
LIMIT = int(LIMIT) if LIMIT else None


# data export pipeline
def export_pipeline_to_csv(col, pipeline: List[Dict[str, Any]], csv_path: str, limit: int | None = None) -> str:
    """
    - datetime -> 'YYYY-MM-DD'
    - Decimal128 -> float
    - dict/list -> JSON string in a single cell
    """
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)

    cursor = col.aggregate(pipeline, allowDiskUse=True)

    first_doc = None
    for doc in cursor:
        first_doc = doc
        break

    if first_doc is None:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            pass
        return csv_path

    def normalize_value(v):
        if v is None:
            return ""
        if isinstance(v, (datetime, date)):
            return v.strftime("%Y-%m-%d")
        if isinstance(v, Decimal128):
            return float(v.to_decimal())
        if isinstance(v, ObjectId):
            return str(v)
        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False)
        return v

    fieldnames = list(first_doc.keys())

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        writer.writerow({k: normalize_value(first_doc.get(k)) for k in fieldnames})
        rows_written = 1

        if limit is not None and rows_written >= limit:
            return csv_path

        for doc in cursor:
            writer.writerow({k: normalize_value(doc.get(k)) for k in fieldnames})
            rows_written += 1
            if limit is not None and rows_written >= limit:
                break

    return csv_path


def main():
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION]

    # Indexes for faster aggregation
    col.create_index([("invoiceDate", 1)])
    col.create_index([("country", 1)])
    col.create_index([("lines.stockCode", 1)])

    # 1) MART: country x month
    # gross/returns/net revenue + orders
    pipeline_country_month = [
        # if cancelled
        {"$addFields": {"isCancellationInvoice": {"$regexMatch": {"input": "$_id", "regex": "^C"}}}},

        # monthStart = первый день месяца (удобно для BI)
        {"$addFields": {
            "monthStart": {
                "$dateFromParts": {
                    "year": {"$year": "$invoiceDate"},
                    "month": {"$month": "$invoiceDate"},
                    "day": 1
                }
            }
        }},

        {"$group": {
            "_id": {"country": "$country", "monthStart": "$monthStart"},
            # orders
            "ordersGross": {"$sum": {"$cond": ["$isCancellationInvoice", 0, 1]}},
            "ordersCancelled": {"$sum": {"$cond": ["$isCancellationInvoice", 1, 0]}},

            # revenue (используем orderTotal, который копим через $inc)
            
            "grossRevenue": {"$sum": {"$cond": ["$isCancellationInvoice", 0, "$orderTotal"]}},
            "returnsRevenue": {"$sum": {"$cond": ["$isCancellationInvoice", "$orderTotal", 0]}},  # отрицательное
            "netRevenue": {"$sum": "$orderTotal"},

            "lines": {"$sum": "$linesCount"},
        }},

        {"$project": {
            "_id": 0,
            "country": "$_id.country",
            "monthStart": "$_id.monthStart",
            "ordersGross": 1,
            "ordersCancelled": 1,
            "lines": 1,
            # Для BI приводим к double в конце
            "grossRevenue": {"$toDouble": "$grossRevenue"},
            "returnsRevenue": {"$toDouble": "$returnsRevenue"},
            "netRevenue": {"$toDouble": "$netRevenue"},
        }},

        {"$sort": {"monthStart": 1, "country": 1}},
    ]

    if LIMIT:
        pipeline_country_month.append({"$limit": LIMIT})

    out1 = os.path.join(OUT_DIR, "mart_country_month.csv")
    export_pipeline_to_csv(col, pipeline_country_month, out1)
    print(f"Saved: {out1}")

    
    # 2) MART: top products (net)
    # по всей выборке: qty, revenue
 
    pipeline_top_products = [
        {"$addFields": {"isCancellationInvoice": {"$regexMatch": {"input": "$_id", "regex": "^C"}}}},
        {"$unwind": "$lines"},

        
        {"$group": {
            "_id": "$lines.stockCode",
            "revenueNet": {"$sum": "$lines.lineTotal"},
            "qtyNet": {"$sum": "$lines.qty"},

            # чтобы отдельно видеть продажи и возвраты (по знаку)
            "revenuePos": {"$sum": {"$cond": [{"$gt": ["$lines.lineTotal", 0]}, "$lines.lineTotal", 0]}},
            "revenueNeg": {"$sum": {"$cond": [{"$lt": ["$lines.lineTotal", 0]}, "$lines.lineTotal", 0]}},

            "lines": {"$sum": 1},
            "exampleDescription": {"$first": "$lines.description"},
        }},
        {"$project": {
            "_id": 0,
            "stockCode": "$_id",
            "description": "$exampleDescription",
            "lines": 1,
            "qtyNet": 1,
            "revenuePos": {"$toDouble": "$revenuePos"},
            "revenueNeg": {"$toDouble": "$revenueNeg"},
            "revenueNet": {"$toDouble": "$revenueNet"},
        }},
        {"$sort": {"revenueNet": -1}},
    ]

    if LIMIT:
        pipeline_top_products.append({"$limit": LIMIT})
    else:
        pipeline_top_products.append({"$limit": 200})

    out2 = os.path.join(OUT_DIR, "mart_top_products.csv")
    export_pipeline_to_csv(col, pipeline_top_products, out2)
    print(f"Saved: {out2}")

   
    # 3) MART: returns rate by country x month
    # returnRateAbs = abs(returnsRevenue) / grossRevenue
 
    pipeline_returns_rate = [
        {"$addFields": {"isCancellationInvoice": {"$regexMatch": {"input": "$_id", "regex": "^C"}}}},
        {"$addFields": {
            "monthStart": {
                "$dateFromParts": {
                    "year": {"$year": "$invoiceDate"},
                    "month": {"$month": "$invoiceDate"},
                    "day": 1
                }
            }
        }},
        {"$group": {
            "_id": {"country": "$country", "monthStart": "$monthStart"},
            "grossRevenue": {"$sum": {"$cond": ["$isCancellationInvoice", 0, "$orderTotal"]}},
            "returnsRevenue": {"$sum": {"$cond": ["$isCancellationInvoice", "$orderTotal", 0]}},
        }},
        {"$project": {
            "_id": 0,
            "country": "$_id.country",
            "monthStart": "$_id.monthStart",
            "grossRevenue": {"$toDouble": "$grossRevenue"},
            "returnsRevenue": {"$toDouble": "$returnsRevenue"},
            "returnRateAbs": {
                "$cond": [
                    {"$gt": ["$grossRevenue", 0]},
                    {"$divide": [{"$abs": "$returnsRevenue"}, "$grossRevenue"]},
                    None
                ]
            }
        }},
        {"$sort": {"monthStart": 1, "country": 1}},
    ]

    if LIMIT:
        pipeline_returns_rate.append({"$limit": LIMIT})

    out3 = os.path.join(OUT_DIR, "mart_returns_country_month.csv")
    export_pipeline_to_csv(col, pipeline_returns_rate, out3)
    print(f"Saved: {out3}")

if __name__ == "__main__":
    main()
