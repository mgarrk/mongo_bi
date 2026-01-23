import os
import csv
from typing import Optional, Dict, Any, List

from pymongo import MongoClient, UpdateOne

from datetime import datetime, timezone



MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:adminpass@localhost:27018/?authSource=admin")
DB_NAME = os.getenv("MONGO_DB", "pet_analytics")
COLLECTION = os.getenv("MONGO_COLLECTION", "raw_invoices")

CSV_PATH = os.getenv("CSV_PATH", "data.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))


def parse_int(x: str) -> Optional[int]:
    x = (x or "").strip()
    if x == "":
        return None
    return int(float(x))  


def parse_float(x: str) -> Optional[float]:
    x = (x or "").strip()
    if x == "":
        return None
    return float(x)


def parse_dt(x: str) -> datetime:
    # (M/D/YYYY H:MM)
    return datetime.strptime(x.strip(), "%m/%d/%Y %H:%M")


def make_line(row: Dict[str, str]) -> Dict[str, Any]:
    qty = parse_int(row["Quantity"]) or 0
    price = parse_float(row["UnitPrice"]) or 0.0
    return {
        "stockCode": row["StockCode"].strip(),
        "description": (row.get("Description") or "").strip(),
        "qty": qty,
        "unitPrice": price,
        "lineTotal": qty * price,
    }


def flush_ops(col, ops: List[UpdateOne]):
    if not ops:
        return 0
    res = col.bulk_write(ops, ordered=False)


def main():
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION]

    col.drop()

    # полезные индексы
    col.create_index([("invoiceDate", 1)])
    col.create_index([("country", 1)])
    col.create_index([("customerId", 1)])
    col.create_index([("lines.stockCode", 1)])

    ops: List[UpdateOne] = []
    processed_lines = 0

    with open(CSV_PATH, "r", encoding="cp1252", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            invoice_no = row["InvoiceNo"].strip()
            invoice_dt = parse_dt(row["InvoiceDate"])
            customer_id = parse_int(row.get("CustomerID", ""))
            country = (row.get("Country") or "").strip()

            line = make_line(row)

            # можно отделять отмены/возвраты:
            # cancellations = invoice_no.startswith("C") or line["qty"] < 0
            is_cancelled = invoice_no.startswith("C") or line["qty"] < 0

            update = UpdateOne(
                {"_id": invoice_no},
                {
                    "$setOnInsert": {
                        "_id": invoice_no,
                        "invoiceDate": invoice_dt,
                        "customerId": customer_id,
                        "country": country,
                        "isCancelled": is_cancelled,
                        "source": "kaggle_online_retail",
                        "ingestedAt": datetime.now(timezone.utc),
                    },
                    "$push": {"lines": line},
                    "$inc": {
                        "linesCount": 1,
                        "orderTotal": line["lineTotal"],
                    },
                },
                upsert=True,
            )

            ops.append(update)
            processed_lines += 1

            if len(ops) >= BATCH_SIZE:
                flush_ops(col, ops)
                ops = []
                if processed_lines % (BATCH_SIZE * 10) == 0:
                    print(f"Processed lines: {processed_lines}")

    if ops:
        flush_ops(col, ops)

    print(f"Done. Processed lines: {processed_lines}")
    print("Example invoice:", col.find_one()) 


if __name__ == "__main__":
    main()
