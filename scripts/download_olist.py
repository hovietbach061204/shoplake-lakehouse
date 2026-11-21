"""Download the Olist Brazilian e-commerce dataset into data/raw.

Uses the official Kaggle API (https://github.com/Kaggle/kaggle-api), which reads
your token from ~/.kaggle/kaggle.json. The script is idempotent: if all nine
files are already there it does nothing, so re-running it is always safe.
That is the same property we will want from every pipeline step later.
"""
from pathlib import Path
import sys

DATASET = "olistbr/brazilian-ecommerce"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
EXPECTED = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]


def missing_files() -> list[str]:
    return [f for f in EXPECTED if not (RAW_DIR / f).exists()]


def main() -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if not missing_files():
        print(f"All {len(EXPECTED)} files already in {RAW_DIR}, nothing to do.")
        return 0

    from kaggle.api.kaggle_api_extended import KaggleApi  # imported late: needs a token

    api = KaggleApi()
    api.authenticate()
    print(f"Downloading {DATASET} into {RAW_DIR} ...")
    api.dataset_download_files(DATASET, path=str(RAW_DIR), unzip=True, quiet=False)

    still_missing = missing_files()
    if still_missing:
        print("Missing after download:", still_missing, file=sys.stderr)
        return 1
    for f in EXPECTED:
        size_mb = (RAW_DIR / f).stat().st_size / 1_000_000
        print(f"  {f:45s} {size_mb:7.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
