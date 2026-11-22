"""Download the Olist Brazilian e-commerce dataset (9 CSVs) into data/raw.

Dataset page: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
"""
import sys

from kaggle_download import download

DATASET = "olistbr/brazilian-ecommerce"
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

if __name__ == "__main__":
    sys.exit(download(DATASET, EXPECTED))
