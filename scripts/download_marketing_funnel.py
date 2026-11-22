"""Download the Olist marketing funnel dataset (2 CSVs) into data/raw.

Dataset page: https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist

What it contains: sellers who signed up to sell on Olist.
  - marketing qualified leads (MQLs): a lead's first contact, its landing page and origin channel
  - closed deals: the leads that became sellers, with business segment and sales rep
It joins to the e-commerce dataset through seller_id (closed_deals.seller_id ->
olist_sellers_dataset.seller_id), so we can follow a seller from first ad click
to the orders they later receive. That is the same question ShopBack asks about
merchants acquired through campaigns.
"""
import sys

from kaggle_download import download

DATASET = "olistbr/marketing-funnel-olist"
EXPECTED = [
    "olist_marketing_qualified_leads_dataset.csv",
    "olist_closed_deals_dataset.csv",
]

if __name__ == "__main__":
    sys.exit(download(DATASET, EXPECTED))
