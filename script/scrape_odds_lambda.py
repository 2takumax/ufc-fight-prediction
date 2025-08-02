import string
import pandas as pd
import datetime
import argparse

import utils
from scrape_odds import OddsScraper

def lambda_handler(event, context):
    bucket_name = "ufc-scraped-data"
    bucket_key = "odds.csv"

    odds_scraper = OddsScraper()
    odds_scraper.get_individual_event_urls()
    existing_df = utils.read_s3_csv(bucket=bucket_name, key=bucket_key)
    target_df = utils.filter_new_records(existing_df=existing_df, target_df=odds_scraper.event_links, key_column="link")

    odds_scraper.scrape_event_odds(target_df=target_df)

    utils.append_to_s3_csv(new_df=odds_scraper.event_odds, bucket=bucket_name, key=bucket_key)
