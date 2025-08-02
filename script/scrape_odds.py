"""
Scrape historic UFC odds from betmma.tips

The work below draws lightly upon another data scientist's work here in identifying an appropriate site to 
scrape odds from as well as some logic to scrape required elements.
    https://github.com/jasonchanhku/web_scraping/blob/master/MMA%20Project/favourite_vs_underdogs.R

This required significant additional effort of my own to rewrite in Python (instead of R), scrape from a 
different page (above work scrapes from a page missing some events), write appropriate pipeline-ready code, 
as well as significant updates to logic which was no longer working correctly  for latest event pages 
- e.g., updating the logic to correctly fetch fighter1, fighter2 and result, improve logic to run faster

"""

import pandas as pd
import numpy as np
import datetime

import requests
from bs4 import BeautifulSoup

import utils

class OddsScraper():
    """Scrape historic odds from betmma.tips"""
    def __init__(self, test=False):
        self.test = test
        self.all_url = "https://www.betmma.tips/past_mma_handicapper_performance_all.php?Org=1"
        self.event_links = None
        self.event_odds = None
        self.curr_time = datetime.datetime.now()

    def get_individual_event_urls(self):
        """Get all individual urls"""

        # lambdaで実行する際にbot扱いとされるのを回避するため
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive"
        }

        response = requests.get(self.all_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        links = [
                    f"http://www.betmma.tips/{a['href']}" for a in soup.select("td td td td a")
                ]
        
        # Manually parse the event table
        # Here, find all <table> tags and target the 9th table (index 8)
        tables = soup.find_all('table')

        target_table = tables[8]  # 9th table (0-indexed)

        rows = target_table.find_all('tr')

        dates = []
        events = []

        for row in rows[1:-1]:  # Skip the first header row and last footer row
            cols = row.find_all('td')
            if len(cols) >= 2:
                date_text = cols[0].get_text(strip=True)
                event_text = cols[1].get_text(strip=True)
                dates.append(date_text)
                events.append(event_text)

        # Convert to DataFrame
        table_df = pd.DataFrame({
            "Date": dates,
            "Event": events,
            "link": links
        })

        # Filter for UFC events only
        table_df = table_df[
            table_df["Event"].str.contains("UFC") &
            ~table_df["Event"].str.contains("Road to UFC")
        ].reset_index(drop=True)

        self.event_links = table_df

    def scrape_event_odds(self, target_df: pd.DataFrame = None) -> None:
        """Iterate over all individual urls to scrape odds"""

        scraped_results = []

        table = target_df if target_df is not None else self.event_links

        if table is None or table.empty:
            print("No data to scrape.")
            self.event_odds = pd.DataFrame()  # 空のDataFrameを代入
            return

        for i, row in table.iterrows():
            print(f"{i+1}/{len(table)} - {row.Date} - {row.link}")

            results = self._scrape_event_odds_page(row.link)
            results["link"] = row.link
            results["date"] = row.Date
            scraped_results.append(results)

            utils.sleep_randomly()

        odds_df = pd.concat(scraped_results).reset_index(drop=True)

        odds_df["timestamp"] = self.curr_time

        self.event_odds = odds_df

    def write_data(self):
        self.event_odds.to_csv("./data/odds_raw.csv", index=False)

    def _scrape_event_odds_page(self, link):
        # lambdaで実行する際にbot扱いとされるのを回避するため
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive"
        }

        sub_response = requests.get(link, headers=headers)
        sub_soup = BeautifulSoup(sub_response.text, 'html.parser')

        event = []
        fighters = []
        fighter1 = []
        fighter2 = []
        fighter1_odds = []
        fighter2_odds = []
        result = []

        # Get fighter names and winner from a tags - no ids or classes to work with
        # so have to do this way
        # - Get all fighter profile links
        # - First one should be fighter1
        # - Second one should be fighter2
        # - Next should give result - but where it is neither fighter1 or fighter2 
        #   and is a new fighter, this is because a draw or N/C was returned
        #   so assume it is a new fighter1

        for a in sub_soup.select("td > a[href*='fighter_profile']"):
            if '\xa0' not in a.next_sibling:
                fighters.append(a.get_text())

        increment = "fighter1"

        for i in fighters:
            
            if increment == "fighter1":
                fighter1.append(i)
                increment = "fighter2"

            elif increment == "fighter2":
                fighter2.append(i)
                increment = "result"
            else:
                # draw - skip to next fight
                if (i not in fighter1) and (i not in fighter2):
                    result.append("-")
                    fighter1.append(i)
                    increment = "fighter2"
                else:
                    result.append(i)
                    increment = "fighter1"

        # Handling for edge case - last fight on list is a draw
        if len(result) == len(fighter1) - 1:
            print("Edge case - appending '-' to result")
            result.append("-")

        # Event
        event_t = sub_soup.select("td h1")[0].get_text()
        event.extend([event_t] * len(fighter1))

        # Label
        # Exact match is sub_soup.select("td td td td tr~ tr+ tr td") but this
        # is very slow especially on large pages
        # This is less precise but works on pages I tested
        label_t = [
                    td.get_text() for td in sub_soup.select("td tr+ tr td") \
                    if (len(td.get_text()) <= 7) and \
                        "@" in td.get_text()
                ]


        label_cleansed = [t.replace("@", "").strip() for t in label_t]

        # Fighter1 odds
        fighter1_odds_t = label_cleansed[0::2]
        fighter1_odds.extend(fighter1_odds_t)

        # Fighter2 odds
        fighter2_odds_t = label_cleansed[1::2]
        fighter2_odds.extend(fighter2_odds_t)

        return pd.DataFrame({
            "link": np.nan,
            "date": np.nan,
            "event": event,
            "fighter1": fighter1,
            "fighter2": fighter2,
            "fighter1_odds": fighter1_odds,
            "fighter2_odds": fighter2_odds,
            "result": result
        })
