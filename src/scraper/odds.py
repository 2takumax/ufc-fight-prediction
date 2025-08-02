"""UFC試合オッズスクレイパー"""

from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from bs4 import BeautifulSoup

from src.scraper.base import BaseScraper
from src.utils.web import clean_text


class OddsScraper(BaseScraper):
    """UFC試合のオッズ情報をスクレイピングするクラス"""
    
    def __init__(self, test_mode: bool = False, update_mode: bool = False, config: Optional[Any] = None):
        super().__init__(test_mode, update_mode, config)
        self.all_events_url = self.config.betmma["all_events_url"]
        self.event_data = pd.DataFrame()
        self.existing_event_links = set()
    
    def get_output_path(self) -> str:
        """出力ファイルパスを取得"""
        return self.config.output["odds_file"]
    
    def _get_event_urls(self) -> pd.DataFrame:
        """イベント一覧を取得"""
        self.logger.info("Fetching event list from BetMMA")
        
        soup = self.get_soup(self.all_events_url)
        
        # リンクを取得
        links = []
        for a in soup.select("td td td td a"):
            if 'href' in a.attrs:
                links.append(f"http://www.betmma.tips/{a['href']}")
        
        # イベントテーブルをパース
        event_data = self._parse_event_table(soup, links)
        
        # UFCイベントのみフィルタ
        ufc_events = event_data[
            event_data["Event"].str.contains("UFC", na=False) &
            ~event_data["Event"].str.contains("Road to UFC", na=False)
        ].reset_index(drop=True)
        
        self.logger.info(f"Found {len(ufc_events)} UFC events")
        
        return ufc_events
    
    def _parse_event_table(self, soup: BeautifulSoup, links: List[str]) -> pd.DataFrame:
        """イベントテーブルをパース"""
        # テーブルを探す（9番目のテーブル）
        tables = soup.find_all('table')
        if len(tables) <= 8:
            self.logger.error("Event table not found")
            return pd.DataFrame()
        
        target_table = tables[8]
        rows = target_table.find_all('tr')
        
        dates = []
        events = []
        
        for row in rows[1:-1]:  # ヘッダーとフッターを除外
            cols = row.find_all('td')
            if len(cols) >= 2:
                dates.append(clean_text(cols[0].text))
                events.append(clean_text(cols[1].text))
        
        # リンクの数と合わせる
        min_length = min(len(dates), len(events), len(links))
        
        return pd.DataFrame({
            "Date": dates[:min_length],
            "Event": events[:min_length],
            "link": links[:min_length]
        })
    
    def _parse_odds_page(self, url: str) -> pd.DataFrame:
        """個別のオッズページをパース"""
        soup = self.get_soup(url)
        
        # イベント名を取得
        event_name = ""
        h1_elem = soup.select_one("td h1")
        if h1_elem:
            event_name = clean_text(h1_elem.text)
        
        # ファイター情報を抽出
        fighters_data = self._extract_fighters_and_results(soup)
        
        # オッズ情報を抽出
        odds_data = self._extract_odds(soup)
        
        # データを結合
        results = []
        for i, (f1, f2, result) in enumerate(fighters_data):
            if i < len(odds_data):
                odds1, odds2 = odds_data[i]
            else:
                odds1, odds2 = "", ""
            
            results.append({
                "event": event_name,
                "fighter1": f1,
                "fighter2": f2,
                "fighter1_odds": odds1,
                "fighter2_odds": odds2,
                "result": result
            })
        
        return pd.DataFrame(results)
    
    def _extract_fighters_and_results(self, soup: BeautifulSoup) -> List[Tuple[str, str, str]]:
        """ファイター名と結果を抽出"""
        fighters = []
        
        # ファイタープロフィールリンクを取得
        for a in soup.select("td > a[href*='fighter_profile']"):
            # 次の兄弟要素をチェック（空白文字を除外）
            if a.next_sibling and '\xa0' not in str(a.next_sibling):
                fighters.append(clean_text(a.text))
        
        # ファイターをペアに分割し、結果を判定
        fights = []
        i = 0
        
        while i < len(fighters):
            if i + 1 < len(fighters):
                fighter1 = fighters[i]
                fighter2 = fighters[i + 1]
                
                # 次のファイターが結果を示すかチェック
                if i + 2 < len(fighters):
                    next_fighter = fighters[i + 2]
                    
                    # 既に登場したファイターなら結果
                    if next_fighter in [fighter1, fighter2]:
                        result = next_fighter
                        i += 3
                    else:
                        # 新しいファイターなら引き分け
                        result = "-"
                        i += 2
                else:
                    # 最後の試合
                    result = "-"
                    i += 2
                
                fights.append((fighter1, fighter2, result))
            else:
                break
        
        return fights
    
    def _extract_odds(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        """オッズ情報を抽出"""
        odds_pairs = []
        
        # オッズラベルを取得
        odds_labels = []
        for td in soup.select("td tr+ tr td"):
            text = clean_text(td.text)
            if len(text) <= 7 and "@" in text:
                odds_labels.append(text.replace("@", "").strip())
        
        # ペアに分割
        for i in range(0, len(odds_labels), 2):
            if i + 1 < len(odds_labels):
                odds_pairs.append((odds_labels[i], odds_labels[i + 1]))
        
        return odds_pairs
    
    
    def _load_existing_odds(self) -> None:
        """既存のオッズデータを読み込んで、処理済みイベントを記録"""
        if self.update_mode:
            existing_data = self.load_existing_data(self.get_output_path())
            if existing_data is not None and 'link' in existing_data.columns:
                self.existing_event_links = set(existing_data['link'].unique())
                self.logger.info(f"Found {len(self.existing_event_links)} existing event links")
    
    def _filter_new_events(self, event_data: pd.DataFrame) -> pd.DataFrame:
        """新規イベントのみをフィルタリング"""
        if not self.update_mode or len(self.existing_event_links) == 0:
            return event_data
        
        # 既存のリンクを除外
        new_events = event_data[~event_data['link'].isin(self.existing_event_links)]
        self.logger.info(f"Found {len(new_events)} new events out of {len(event_data)} total events")
        
        return new_events
    
    def scrape(self) -> pd.DataFrame:
        """スクレイピングを実行"""
        # 既存データを読み込む
        self._load_existing_odds()
        
        # イベント一覧を取得
        all_event_data = self._get_event_urls()
        
        if all_event_data.empty:
            self.logger.warning("No events found to scrape")
            return pd.DataFrame()
        
        # 差分更新モードの場合、新規イベントのみをフィルタ
        if self.update_mode:
            self.event_data = self._filter_new_events(all_event_data)
            if self.event_data.empty:
                self.logger.info("No new events to scrape")
                return pd.DataFrame()
        else:
            self.event_data = all_event_data
        
        # 各イベントのオッズを取得
        def process_event(row):
            try:
                # 差分更新モードで既存イベントの場合はスキップ
                if self.update_mode and row['link'] in self.existing_event_links:
                    self.logger.debug(f"Skipping existing event: {row['Event']}")
                    return pd.DataFrame()
                
                odds_data = self._parse_odds_page(row['link'])
                if not odds_data.empty:
                    odds_data['date'] = row['Date']
                    odds_data['link'] = row['link']
                    self.logger.info(f"Scraped odds for: {row['Event']}")
                return odds_data
            except Exception as e:
                self.logger.error(f"Failed to scrape odds for {row['Event']}: {e}")
                return pd.DataFrame()
        
        # プログレスバー付きで処理
        results = self.process_with_progress(
            [row for _, row in self.event_data.iterrows()],
            process_event,
            desc="Scraping odds"
        )
        
        # 結果を結合
        if results:
            return pd.concat([r for r in results if not r.empty], ignore_index=True)
        
        return pd.DataFrame()