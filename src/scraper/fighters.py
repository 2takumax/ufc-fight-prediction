"""UFCファイター情報スクレイパー"""

import string
from typing import List, Dict, Any, Optional
import pandas as pd
from bs4 import BeautifulSoup

from src.scraper.base import BaseScraper
from src.utils.web import clean_text
from src.utils.data import parse_fight_record


class FighterScraper(BaseScraper):
    """UFCファイター情報をスクレイピングするクラス"""
    
    def __init__(self, test_mode: bool = False, update_mode: bool = False, config: Optional[Any] = None):
        super().__init__(test_mode, update_mode, config)
        self.fighter_index_url = self.config.ufc_stats["fighter_index_url"]
        self.fighter_urls = []
    
    def get_output_path(self) -> str:
        """出力ファイルパスを取得"""
        return self.config.output["fighters_file"]
    
    def _get_all_fighter_urls(self) -> List[str]:
        """全てのファイターのURLを取得"""
        self.logger.info("Fetching fighter URLs from index pages")
        
        all_fighter_urls = []
        letters = list(string.ascii_lowercase)
        
        for letter in letters:
            page_url = self.fighter_index_url.format(letter=letter)
            
            try:
                soup = self.get_soup(page_url)
                urls = self._extract_fighter_urls_from_page(soup)
                all_fighter_urls.extend(urls)
                
                self.logger.debug(f"Found {len(urls)} fighters for letter '{letter}'")
                self.sleep_randomly()
                
            except Exception as e:
                self.logger.error(f"Failed to fetch fighters for letter '{letter}': {e}")
                continue
        
        # 重複を除去
        unique_urls = list(set(all_fighter_urls))
        self.logger.info(f"Found {len(unique_urls)} unique fighter URLs")
        
        return unique_urls
    
    def _extract_fighter_urls_from_page(self, soup: BeautifulSoup) -> List[str]:
        """ページからファイターURLを抽出"""
        urls = []
        
        link_elements = soup.find_all("a", class_="b-link b-link_style_black")
        for link in link_elements:
            if "href" in link.attrs:
                urls.append(link["href"])
        
        return urls
    
    def _parse_fighter_data(self, fighter_url: str) -> Dict[str, Any]:
        """個別のファイターページからデータをパース"""
        soup = self.get_soup(fighter_url)
        
        fighter_data = {"url": fighter_url}
        
        # ファイター名
        name_elem = soup.find("span", class_="b-content__title-highlight")
        if name_elem:
            fighter_data["name"] = clean_text(name_elem.text)
        
        # 戦績
        record_elem = soup.find("span", class_="b-content__title-record")
        if record_elem:
            record_text = clean_text(record_elem.text.replace("Record:", ""))
            fighter_data["fight_record"] = record_text
            
            # 戦績を分解
            record_parts = parse_fight_record(record_text)
            fighter_data.update(record_parts)
        
        # ニックネーム
        nickname_elem = soup.find("p", class_="b-content__Nickname")
        if nickname_elem:
            fighter_data["nickname"] = clean_text(nickname_elem.text)
        
        # その他の詳細情報
        details = self._extract_fighter_details(soup)
        fighter_data.update(details)
        
        return fighter_data
    
    def _extract_fighter_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """ファイターの詳細情報を抽出"""
        details = {}
        
        # 詳細情報リスト
        detail_items = soup.find_all(
            "li", 
            class_="b-list__box-list-item b-list__box-list-item_type_block"
        )
        
        for item in detail_items:
            text = clean_text(item.text)
            if ":" in text:
                parts = text.split(":", 1)
                if len(parts) == 2:
                    key = parts[0].strip().lower().replace(" ", "_")
                    value = parts[1].strip()
                    
                    # 特殊な処理が必要なフィールド
                    if key == "height":
                        details[key] = value
                        details["height_cm"] = self._convert_height_to_cm(value)
                    elif key == "weight":
                        details[key] = value
                        details["weight_kg"] = self._convert_weight_to_kg(value)
                    elif key == "reach":
                        details[key] = value
                        details["reach_cm"] = self._convert_reach_to_cm(value)
                    else:
                        details[key] = value
        
        return details
    
    def _convert_height_to_cm(self, height_str: str) -> Optional[float]:
        """身長をcmに変換（例: "5' 11\"" -> 180.34）"""
        import re
        
        match = re.match(r"(\d+)'\s*(\d+)\"?", height_str)
        if match:
            feet = int(match.group(1))
            inches = int(match.group(2))
            return round((feet * 30.48) + (inches * 2.54), 2)
        return None
    
    def _convert_weight_to_kg(self, weight_str: str) -> Optional[float]:
        """体重をkgに変換（例: "185 lbs." -> 83.91）"""
        import re
        
        match = re.search(r"(\d+)\s*lbs", weight_str)
        if match:
            lbs = int(match.group(1))
            return round(lbs * 0.453592, 2)
        return None
    
    def _convert_reach_to_cm(self, reach_str: str) -> Optional[float]:
        """リーチをcmに変換（例: "76\"" -> 193.04）"""
        import re
        
        match = re.search(r"(\d+(?:\.\d+)?)\s*\"?", reach_str)
        if match:
            inches = float(match.group(1))
            return round(inches * 2.54, 2)
        return None
    
    def _clean_fighter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """ファイターデータをクリーニング"""
        # 必要に応じてデータ型を変換
        numeric_columns = ['wins', 'losses', 'draws', 'height_cm', 'weight_kg', 'reach_cm']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 勝率を計算
        if all(col in df.columns for col in ['wins', 'losses', 'draws']):
            df['total_fights'] = df['wins'] + df['losses'] + df['draws']
            df['win_rate'] = df['wins'] / df['total_fights']
            df['win_rate'] = df['win_rate'].fillna(0)
        
        return df
    
    def scrape(self) -> pd.DataFrame:
        """スクレイピングを実行"""
        # ファイターURLを取得
        self.fighter_urls = self._get_all_fighter_urls()
        
        # 各ファイターのデータを取得
        def process_fighter(fighter_url):
            try:
                fighter_data = self._parse_fighter_data(fighter_url)
                if fighter_data.get('name'):
                    self.logger.info(f"Scraped: {fighter_data['name']}")
                return fighter_data
            except Exception as e:
                self.logger.error(f"Failed to scrape fighter {fighter_url}: {e}")
                return {}
        
        # プログレスバー付きで処理
        results = self.process_with_progress(
            self.fighter_urls,
            process_fighter,
            desc="Scraping fighters"
        )
        
        # 結果をDataFrameに変換
        if results:
            df = pd.DataFrame([r for r in results if r])
            return self._clean_fighter_data(df)
        
        return pd.DataFrame()