"""UFCイベント、試合詳細、結果、統計スクレイパー"""

from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import re
import numpy as np
from bs4 import BeautifulSoup

from src.scraper.base import BaseScraper
from src.utils.web import clean_text


class EventsScraper(BaseScraper):
    """UFCイベント情報をスクレイピングするクラス"""
    
    def __init__(self, test_mode: bool = False, update_mode: bool = False, config: Optional[Any] = None):
        super().__init__(test_mode, update_mode, config)
        self.completed_events_url = self.config.ufc_stats["completed_events_url"]
        self.existing_events = set()
        self.existing_fight_urls = set()
    
    def get_output_path(self) -> str:
        """出力ファイルパスを取得"""
        # デフォルトはイベント詳細ファイル
        return self.config.output["event_details_file"]
    
    def _load_existing_data(self) -> None:
        """既存データを読み込んで処理済みイベントを記録"""
        if self.update_mode:
            # イベント詳細の既存データを確認
            event_details = self.load_existing_data(self.config.output["event_details_file"])
            if event_details is not None and 'EVENT' in event_details.columns:
                self.existing_events = set(event_details['EVENT'].unique())
                self.logger.info(f"Found {len(self.existing_events)} existing events")
            
            # 試合詳細の既存URLを確認
            fight_details = self.load_existing_data(self.config.output["fight_details_file"])
            if fight_details is not None and 'URL' in fight_details.columns:
                self.existing_fight_urls = set(fight_details['URL'].unique())
                self.logger.info(f"Found {len(self.existing_fight_urls)} existing fight URLs")
    
    def parse_event_details(self, soup: BeautifulSoup) -> pd.DataFrame:
        """イベント詳細をパース"""
        # イベント名とURL
        event_names = []
        event_urls = []
        for tag in soup.find_all('a', class_='b-link b-link_style_black'):
            event_names.append(tag.text.strip())
            event_urls.append(tag['href'])
        
        # イベント日付
        event_dates = []
        for tag in soup.find_all('span', class_='b-statistics__date'):
            event_dates.append(tag.text.strip())
        
        # イベント場所
        event_locations = []
        for tag in soup.find_all('td', class_='b-statistics__table-col b-statistics__table-col_style_big-top-padding'):
            event_locations.append(tag.text.strip())
        
        # 最初の要素を除外（予定されているイベント）
        event_dates = event_dates[1:]
        event_locations = event_locations[1:]
        
        # データフレーム作成
        event_details_df = pd.DataFrame({
            'EVENT': event_names,
            'URL': event_urls,
            'DATE': event_dates,
            'LOCATION': event_locations
        })
        
        return event_details_df
    
    def parse_fight_details(self, soup: BeautifulSoup) -> pd.DataFrame:
        """試合詳細をパース"""
        # 試合URL
        fight_urls = []
        for tag in soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click'):
            fight_urls.append(tag['data-link'])
        
        # ファイター名
        fighters_in_event = []
        for tag in soup.find_all('a', class_='b-link b-link_style_black'):
            fighters_in_event.append(tag.text.strip())
        
        # ファイターをペアにして試合を作成
        fights_in_event = [f"{fighter_a} vs. {fighter_b}" for fighter_a, fighter_b in zip(fighters_in_event[::2], fighters_in_event[1::2])]
        
        # データフレーム作成
        fight_details_df = pd.DataFrame({'BOUT': fights_in_event, 'URL': fight_urls})
        
        # イベント名を追加
        event_name = soup.find('h2', class_='b-content__title')
        if event_name:
            fight_details_df['EVENT'] = event_name.text.strip()
            # カラムの順序を調整
            fight_details_df = fight_details_df[['EVENT', 'BOUT', 'URL']]
        
        return fight_details_df
    
    def parse_fight_results(self, soup: BeautifulSoup) -> List[str]:
        """試合結果をパース"""
        fight_results = []
        
        # イベント名
        event_elem = soup.find('h2', class_='b-content__title')
        if event_elem:
            fight_results.append(event_elem.text)
        
        # ファイター名
        for tag in soup.find_all('a', class_='b-link b-fight-details__person-link'):
            fight_results.append(tag.text)
        
        # 勝敗結果（W/L）
        for tag in soup.find_all('div', class_='b-fight-details__person'):
            for i_text in tag.find_all('i'):
                fight_results.append(i_text.text)
        
        # 階級
        weightclass_elem = soup.find('div', class_='b-fight-details__fight-head')
        if weightclass_elem:
            fight_results.append(weightclass_elem.text)
        
        # 勝利方法
        method_elem = soup.find('i', class_='b-fight-details__text-item_first')
        if method_elem:
            fight_results.append(method_elem.text)
        
        # その他の結果（ラウンド、時間、時間形式、レフェリー）
        remaining_results = soup.find_all('p', class_='b-fight-details__text')
        
        if len(remaining_results) > 0:
            # ラウンド、時間、時間形式、レフェリー
            for tag in remaining_results[0].find_all('i', class_='b-fight-details__text-item'):
                fight_results.append(tag.text.strip())
        
        # 詳細
        if len(remaining_results) > 1:
            fight_results.append(remaining_results[1].get_text())
        
        # テキストのクリーニング
        fight_results = [text.replace('\n', '').replace('  ', '') for text in fight_results]
        
        return fight_results
    
    def organise_fight_results(self, results_from_soup: List[str], fight_results_column_names: List[str]) -> pd.DataFrame:
        """試合結果を整理"""
        fight_results_clean = []
        
        # イベント名
        fight_results_clean.append(results_from_soup[0] if len(results_from_soup) > 0 else '')
        
        # ファイター名を結合
        if len(results_from_soup) >= 3:
            fight_results_clean.append(' vs. '.join(results_from_soup[1:3]))
        else:
            fight_results_clean.append('')
        
        # 勝敗結果を結合
        if len(results_from_soup) >= 5:
            fight_results_clean.append('/'.join(results_from_soup[3:5]))
        else:
            fight_results_clean.append('')
        
        # 残りの結果（ラベルを削除）
        if len(results_from_soup) > 5:
            fight_results_clean.extend([re.sub('^(.+?): ?', '', text) for text in results_from_soup[5:]])
        
        # データフレーム作成
        fight_result_df = pd.DataFrame(columns=fight_results_column_names)
        
        # 不足する列を埋める
        while len(fight_results_clean) < len(fight_results_column_names):
            fight_results_clean.append('')
        
        fight_result_df.loc[0] = fight_results_clean[:len(fight_results_column_names)]
        
        return fight_result_df
    
    def parse_fight_stats(self, soup: BeautifulSoup) -> Tuple[List[str], List[str]]:
        """試合統計をパース"""
        fighter_a_stats = []
        fighter_b_stats = []
        
        # すべての統計テーブルの列を取得
        for tag in soup.find_all('td', class_='b-fight-details__table-col'):
            # 各列内のp要素を取得
            for index, p_text in enumerate(tag.find_all('p')):
                # 偶数インデックスは最初のファイター、奇数は2番目のファイター
                if index % 2 == 0:
                    fighter_a_stats.append(p_text.text.strip())
                else:
                    fighter_b_stats.append(p_text.text.strip())
        
        return fighter_a_stats, fighter_b_stats
    
    def organise_fight_stats(self, stats_from_soup: List[str]) -> List[List[str]]:
        """統計を整理"""
        fighter_stats_clean = []
        
        if not stats_from_soup:
            return fighter_stats_clean
        
        # ファイター名で統計をグループ化
        fighter_name = stats_from_soup[0] if stats_from_soup else ''
        current_group = []
        
        for stat in stats_from_soup:
            if stat == fighter_name and current_group:
                fighter_stats_clean.append(current_group)
                current_group = [stat]
            else:
                current_group.append(stat)
        
        if current_group:
            fighter_stats_clean.append(current_group)
        
        return fighter_stats_clean
    
    def convert_fight_stats_to_df(self, clean_fighter_stats: List[List[str]], 
                                 totals_column_names: List[str], 
                                 significant_strikes_column_names: List[str]) -> pd.DataFrame:
        """統計をデータフレームに変換"""
        totals_df = pd.DataFrame(columns=totals_column_names)
        significant_strikes_df = pd.DataFrame(columns=significant_strikes_column_names)
        
        # 統計がない場合
        if len(clean_fighter_stats) == 0:
            totals_df.loc[0] = [np.nan] * len(totals_column_names)
            significant_strikes_df.loc[0] = [np.nan] * len(significant_strikes_column_names)
        else:
            # ラウンド数を計算
            number_of_rounds = int((len(clean_fighter_stats) - 2) / 2) if len(clean_fighter_stats) >= 2 else 0
            
            # 各ラウンドの統計を処理
            for round_num in range(number_of_rounds):
                try:
                    # トータル統計
                    if round_num + 1 < len(clean_fighter_stats):
                        totals_row = ['Round ' + str(round_num + 1)] + clean_fighter_stats[round_num + 1]
                        # 列数を調整
                        while len(totals_row) < len(totals_column_names):
                            totals_row.append('')
                        totals_df.loc[len(totals_df)] = totals_row[:len(totals_column_names)]
                    
                    # 有効打撃統計
                    sig_idx = round_num + 1 + int(len(clean_fighter_stats) / 2)
                    if sig_idx < len(clean_fighter_stats):
                        sig_row = ['Round ' + str(round_num + 1)] + clean_fighter_stats[sig_idx]
                        # 列数を調整
                        while len(sig_row) < len(significant_strikes_column_names):
                            sig_row.append('')
                        significant_strikes_df.loc[len(significant_strikes_df)] = sig_row[:len(significant_strikes_column_names)]
                except Exception as e:
                    self.logger.warning(f"Error processing round {round_num + 1} stats: {e}")
        
        # データフレームを結合
        if not totals_df.empty and not significant_strikes_df.empty:
            fighter_stats_df = totals_df.merge(significant_strikes_df, how='inner', on='ROUND')
        else:
            # 空のデータフレームを返す
            all_columns = self.config.column_names["fight_stats"]
            fighter_stats_df = pd.DataFrame(columns=all_columns)
        
        return fighter_stats_df
    
    def combine_fighter_stats_dfs(self, fighter_a_stats_df: pd.DataFrame, 
                                fighter_b_stats_df: pd.DataFrame, 
                                soup: BeautifulSoup) -> pd.DataFrame:
        """両ファイターの統計を結合"""
        # データフレームを連結
        fight_stats = pd.concat([fighter_a_stats_df, fighter_b_stats_df], ignore_index=True)
        
        # イベント名を追加
        event_elem = soup.find('h2', class_='b-content__title')
        if event_elem:
            fight_stats['EVENT'] = event_elem.text.strip()
        
        # ファイター名を取得
        fighters_names = []
        for tag in soup.find_all('a', class_='b-link b-fight-details__person-link'):
            fighters_names.append(tag.text.strip())
        
        # 試合名を追加
        if len(fighters_names) >= 2:
            fight_stats['BOUT'] = ' vs. '.join(fighters_names[:2])
        
        # カラムの順序を調整
        expected_columns = self.config.column_names["fight_stats"]
        existing_columns = [col for col in expected_columns if col in fight_stats.columns]
        missing_columns = [col for col in expected_columns if col not in fight_stats.columns]
        
        # 不足している列を追加
        for col in missing_columns:
            fight_stats[col] = ''
        
        # 期待される順序で返す
        return fight_stats[expected_columns]
    
    def parse_organise_fight_results_and_stats(self, soup: BeautifulSoup, url: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """試合結果と統計をパースして整理"""
        # 試合結果をパース
        fight_results = self.parse_fight_results(soup)
        fight_results.append('URL:' + url)
        fight_results_df = self.organise_fight_results(
            fight_results, 
            self.config.column_names["fight_results"]
        )
        
        # URLを正しく設定
        if 'URL' in fight_results_df.columns:
            fight_results_df['URL'] = url
        
        # 試合統計をパース
        fighter_a_stats, fighter_b_stats = self.parse_fight_stats(soup)
        fighter_a_stats_clean = self.organise_fight_stats(fighter_a_stats)
        fighter_b_stats_clean = self.organise_fight_stats(fighter_b_stats)
        
        # 統計をデータフレームに変換
        fighter_a_stats_df = self.convert_fight_stats_to_df(
            fighter_a_stats_clean,
            self.config.column_names["totals"],
            self.config.column_names["significant_strikes"]
        )
        fighter_b_stats_df = self.convert_fight_stats_to_df(
            fighter_b_stats_clean,
            self.config.column_names["totals"],
            self.config.column_names["significant_strikes"]
        )
        
        # 統計を結合
        fight_stats_df = self.combine_fighter_stats_dfs(
            fighter_a_stats_df, 
            fighter_b_stats_df, 
            soup
        )
        
        return fight_results_df, fight_stats_df
    
    def scrape(self) -> Dict[str, pd.DataFrame]:
        """スクレイピングを実行"""
        # 既存データを読み込む
        self._load_existing_data()
        
        # イベント一覧を取得
        self.logger.info("Fetching event list from UFC Stats")
        soup = self.get_soup(self.completed_events_url)
        
        # イベント詳細をパース
        event_details_df = self.parse_event_details(soup)
        
        # 差分更新モードの場合、新規イベントのみをフィルタ
        if self.update_mode:
            new_events = event_details_df[~event_details_df['EVENT'].isin(self.existing_events)]
            self.logger.info(f"Found {len(new_events)} new events out of {len(event_details_df)} total events")
            
            if new_events.empty:
                self.logger.info("No new events to scrape")
                return {
                    'event_details': pd.DataFrame(),
                    'fight_details': pd.DataFrame(),
                    'fight_results': pd.DataFrame(),
                    'fight_stats': pd.DataFrame()
                }
            
            events_to_process = new_events
        else:
            events_to_process = event_details_df
        
        # テストモードの場合は最初の3イベントのみ
        if self.test_mode:
            events_to_process = events_to_process.head(3)
        
        # 結果を格納するデータフレーム
        all_fight_details = []
        all_fight_results = []
        all_fight_stats = []
        
        # 各イベントを処理
        def process_event(row):
            try:
                event_url = row['URL']
                event_name = row['EVENT']
                
                self.logger.info(f"Processing event: {event_name}")
                soup = self.get_soup(event_url)
                
                # 試合詳細をパース
                fight_details_df = self.parse_fight_details(soup)
                
                # 各試合の詳細を処理
                fight_results_list = []
                fight_stats_list = []
                
                for _, fight_row in fight_details_df.iterrows():
                    fight_url = fight_row['URL']
                    
                    # 差分更新モードで既存の試合URLはスキップ
                    if self.update_mode and fight_url in self.existing_fight_urls:
                        continue
                    
                    try:
                        fight_soup = self.get_soup(fight_url)
                        fight_results_df, fight_stats_df = self.parse_organise_fight_results_and_stats(
                            fight_soup, fight_url
                        )
                        fight_results_list.append(fight_results_df)
                        fight_stats_list.append(fight_stats_df)
                    except Exception as e:
                        self.logger.error(f"Failed to parse fight {fight_url}: {e}")
                
                return {
                    'fight_details': fight_details_df,
                    'fight_results': fight_results_list,
                    'fight_stats': fight_stats_list
                }
            except Exception as e:
                self.logger.error(f"Failed to process event {row['EVENT']}: {e}")
                return None
        
        # プログレスバー付きで処理
        results = self.process_with_progress(
            [row for _, row in events_to_process.iterrows()],
            process_event,
            desc="Processing events"
        )
        
        # 結果を集約
        for result in results:
            if result:
                all_fight_details.append(result['fight_details'])
                all_fight_results.extend(result['fight_results'])
                all_fight_stats.extend(result['fight_stats'])
        
        # データフレームを結合
        fight_details_df = pd.concat(all_fight_details, ignore_index=True) if all_fight_details else pd.DataFrame()
        fight_results_df = pd.concat(all_fight_results, ignore_index=True) if all_fight_results else pd.DataFrame()
        fight_stats_df = pd.concat(all_fight_stats, ignore_index=True) if all_fight_stats else pd.DataFrame()
        
        # 結果を返す
        return {
            'event_details': events_to_process,
            'fight_details': fight_details_df,
            'fight_results': fight_results_df,
            'fight_stats': fight_stats_df
        }