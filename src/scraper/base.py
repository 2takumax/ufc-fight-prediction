"""ベーススクレイパークラス"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

from src.config.settings import Settings


class BaseScraper(ABC):
    """全てのスクレイパーの基底クラス"""
    
    def __init__(self, test_mode: bool = False, update_mode: bool = False, config: Optional[Settings] = None):
        """
        Args:
            test_mode: テストモード（True の場合、データ取得を制限）
            update_mode: 差分更新モード（True の場合、新規データのみ取得）
            config: 設定オブジェクト
        """
        self.test_mode = test_mode
        self.update_mode = update_mode
        self.config = config or Settings()
        self.logger = self._setup_logger()
        self.session = self._setup_session()
        self.timestamp = datetime.now()
        
    def _setup_logger(self) -> logging.Logger:
        """ロガーの設定"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(self.config.logging["level"])
        
        # コンソールハンドラーの設定
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(self.config.logging["format"])
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _setup_session(self) -> requests.Session:
        """リトライ機能付きのセッションを設定"""
        session = requests.Session()
        
        # リトライ戦略の設定
        retry_strategy = Retry(
            total=3,  # 最大リトライ回数
            backoff_factor=1,  # リトライ間隔の増加率
            status_forcelist=[429, 500, 502, 503, 504],  # リトライ対象のステータスコード
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # デフォルトヘッダーの設定
        session.headers.update(self.config.scraping.get("headers", {}))
        
        return session
    
    def get_soup(self, url: str, **kwargs) -> BeautifulSoup:
        """URLからBeautifulSoupオブジェクトを取得"""
        try:
            self.logger.debug(f"Fetching URL: {url}")
            response = self.session.get(url, timeout=30, **kwargs)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error while fetching {url}: {e}")
            raise
    
    def sleep_randomly(self) -> None:
        """ランダムな時間スリープ（スクレイピング間隔）"""
        min_sleep = self.config.scraping.get("sleep_min", 2)
        max_sleep = self.config.scraping.get("sleep_max", 4)
        sleep_time = np.random.uniform(min_sleep, max_sleep)
        
        self.logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
        time.sleep(sleep_time)
    
    def save_data(self, data: pd.DataFrame, output_path: str, append: bool = False) -> None:
        """データをCSVファイルに保存
        
        Args:
            data: 保存するデータ
            output_path: 出力ファイルパス
            append: 既存ファイルに追記するか
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if append and output_path.exists():
                # 既存データを読み込んで結合
                existing_data = pd.read_csv(output_path)
                data = pd.concat([existing_data, data], ignore_index=True)
                self.logger.info(f"Appending {len(data) - len(existing_data)} new records")
            
            data.to_csv(output_path, index=False)
            self.logger.info(f"Data saved to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save data to {output_path}: {e}")
            raise
    
    def validate_data(self, data: pd.DataFrame, required_columns: List[str]) -> bool:
        """データの基本的な検証"""
        if data.empty:
            self.logger.warning("Data is empty")
            return False
        
        missing_columns = set(required_columns) - set(data.columns)
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # NULL値のチェック
        null_counts = data[required_columns].isnull().sum()
        if null_counts.any():
            self.logger.warning(f"NULL values found: {null_counts[null_counts > 0].to_dict()}")
        
        return True
    
    def process_with_progress(self, items: List[Any], process_func, desc: str = "Processing") -> List[Any]:
        """プログレスバー付きでアイテムを処理"""
        results = []
        
        # テストモードの場合、処理数を制限
        if self.test_mode:
            items = items[:10]
            self.logger.info(f"Test mode: Processing only first 10 items")
        
        for item in tqdm(items, desc=desc):
            try:
                result = process_func(item)
                results.append(result)
                self.sleep_randomly()
            except Exception as e:
                self.logger.error(f"Error processing item {item}: {e}")
                if not self.config.scraping.get("continue_on_error", True):
                    raise
                continue
        
        return results
    
    @abstractmethod
    def scrape(self) -> pd.DataFrame:
        """スクレイピングを実行する抽象メソッド"""
        pass
    
    @abstractmethod
    def get_output_path(self) -> str:
        """出力ファイルパスを取得する抽象メソッド"""
        pass
    
    def load_existing_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """既存データを読み込む"""
        try:
            path = Path(file_path)
            if path.exists():
                data = pd.read_csv(path)
                self.logger.info(f"Loaded {len(data)} existing records from {file_path}")
                return data
            else:
                self.logger.info(f"No existing data found at {file_path}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to load existing data: {e}")
            return None
    
    def run(self) -> None:
        """スクレイピングの実行"""
        try:
            self.logger.info(f"Starting {self.__class__.__name__}")
            if self.update_mode:
                self.logger.info("Running in update mode (incremental)")
            
            # スクレイピング実行
            result = self.scrape()
            
            # 結果が辞書の場合（複数のデータフレーム）
            if isinstance(result, dict):
                for key, data in result.items():
                    if not data.empty:
                        # タイムスタンプを追加
                        data['timestamp'] = self.timestamp
                        
                        # データ保存
                        if key == 'event_details':
                            output_path = self.config.output["event_details_file"]
                        elif key == 'fight_details':
                            output_path = self.config.output["fight_details_file"]
                        elif key == 'fight_results':
                            output_path = self.config.output["fight_results_file"]
                        elif key == 'fight_stats':
                            output_path = self.config.output["fight_stats_file"]
                        else:
                            continue
                        
                        self.save_data(data, output_path, append=self.update_mode)
                        self.logger.info(f"Saved {key}: {len(data)} records to {output_path}")
            else:
                # 結果が単一のデータフレーム
                if not result.empty:
                    # タイムスタンプを追加
                    result['timestamp'] = self.timestamp
                    
                    # データ保存
                    output_path = self.get_output_path()
                    self.save_data(result, output_path, append=self.update_mode)
            
            self.logger.info(f"Completed {self.__class__.__name__} successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to run {self.__class__.__name__}: {e}")
            raise