"""前処理の基底クラス"""

from abc import ABC, abstractmethod
from typing import Optional, Any
import pandas as pd
import logging
from pathlib import Path


class BasePreprocessor(ABC):
    """前処理の基底クラス"""
    
    def __init__(self, config: Optional[Any] = None):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """前処理を実行する抽象メソッド"""
        pass
    
    def save_data(self, df: pd.DataFrame, output_path: str) -> None:
        """データを保存"""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(path, index=False)
        self.logger.info(f"Preprocessed data saved to {path}")
    
    def load_data(self, input_path: str) -> pd.DataFrame:
        """データを読み込む"""
        path = Path(input_path)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        
        return pd.read_csv(path)
    
    def run(self, input_path: str, output_path: str) -> pd.DataFrame:
        """前処理を実行"""
        self.logger.info(f"Loading data from {input_path}")
        df = self.load_data(input_path)
        
        self.logger.info(f"Starting preprocessing on {len(df)} records")
        processed_df = self.preprocess(df)
        
        self.save_data(processed_df, output_path)
        self.logger.info(f"Preprocessing completed. Output shape: {processed_df.shape}")
        
        return processed_df