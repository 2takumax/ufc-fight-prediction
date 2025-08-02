"""設定管理モジュール"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
import yaml


class Settings:
    """設定管理クラス"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Args:
            config_path: 設定ファイルのパス。Noneの場合はデフォルトパスを使用
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config.yaml"
        
        self.config_path = Path(config_path)
        self._config = self._load_config()
        self._apply_env_overrides()
    
    def _load_config(self) -> Dict[str, Any]:
        """設定ファイルを読み込む"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _apply_env_overrides(self) -> None:
        """環境変数による設定の上書き"""
        # S3バケット名の上書き
        if bucket := os.getenv("UFC_S3_BUCKET"):
            self._config.setdefault("aws", {})["s3_bucket"] = bucket
        
        # ログレベルの上書き
        if log_level := os.getenv("UFC_LOG_LEVEL"):
            self._config.setdefault("logging", {})["level"] = log_level
    
    @property
    def ufc_stats(self) -> Dict[str, str]:
        """UFC Stats関連の設定"""
        return self._config.get("ufc_stats", {})
    
    @property
    def betmma(self) -> Dict[str, str]:
        """BetMMA関連の設定"""
        return self._config.get("betmma", {})
    
    @property
    def output(self) -> Dict[str, str]:
        """出力ファイル関連の設定"""
        return self._config.get("output", {})
    
    @property
    def scraping(self) -> Dict[str, Any]:
        """スクレイピング関連の設定"""
        return self._config.get("scraping", {})
    
    @property
    def aws(self) -> Dict[str, str]:
        """AWS関連の設定"""
        return self._config.get("aws", {})
    
    @property
    def logging(self) -> Dict[str, Any]:
        """ログ関連の設定"""
        default = {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
        return self._config.get("logging", default)
    
    @property
    def column_names(self) -> Dict[str, Any]:
        """カラム名の設定"""
        return self._config.get("column_names", {})
    
    def get(self, key: str, default: Any = None) -> Any:
        """任意のキーで設定値を取得"""
        return self._config.get(key, default)