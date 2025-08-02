"""Web関連のユーティリティ関数"""

import time
from typing import Optional, Dict, Any
import numpy as np
import requests
from bs4 import BeautifulSoup


def get_soup(url: str, headers: Optional[Dict[str, str]] = None) -> BeautifulSoup:
    """URLからBeautifulSoupオブジェクトを取得
    
    Args:
        url: 取得するURL
        headers: HTTPヘッダー
        
    Returns:
        BeautifulSoupオブジェクト
    """
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return BeautifulSoup(response.content, 'html.parser')


def sleep_randomly(min_seconds: float = 2, max_seconds: float = 4) -> None:
    """ランダムな時間スリープ
    
    Args:
        min_seconds: 最小スリープ時間（秒）
        max_seconds: 最大スリープ時間（秒）
    """
    sleep_time = np.random.uniform(min_seconds, max_seconds)
    time.sleep(sleep_time)


def clean_text(text: str) -> str:
    """テキストをクリーニング
    
    Args:
        text: クリーニングするテキスト
        
    Returns:
        クリーニング済みテキスト
    """
    return text.strip().replace('\n', ' ').replace('\t', ' ').strip()


def safe_find_text(soup: BeautifulSoup, *args, **kwargs) -> str:
    """安全にテキストを取得
    
    Args:
        soup: BeautifulSoupオブジェクト
        *args, **kwargs: find/find_allメソッドの引数
        
    Returns:
        見つかったテキスト、見つからない場合は空文字列
    """
    element = soup.find(*args, **kwargs)
    if element:
        return clean_text(element.get_text())
    return ""


def extract_number(text: str) -> Optional[float]:
    """テキストから数値を抽出
    
    Args:
        text: 数値を含むテキスト
        
    Returns:
        抽出された数値、抽出できない場合はNone
    """
    import re
    
    # 数値パターンを検索
    match = re.search(r'[-+]?\d*\.?\d+', text)
    if match:
        try:
            return float(match.group())
        except ValueError:
            return None
    return None