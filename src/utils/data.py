"""データ処理関連のユーティリティ関数"""

from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> tuple[bool, List[str]]:
    """DataFrameの検証
    
    Args:
        df: 検証するDataFrame
        required_columns: 必須カラムのリスト
        
    Returns:
        (検証結果, エラーメッセージのリスト)
    """
    errors = []
    
    if df.empty:
        errors.append("DataFrame is empty")
        return False, errors
    
    # 必須カラムのチェック
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Missing columns: {missing_columns}")
    
    # NULL値のチェック
    null_counts = df[list(set(required_columns) & set(df.columns))].isnull().sum()
    null_columns = null_counts[null_counts > 0]
    if not null_columns.empty:
        errors.append(f"NULL values found in: {null_columns.to_dict()}")
    
    return len(errors) == 0, errors


def clean_numeric_column(series: pd.Series) -> pd.Series:
    """数値カラムのクリーニング
    
    Args:
        series: クリーニングするSeries
        
    Returns:
        クリーニング済みSeries
    """
    # 文字列から数値を抽出
    if series.dtype == 'object':
        series = series.str.extract(r'([-+]?\d*\.?\d+)', expand=False)
    
    # 数値に変換
    return pd.to_numeric(series, errors='coerce')


def parse_fight_record(record: str) -> Dict[str, int]:
    """戦績文字列をパース（例: "20-5-0" -> {"wins": 20, "losses": 5, "draws": 0}）
    
    Args:
        record: 戦績文字列
        
    Returns:
        パース結果の辞書
    """
    parts = record.strip().split('-')
    if len(parts) >= 3:
        return {
            'wins': int(parts[0]) if parts[0].isdigit() else 0,
            'losses': int(parts[1]) if parts[1].isdigit() else 0,
            'draws': int(parts[2]) if parts[2].isdigit() else 0
        }
    return {'wins': 0, 'losses': 0, 'draws': 0}


def calculate_win_rate(wins: int, total_fights: int) -> float:
    """勝率を計算
    
    Args:
        wins: 勝利数
        total_fights: 総試合数
        
    Returns:
        勝率（0-1の範囲）
    """
    if total_fights == 0:
        return 0.0
    return wins / total_fights


def merge_fighter_names(df: pd.DataFrame, 
                       fighter1_col: str = 'fighter1', 
                       fighter2_col: str = 'fighter2') -> pd.DataFrame:
    """ファイター名を統一フォーマットに変換
    
    Args:
        df: 処理するDataFrame
        fighter1_col: ファイター1のカラム名
        fighter2_col: ファイター2のカラム名
        
    Returns:
        処理済みDataFrame
    """
    df = df.copy()
    
    # 名前の正規化（大文字小文字、余分なスペースなど）
    for col in [fighter1_col, fighter2_col]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()
    
    return df


def filter_recent_data(df: pd.DataFrame, 
                      date_col: str, 
                      days: int = 365) -> pd.DataFrame:
    """指定日数以内のデータをフィルタリング
    
    Args:
        df: フィルタリングするDataFrame
        date_col: 日付カラム名
        days: 何日以内のデータを取得するか
        
    Returns:
        フィルタリング済みDataFrame
    """
    df = df.copy()
    
    # 日付カラムをdatetimeに変換
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # 現在から指定日数以内のデータをフィルタ
    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days)
    return df[df[date_col] >= cutoff_date]