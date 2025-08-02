"""オッズデータの前処理"""

from typing import Optional
import pandas as pd
import numpy as np

from src.preprocessing.base import BasePreprocessor


class OddsPreprocessor(BasePreprocessor):
    """オッズデータの前処理クラス"""
    
    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """オッズデータの前処理を実行"""
        df = df.copy()
        
        # 基本的なクリーニング
        df = self._clean_basic(df)
        
        # オッズの数値変換（すでにdecimalなので変換は不要）
        df = self._convert_odds_to_numeric(df)
        
        # 勝率の計算
        df = self._calculate_implied_probability(df)
        
        # 結果のエンコーディング
        df = self._encode_results(df)
        
        # 特徴量の作成
        df = self._create_features(df)
        
        return df
    
    def _clean_basic(self, df: pd.DataFrame) -> pd.DataFrame:
        """基本的なクリーニング"""
        # 空白の除去
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].str.strip()
        
        # 重複の削除
        df = df.drop_duplicates()
        
        return df
    
    def _convert_odds_to_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """オッズを数値に変換（すでにdecimalと仮定）"""
        odds_columns = ['fighter1_odds', 'fighter2_odds']
        
        for col in odds_columns:
            if col in df.columns:
                # 数値に変換（エラーはNaNに）
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def _calculate_implied_probability(self, df: pd.DataFrame) -> pd.DataFrame:
        """インプライド確率を計算"""
        if 'fighter1_odds' in df.columns and 'fighter2_odds' in df.columns:
            # Decimal oddsからインプライド確率を計算
            df['fighter1_implied_prob'] = 1 / df['fighter1_odds']
            df['fighter2_implied_prob'] = 1 / df['fighter2_odds']
            
            # マージン（ブックメーカーの利益）を計算
            df['total_implied_prob'] = df['fighter1_implied_prob'] + df['fighter2_implied_prob']
            df['bookmaker_margin'] = df['total_implied_prob'] - 1
            
            # 正規化された確率
            df['fighter1_norm_prob'] = df['fighter1_implied_prob'] / df['total_implied_prob']
            df['fighter2_norm_prob'] = df['fighter2_implied_prob'] / df['total_implied_prob']
        
        return df
    
    def _encode_results(self, df: pd.DataFrame) -> pd.DataFrame:
        """結果をエンコード"""
        if 'result' in df.columns:
            # 勝者をバイナリでエンコード（1: fighter1の勝利, 0: fighter2の勝利, -1: 引き分け/無効）
            df['result_encoded'] = df.apply(lambda row: 
                1 if row['result'] == row['fighter1'] 
                else (0 if row['result'] == row['fighter2'] 
                else -1), axis=1
            )
            
            # 結果のカテゴリ
            df['has_result'] = (df['result'] != '-').astype(int)
        
        return df
    
    def _create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """追加の特徴量を作成"""
        if 'fighter1_odds' in df.columns and 'fighter2_odds' in df.columns:
            # オッズの差
            df['odds_diff'] = df['fighter1_odds'] - df['fighter2_odds']
            
            # オッズの比率
            df['odds_ratio'] = df['fighter1_odds'] / df['fighter2_odds']
            
            # お気に入り（favorite）フラグ
            df['fighter1_is_favorite'] = (df['fighter1_odds'] < df['fighter2_odds']).astype(int)
            
            # アンダードッグのオッズ
            df['underdog_odds'] = df[['fighter1_odds', 'fighter2_odds']].max(axis=1)
            df['favorite_odds'] = df[['fighter1_odds', 'fighter2_odds']].min(axis=1)
            
            # オッズの差（絶対値）
            df['odds_gap'] = abs(df['odds_diff'])
        
        # 日付の処理
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day_of_week'] = df['date'].dt.dayofweek
        
        return df