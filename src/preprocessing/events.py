"""イベントデータの前処理"""

from typing import Dict, Optional
import pandas as pd
import numpy as np
import re

from src.preprocessing.base import BasePreprocessor


class EventsPreprocessor(BasePreprocessor):
    """イベントデータの前処理クラス"""
    
    def preprocess_all(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """すべてのイベント関連データを前処理"""
        processed = {}
        
        # 各データフレームを個別に処理
        if 'event_details' in data_dict:
            processed['event_details'] = self._preprocess_event_details(data_dict['event_details'])
        
        if 'fight_details' in data_dict:
            processed['fight_details'] = self._preprocess_fight_details(data_dict['fight_details'])
        
        if 'fight_results' in data_dict:
            processed['fight_results'] = self._preprocess_fight_results(data_dict['fight_results'])
        
        if 'fight_stats' in data_dict:
            processed['fight_stats'] = self._preprocess_fight_stats(data_dict['fight_stats'])
        
        return processed
    
    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """汎用の前処理（単一データフレーム用）"""
        return self._clean_basic(df)
    
    def _clean_basic(self, df: pd.DataFrame) -> pd.DataFrame:
        """基本的なクリーニング"""
        df = df.copy()
        
        # 空白の除去
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].str.strip()
        
        # 重複の削除
        df = df.drop_duplicates()
        
        return df
    
    def _preprocess_event_details(self, df: pd.DataFrame) -> pd.DataFrame:
        """イベント詳細の前処理"""
        df = self._clean_basic(df)
        
        # 日付の処理
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            df['year'] = df['DATE'].dt.year
            df['month'] = df['DATE'].dt.month
            df['day'] = df['DATE'].dt.day
            df['day_of_week'] = df['DATE'].dt.dayofweek
        
        # 場所の分割
        if 'LOCATION' in df.columns:
            location_parts = df['LOCATION'].str.split(', ', expand=True)
            df['city'] = location_parts[0] if 0 in location_parts.columns else None
            df['state'] = location_parts[1] if 1 in location_parts.columns else None
            df['country'] = location_parts[2] if 2 in location_parts.columns else None
        
        # イベントタイプの抽出
        if 'EVENT' in df.columns:
            df['is_ppv'] = df['EVENT'].str.contains(r'UFC \d+', regex=True).fillna(False)
            df['is_fight_night'] = df['EVENT'].str.contains('Fight Night', case=False).fillna(False)
        
        return df
    
    def _preprocess_fight_details(self, df: pd.DataFrame) -> pd.DataFrame:
        """試合詳細の前処理"""
        df = self._clean_basic(df)
        
        # ファイター名の抽出
        if 'BOUT' in df.columns:
            # "Fighter A vs. Fighter B" の形式から抽出
            bout_parts = df['BOUT'].str.split(' vs. ', expand=True)
            df['fighter1'] = bout_parts[0] if 0 in bout_parts.columns else None
            df['fighter2'] = bout_parts[1] if 1 in bout_parts.columns else None
        
        return df
    
    def _preprocess_fight_results(self, df: pd.DataFrame) -> pd.DataFrame:
        """試合結果の前処理"""
        df = self._clean_basic(df)
        
        # 結果のエンコーディング
        if 'OUTCOME' in df.columns:
            # W/L, L/W, D/D などをパース
            outcome_parts = df['OUTCOME'].str.split('/', expand=True)
            df['fighter1_result'] = outcome_parts[0] if 0 in outcome_parts.columns else None
            df['fighter2_result'] = outcome_parts[1] if 1 in outcome_parts.columns else None
            
            # 勝者の判定
            df['winner'] = df.apply(lambda row: 
                'fighter1' if row.get('fighter1_result') == 'W' 
                else ('fighter2' if row.get('fighter2_result') == 'W' 
                else 'draw'), axis=1
            )
        
        # 階級のクリーニング
        if 'WEIGHTCLASS' in df.columns:
            df['WEIGHTCLASS'] = df['WEIGHTCLASS'].str.replace(' Bout', '')
            df['WEIGHTCLASS'] = df['WEIGHTCLASS'].str.strip()
        
        # 終了方法の分類
        if 'METHOD' in df.columns:
            df['finish_type'] = df['METHOD'].apply(self._classify_finish_method)
        
        # ラウンドと時間の数値化
        if 'ROUND' in df.columns:
            df['ROUND'] = pd.to_numeric(df['ROUND'], errors='coerce')
        
        if 'TIME' in df.columns:
            df['time_seconds'] = df['TIME'].apply(self._time_to_seconds)
        
        return df
    
    def _preprocess_fight_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """試合統計の前処理"""
        df = self._clean_basic(df)
        
        # 統計値の数値化
        stat_columns = ['KD', 'SUB.ATT', 'REV.', 'HEAD', 'BODY', 'LEG', 'DISTANCE', 'CLINCH', 'GROUND']
        
        for col in stat_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 打撃統計のパース（例: "10 of 20"）
        strike_columns = ['SIG.STR.', 'TOTAL STR.']
        for col in strike_columns:
            if col in df.columns:
                self._parse_strike_stats(df, col)
        
        # テイクダウン統計のパース
        if 'TD' in df.columns:
            self._parse_takedown_stats(df)
        
        # パーセンテージの数値化
        pct_columns = ['SIG.STR. %', 'TD %']
        for col in pct_columns:
            if col in df.columns:
                df[col] = df[col].str.rstrip('%')
                df[col] = pd.to_numeric(df[col], errors='coerce') / 100
        
        # コントロール時間のパース
        if 'CTRL' in df.columns:
            df['control_seconds'] = df['CTRL'].apply(self._time_to_seconds)
        
        # ラウンド番号の抽出
        if 'ROUND' in df.columns:
            df['round_num'] = df['ROUND'].str.extract(r'Round (\d+)')[0]
            df['round_num'] = pd.to_numeric(df['round_num'], errors='coerce')
        
        return df
    
    def _classify_finish_method(self, method: str) -> str:
        """終了方法を分類"""
        if pd.isna(method):
            return 'unknown'
        
        method_lower = method.lower()
        
        if 'ko' in method_lower or 'tko' in method_lower:
            return 'knockout'
        elif 'submission' in method_lower:
            return 'submission'
        elif 'decision' in method_lower:
            return 'decision'
        else:
            return 'other'
    
    def _time_to_seconds(self, time_str: str) -> Optional[float]:
        """時間文字列を秒に変換"""
        if pd.isna(time_str):
            return None
        
        try:
            parts = str(time_str).split(':')
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = int(parts[1])
                return minutes * 60 + seconds
            else:
                return None
        except:
            return None
    
    def _parse_strike_stats(self, df: pd.DataFrame, col: str) -> None:
        """打撃統計をパース（"10 of 20" -> landed: 10, attempted: 20）"""
        if col in df.columns:
            strike_parts = df[col].str.extract(r'(\d+) of (\d+)')
            df[f'{col}_landed'] = pd.to_numeric(strike_parts[0], errors='coerce')
            df[f'{col}_attempted'] = pd.to_numeric(strike_parts[1], errors='coerce')
            df[f'{col}_accuracy'] = df[f'{col}_landed'] / df[f'{col}_attempted']
            df[f'{col}_accuracy'] = df[f'{col}_accuracy'].fillna(0)
    
    def _parse_takedown_stats(self, df: pd.DataFrame) -> None:
        """テイクダウン統計をパース"""
        if 'TD' in df.columns:
            td_parts = df['TD'].str.extract(r'(\d+) of (\d+)')
            df['TD_landed'] = pd.to_numeric(td_parts[0], errors='coerce')
            df['TD_attempted'] = pd.to_numeric(td_parts[1], errors='coerce')
            df['TD_success_rate'] = df['TD_landed'] / df['TD_attempted']
            df['TD_success_rate'] = df['TD_success_rate'].fillna(0)