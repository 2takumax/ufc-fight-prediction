#!/usr/bin/env python3
"""静的サイト生成スクリプト"""

import argparse
import logging
import sys
import json
from pathlib import Path
from datetime import datetime
from collections import Counter
import shutil

import pandas as pd
import numpy as np
from jinja2 import Environment, FileSystemLoader

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, str(Path(__file__).parent))

from src.config.settings import Settings


def setup_logging():
    """ロギングの設定"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def load_data(config: Settings):
    """データを読み込む"""
    data = {}
    
    # イベントデータ
    if Path(config.output["event_details_file"]).exists():
        data['event_details'] = pd.read_csv(config.output["event_details_file"])
    
    if Path(config.output["fight_details_file"]).exists():
        data['fight_details'] = pd.read_csv(config.output["fight_details_file"])
    
    if Path(config.output["fight_results_file"]).exists():
        data['fight_results'] = pd.read_csv(config.output["fight_results_file"])
    
    if Path(config.output["fight_stats_file"]).exists():
        data['fight_stats'] = pd.read_csv(config.output["fight_stats_file"])
    
    # オッズデータ
    if Path(config.output["odds_file"]).exists():
        data['odds'] = pd.read_csv(config.output["odds_file"])
    
    # 前処理済みデータがあれば優先
    preprocessed_odds = "./data/odds_preprocessed.csv"
    if Path(preprocessed_odds).exists():
        data['odds_preprocessed'] = pd.read_csv(preprocessed_odds)
    
    return data


def calculate_statistics(data):
    """統計情報を計算"""
    stats = {}
    
    # 基本統計
    if 'event_details' in data:
        stats['total_events'] = len(data['event_details'])
        stats['latest_event'] = data['event_details'].iloc[0]['EVENT'] if len(data['event_details']) > 0 else 'N/A'
    
    if 'fight_details' in data:
        stats['total_fights'] = len(data['fight_details'])
    
    if 'fight_results' in data:
        # 終了方法の分布
        if 'METHOD' in data['fight_results'].columns:
            method_counts = data['fight_results']['METHOD'].value_counts()
            
            # 終了方法をカテゴリ分け
            finish_methods = {
                'Decision': 0,
                'KO/TKO': 0,
                'Submission': 0,
                'Other': 0
            }
            
            for method, count in method_counts.items():
                if pd.isna(method):
                    continue
                method_lower = str(method).lower()
                if 'decision' in method_lower:
                    finish_methods['Decision'] += count
                elif 'ko' in method_lower or 'tko' in method_lower:
                    finish_methods['KO/TKO'] += count
                elif 'submission' in method_lower:
                    finish_methods['Submission'] += count
                else:
                    finish_methods['Other'] += count
            
            stats['finish_method_data'] = {
                'labels': list(finish_methods.keys()),
                'values': list(finish_methods.values())
            }
        
        # 階級別試合数
        if 'WEIGHTCLASS' in data['fight_results'].columns:
            weight_class_counts = data['fight_results']['WEIGHTCLASS'].value_counts().head(10)
            stats['weight_class_data'] = {
                'labels': weight_class_counts.index.tolist(),
                'values': weight_class_counts.values.tolist()
            }
    
    # ファイター数（ユニークなファイター名を数える）
    unique_fighters = set()
    if 'fight_details' in data and 'BOUT' in data['fight_details'].columns:
        for bout in data['fight_details']['BOUT']:
            if pd.notna(bout) and ' vs. ' in bout:
                fighters = bout.split(' vs. ')
                unique_fighters.update([f.strip() for f in fighters])
    
    stats['total_fighters'] = len(unique_fighters)
    
    # オッズ分析
    if 'odds_preprocessed' in data:
        odds_df = data['odds_preprocessed']
        
        # お気に入りの勝率
        if 'fighter1_is_favorite' in odds_df.columns and 'result_encoded' in odds_df.columns:
            favorite_wins = odds_df[
                (odds_df['fighter1_is_favorite'] == 1) & (odds_df['result_encoded'] == 1) |
                (odds_df['fighter1_is_favorite'] == 0) & (odds_df['result_encoded'] == 0)
            ]
            valid_results = odds_df[odds_df['result_encoded'].isin([0, 1])]
            
            if len(valid_results) > 0:
                stats['odds_accuracy'] = round((len(favorite_wins) / len(valid_results)) * 100, 1)
            else:
                stats['odds_accuracy'] = 0
        
        # 平均ブックメーカーマージン
        if 'bookmaker_margin' in odds_df.columns:
            stats['avg_bookmaker_margin'] = round(odds_df['bookmaker_margin'].mean() * 100, 2)
        
        # 最大アップセット（アンダードッグの勝利）
        if 'fighter1_is_favorite' in odds_df.columns and 'result_encoded' in odds_df.columns:
            upsets = odds_df[
                ((odds_df['fighter1_is_favorite'] == 1) & (odds_df['result_encoded'] == 0)) |
                ((odds_df['fighter1_is_favorite'] == 0) & (odds_df['result_encoded'] == 1))
            ]
            
            if len(upsets) > 0:
                # オッズ差が最大のアップセットを見つける
                upsets_with_gap = upsets.copy()
                if 'odds_gap' in upsets_with_gap.columns:
                    biggest_upset_idx = upsets_with_gap['odds_gap'].idxmax()
                    biggest_upset_row = upsets_with_gap.loc[biggest_upset_idx]
                    
                    if biggest_upset_row['result_encoded'] == 0:
                        winner = biggest_upset_row['fighter2']
                        loser = biggest_upset_row['fighter1']
                        odds = biggest_upset_row['fighter2_odds']
                    else:
                        winner = biggest_upset_row['fighter1']
                        loser = biggest_upset_row['fighter2']
                        odds = biggest_upset_row['fighter1_odds']
                    
                    stats['biggest_upset'] = {
                        'winner': winner,
                        'loser': loser,
                        'odds': f"{odds:.2f}"
                    }
    
    # デフォルト値
    stats.setdefault('odds_accuracy', 0)
    stats.setdefault('avg_bookmaker_margin', 0)
    stats.setdefault('biggest_upset', {'winner': 'N/A', 'loser': 'N/A', 'odds': 'N/A'})
    stats.setdefault('most_accurate', {'winner': 'N/A', 'loser': 'N/A', 'odds': 'N/A'})
    
    return stats


def generate_index_page(env, data, stats, output_dir):
    """インデックスページを生成"""
    template = env.get_template('index.html')
    
    # 最近のイベント
    recent_events = []
    if 'event_details' in data:
        events_df = data['event_details'].head(10)
        
        for _, event in events_df.iterrows():
            # そのイベントの試合数を数える
            fight_count = 0
            if 'fight_details' in data:
                fight_count = len(data['fight_details'][data['fight_details']['EVENT'] == event['EVENT']])
            
            recent_events.append({
                'id': events_df.index[_],
                'date': event['DATE'],
                'name': event['EVENT'],
                'location': event['LOCATION'],
                'fight_count': fight_count
            })
    
    # テンプレートに渡すデータ
    context = {
        'update_date': datetime.now().strftime('%Y年%m月%d日'),
        'total_events': stats.get('total_events', 0),
        'total_fights': stats.get('total_fights', 0),
        'total_fighters': stats.get('total_fighters', 0),
        'odds_accuracy': stats.get('odds_accuracy', 0),
        'latest_event': stats.get('latest_event', 'N/A'),
        'recent_events': recent_events,
        'finish_method_data': json.dumps(stats.get('finish_method_data', {'labels': [], 'values': []})),
        'weight_class_data': json.dumps(stats.get('weight_class_data', {'labels': [], 'values': []})),
        'biggest_upset': stats.get('biggest_upset', {}),
        'most_accurate': stats.get('most_accurate', {}),
        'avg_bookmaker_margin': stats.get('avg_bookmaker_margin', 0)
    }
    
    # HTMLを生成
    html = template.render(**context)
    
    # ファイルに保存
    output_path = output_dir / 'index.html'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    logging.info(f"Generated: {output_path}")


def copy_static_files(output_dir):
    """静的ファイルをコピー"""
    static_dir = Path('site/static')
    output_static_dir = output_dir
    
    # CSS
    css_dir = output_static_dir / 'css'
    css_dir.mkdir(exist_ok=True)
    for css_file in (static_dir / 'css').glob('*.css'):
        shutil.copy2(css_file, css_dir / css_file.name)
    
    # JS
    js_dir = output_static_dir / 'js'
    js_dir.mkdir(exist_ok=True)
    for js_file in (static_dir / 'js').glob('*.js'):
        shutil.copy2(js_file, js_dir / js_file.name)
    
    logging.info("Copied static files")


def main():
    """メイン処理"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # 引数解析
    parser = argparse.ArgumentParser(description="UFC静的サイト生成")
    parser.add_argument('--output', type=str, default='site/output', help='出力ディレクトリ')
    parser.add_argument('--config', type=str, help='設定ファイルのパス')
    args = parser.parse_args()
    
    try:
        # 設定読み込み
        config = Settings(args.config)
        
        # データ読み込み
        logger.info("Loading data...")
        data = load_data(config)
        
        # 統計計算
        logger.info("Calculating statistics...")
        stats = calculate_statistics(data)
        
        # 出力ディレクトリ作成
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Jinja2環境設定
        env = Environment(loader=FileSystemLoader('site/templates'))
        
        # ページ生成
        logger.info("Generating pages...")
        generate_index_page(env, data, stats, output_dir)
        
        # 静的ファイルコピー
        copy_static_files(output_dir)
        
        logger.info(f"Site generated successfully in {output_dir}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error generating site: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())