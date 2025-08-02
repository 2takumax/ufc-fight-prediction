#!/usr/bin/env python3
"""データ前処理のエントリーポイント"""

import argparse
import logging
import sys
from pathlib import Path

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, str(Path(__file__).parent))

from src.config.settings import Settings
from src.preprocessing.odds import OddsPreprocessor
from src.preprocessing.events import EventsPreprocessor


def setup_logging(config: Settings):
    """ロギングの設定"""
    logging.basicConfig(
        level=config.logging["level"],
        format=config.logging["format"]
    )


def parse_arguments():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(
        description="UFCデータ前処理ツール"
    )
    
    parser.add_argument(
        "--odds",
        action="store_true",
        help="オッズデータを前処理"
    )
    
    parser.add_argument(
        "--events",
        action="store_true",
        help="イベントデータを前処理"
    )
    
    parser.add_argument(
        "--input",
        type=str,
        help="入力ファイルパス（オプション）"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="出力ファイルパス（オプション）"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        help="設定ファイルのパス"
    )
    
    return parser.parse_args()


def main():
    """メイン処理"""
    # 引数解析
    args = parse_arguments()
    
    # 設定読み込み
    config = Settings(args.config)
    
    # ロギング設定
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    # 引数チェック
    if not any([args.odds, args.events]):
        logger.error("少なくとも1つの前処理オプションを指定してください")
        logger.info("使用例: python preprocess.py --odds")
        return 1
    
    try:
        # オッズデータの前処理
        if args.odds:
            logger.info("=" * 50)
            logger.info("オッズデータの前処理を開始")
            logger.info("=" * 50)
            
            preprocessor = OddsPreprocessor(config=config)
            
            # デフォルトのパス
            input_path = args.input or config.output["odds_file"]
            output_path = args.output or "./data/odds_preprocessed.csv"
            
            preprocessor.run(input_path, output_path)
        
        # イベントデータの前処理
        if args.events:
            logger.info("=" * 50)
            logger.info("イベントデータの前処理を開始")
            logger.info("=" * 50)
            
            preprocessor = EventsPreprocessor(config=config)
            
            # 各データファイルを処理
            data_files = {
                'event_details': config.output["event_details_file"],
                'fight_details': config.output["fight_details_file"],
                'fight_results': config.output["fight_results_file"],
                'fight_stats': config.output["fight_stats_file"]
            }
            
            # データを読み込み
            data_dict = {}
            for key, path in data_files.items():
                if Path(path).exists():
                    logger.info(f"Loading {key} from {path}")
                    data_dict[key] = preprocessor.load_data(path)
                else:
                    logger.warning(f"File not found: {path}")
            
            # 前処理実行
            processed_data = preprocessor.preprocess_all(data_dict)
            
            # 保存
            for key, df in processed_data.items():
                output_path = f"./data/{key}_preprocessed.csv"
                preprocessor.save_data(df, output_path)
        
        logger.info("=" * 50)
        logger.info("前処理が完了しました")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())