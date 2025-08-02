#!/usr/bin/env python3
"""UFC Fight Prediction - メインエントリーポイント"""

import argparse
import logging
import sys
from pathlib import Path

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, str(Path(__file__).parent))

from src.config.settings import Settings
from src.scraper.events import EventsScraper
from src.scraper.fighters import FighterScraper
from src.scraper.odds import OddsScraper


def setup_logging(config: Settings):
    """ロギングの設定"""
    logging.basicConfig(
        level=config.logging["level"],
        format=config.logging["format"]
    )


def parse_arguments():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(
        description="UFC試合データスクレイピングツール"
    )
    
    parser.add_argument(
        "--events",
        action="store_true",
        help="イベント結果をスクレイピング"
    )
    
    parser.add_argument(
        "--fighters", 
        action="store_true",
        help="ファイター情報をスクレイピング"
    )
    
    parser.add_argument(
        "--odds",
        action="store_true",
        help="オッズ情報をスクレイピング"
    )
    
    parser.add_argument(
        "--test",
        action="store_true",
        help="テストモード（少量のデータのみ処理）"
    )
    
    parser.add_argument(
        "--update",
        action="store_true",
        help="差分更新モード（新規データのみ取得）"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        help="設定ファイルのパス"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="ログレベル"
    )
    
    return parser.parse_args()


def main():
    """メイン処理"""
    # 引数解析
    args = parse_arguments()
    
    # 設定読み込み
    config = Settings(args.config)
    
    # ログレベルの上書き
    if args.log_level:
        config._config["logging"]["level"] = args.log_level
    
    # ロギング設定
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    # 引数チェック
    if not any([args.events, args.fighters, args.odds]):
        logger.error("少なくとも1つのスクレイピングオプションを指定してください")
        logger.info("使用例: python main.py --events --fighters --odds")
        return 1
    
    try:
        # ファイタースクレイピング
        if args.fighters:
            logger.info("=" * 50)
            logger.info("ファイター情報のスクレイピングを開始")
            if args.update:
                logger.info("モード: 差分更新")
            logger.info("=" * 50)
            
            scraper = FighterScraper(test_mode=args.test, update_mode=args.update, config=config)
            scraper.run()
        
        # イベントスクレイピング
        if args.events:
            logger.info("=" * 50)
            logger.info("イベント結果のスクレイピングを開始")
            if args.update:
                logger.info("モード: 差分更新")
            logger.info("=" * 50)
            
            scraper = EventsScraper(test_mode=args.test, update_mode=args.update, config=config)
            scraper.run()
        
        # オッズスクレイピング
        if args.odds:
            logger.info("=" * 50)
            logger.info("オッズ情報のスクレイピングを開始")
            if args.update:
                logger.info("モード: 差分更新")
            logger.info("=" * 50)
            
            scraper = OddsScraper(test_mode=args.test, update_mode=args.update, config=config)
            scraper.run()
        
        logger.info("=" * 50)
        logger.info("全ての処理が完了しました")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())