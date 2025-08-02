# UFC Fight Prediction

UFC試合予測モデルを構築し、毎週の試合予測を行うプロジェクトです。

## プロジェクト構造

```
ufc-fight-prediction/
├── src/
│   ├── config/
│   │   └── settings.py        # 設定管理クラス
│   ├── scraper/
│   │   ├── base.py           # ベーススクレイパークラス
│   │   ├── events.py         # イベントスクレイパー
│   │   ├── fighters.py       # ファイタースクレイパー
│   │   └── odds.py           # オッズスクレイパー
│   ├── utils/
│   │   ├── web.py            # Web関連ユーティリティ
│   │   ├── data.py           # データ処理ユーティリティ
│   │   └── aws.py            # AWS関連ユーティリティ
│   └── models/               # 機械学習モデル（今後実装）
├── data/                     # スクレイピングデータ
├── tests/                    # テストコード
├── config.yaml              # 設定ファイル
├── main.py                  # メインエントリーポイント
└── requirements.txt         # 依存パッケージ
```

## 主な機能

### 1. スクレイピング機能
- **イベント結果**: UFCの試合結果を取得
- **ファイター情報**: 各ファイターの詳細情報と戦績を取得
- **オッズ情報**: 試合のベッティングオッズを取得

### 2. 保守性の高い設計
- **ベースクラス**: 共通機能を集約し、コードの重複を排除
- **エラーハンドリング**: リトライ機能と適切なエラー処理
- **ログ機能**: 詳細なログ出力で問題の追跡が容易
- **データ検証**: スクレイピング結果の自動検証
- **プログレスバー**: 処理の進捗を視覚的に確認

## セットアップ

1. 依存関係のインストール
```bash
pip install -r requirements.txt
```

2. 設定ファイルの確認
`config.yaml`で各種設定を調整できます。

## 使い方

### 基本的な使用方法

```bash
# イベント結果のスクレイピング
python main.py --events

# ファイター情報のスクレイピング
python main.py --fighters

# オッズ情報のスクレイピング
python main.py --odds

# 全てのデータをスクレイピング
python main.py --events --fighters --odds

# テストモード（少量のデータのみ処理）
python main.py --events --test

# ログレベルを変更
python main.py --events --log-level DEBUG
```

### 環境変数

以下の環境変数で設定を上書きできます：
- `UFC_S3_BUCKET`: S3バケット名
- `UFC_LOG_LEVEL`: ログレベル（DEBUG, INFO, WARNING, ERROR）

## データ

スクレイピングされたデータは`data/`ディレクトリに保存されます：
- `events_raw.csv`: UFCイベント結果
- `fighters_raw.csv`: ファイター情報と統計
- `odds_raw.csv`: 試合オッズ

## 今後の予定

- [ ] 機械学習モデルの実装
- [ ] Lambda関数への移行
- [ ] S3での静的ホスティング
- [ ] 予測結果の可視化
- [ ] APIエンドポイントの実装

## 開発者向け情報

### テストの実行
```bash
pytest tests/
```

### コードスタイル
- 型ヒントを使用
- docstringで関数の説明を記載
- PEP 8に準拠