import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import time
from io import StringIO
import boto3
from botocore.exceptions import ClientError

def get_soup(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    return soup

def sleep_randomly():
    """Sleep for some random time between requests"""
    sleep_time = np.random.uniform(2,4)
    time.sleep(sleep_time)

def read_s3_csv(bucket: str, key: str) -> pd.DataFrame:
    """
    S3上のCSVファイルを読み込む関数。
    ファイルが存在しなければ例外をスローする。

    Args:
        bucket (str): S3バケット名
        key (str): S3オブジェクトキー（パス）

    Returns:
        pd.DataFrame: CSVを読み込んだDataFrame

    Raises:
        FileNotFoundError: 指定したS3キーが存在しない場合
        ClientError: その他のS3アクセスエラー
    """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise FileNotFoundError(f"S3 file not found: s3://{bucket}/{key}")
        else:
            raise  # 他のエラーはそのまま投げる

def filter_new_records(existing_df: pd.DataFrame, target_df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    """
    既存データと対象データの差分を抽出する共通関数。

    Args:
        existing_df (pd.DataFrame): 既存データ
        target_df (pd.DataFrame): スクレイピング対象候補
        key_column (str): 一意な判定に使うカラム名（例: "link"）

    Returns:
        pd.DataFrame: `target_df` のうち、`existing_df` に存在しないレコードだけ
    """
    if key_column not in existing_df.columns:
        raise ValueError(f"{key_column} is not in existing_df.columns")
    if key_column not in target_df.columns:
        raise ValueError(f"{key_column} is not in target_df.columns")

    existing_keys = set(existing_df[key_column].unique())
    return target_df[~target_df[key_column].isin(existing_keys)].reset_index(drop=True)

def append_to_s3_csv(new_df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    S3 に存在する CSV に対して、指定の DataFrame を追記する（実際には読み出してから結合して再保存する）。

    Args:
        new_df (pd.DataFrame): 追加したいデータ
        bucket (str): S3バケット名
        key (str): S3キー（ファイルパス）

    Raises:
        ClientError: S3アクセスに失敗した場合
    """
    s3 = boto3.client("s3")

    # 既存データを読み込む
    obj = s3.get_object(Bucket=bucket, Key=key)
    existing_df = pd.read_csv(obj["Body"])

    # 追記（結合）
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    # S3 に上書き保存（＝実質的な追記）
    csv_buffer = StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    except ClientError as e:
        raise
