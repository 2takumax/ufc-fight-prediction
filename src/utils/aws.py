"""AWS関連のユーティリティ関数"""

from typing import Optional
import pandas as pd
from io import StringIO
import boto3
from botocore.exceptions import ClientError


class S3Handler:
    """S3操作を行うハンドラークラス"""
    
    def __init__(self, bucket_name: Optional[str] = None):
        """
        Args:
            bucket_name: S3バケット名
        """
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
    
    def read_csv(self, key: str, bucket: Optional[str] = None) -> pd.DataFrame:
        """S3からCSVファイルを読み込む
        
        Args:
            key: S3オブジェクトキー
            bucket: バケット名（Noneの場合はインスタンスのデフォルトを使用）
            
        Returns:
            読み込んだDataFrame
            
        Raises:
            FileNotFoundError: ファイルが存在しない場合
            ClientError: その他のS3エラー
        """
        bucket = bucket or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be specified")
        
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj['Body'])
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"S3 file not found: s3://{bucket}/{key}")
            raise
    
    def write_csv(self, df: pd.DataFrame, key: str, bucket: Optional[str] = None) -> None:
        """DataFrameをS3にCSVとして保存
        
        Args:
            df: 保存するDataFrame
            key: S3オブジェクトキー
            bucket: バケット名（Noneの場合はインスタンスのデフォルトを使用）
            
        Raises:
            ClientError: S3エラー
        """
        bucket = bucket or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be specified")
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=csv_buffer.getvalue()
            )
        except ClientError as e:
            raise
    
    def file_exists(self, key: str, bucket: Optional[str] = None) -> bool:
        """S3ファイルの存在確認
        
        Args:
            key: S3オブジェクトキー
            bucket: バケット名
            
        Returns:
            ファイルが存在する場合True
        """
        bucket = bucket or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be specified")
        
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def append_csv(self, new_df: pd.DataFrame, key: str, bucket: Optional[str] = None) -> None:
        """既存のCSVファイルに追記
        
        Args:
            new_df: 追記するDataFrame
            key: S3オブジェクトキー
            bucket: バケット名
        """
        bucket = bucket or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be specified")
        
        # 既存データを読み込む
        if self.file_exists(key, bucket):
            existing_df = self.read_csv(key, bucket)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df
        
        # 保存
        self.write_csv(combined_df, key, bucket)


def filter_new_records(existing_df: pd.DataFrame, 
                      target_df: pd.DataFrame, 
                      key_column: str) -> pd.DataFrame:
    """既存データに存在しない新規レコードを抽出
    
    Args:
        existing_df: 既存データ
        target_df: 対象データ
        key_column: 一意性を判定するカラム名
        
    Returns:
        新規レコードのみのDataFrame
    """
    if key_column not in existing_df.columns:
        raise ValueError(f"{key_column} not in existing_df")
    if key_column not in target_df.columns:
        raise ValueError(f"{key_column} not in target_df")
    
    existing_keys = set(existing_df[key_column].unique())
    return target_df[~target_df[key_column].isin(existing_keys)].reset_index(drop=True)