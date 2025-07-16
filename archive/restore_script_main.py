#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
復元スクリプト メイン処理
AWS S3 Glacier Deep Archiveからファイルサーバへの復元処理
"""

import os
import sys
import json
import logging
import argparse
import datetime
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import csv

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = "config/archive_config.json"

class RestoreProcessor:
    """復元処理のメインクラス"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
        self.csv_errors = []  # CSV検証エラーを記録
        self.stats = {
            'total_files': 0,
            'restore_requested': 0,
            'restore_completed': 0,
            'failed_files': 0,
            'start_time': None,
            'end_time': None
        }
        
    def load_config(self, config_path: str) -> Dict:
        """設定ファイルを読み込み（アーカイブスクリプトと共通）"""
        # デフォルト設定
        default_config = {
            "logging": {
                "log_directory": "logs",
                "log_level": "INFO"
            },
            "restore": {
                "check_interval": 300,  # 5分間隔
                "max_wait_time": 86400,  # 24時間
                "restore_tier": "Standard"  # Standard, Expedited, Bulk
            },
            "processing": {
                "retry_count": 3
            }
        }
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            # デフォルト設定とマージ
            for key, value in default_config.items():
                if key not in config:
                    config[key] = value
                elif isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        if sub_key not in config[key]:
                            config[key][sub_key] = sub_value
                            
            return config
        except FileNotFoundError:
            print(f"設定ファイルが見つかりません。デフォルト設定を使用します: {config_path}")
            return default_config
        except json.JSONDecodeError as e:
            print(f"設定ファイルの形式が正しくありません。デフォルト設定を使用します: {e}")
            return default_config
        except Exception as e:
            print(f"設定ファイル読み込みエラー。デフォルト設定を使用します: {e}")
            return default_config
            
    def setup_logger(self) -> logging.Logger:
        """ログ設定の初期化"""
        logger = logging.getLogger('restore_processor')
        logger.setLevel(logging.INFO)
        
        # 既存のハンドラーをクリア
        logger.handlers.clear()
        
        # ログフォーマット
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # コンソール出力
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # ファイル出力
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            log_file = log_dir / f"restore_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            logger.info(f"ログファイル: {log_file}")
        except Exception as e:
            logger.warning(f"ログファイル設定エラー: {e}")
        
        return logger
        
    def validate_csv_input(self, csv_path: str) -> Tuple[List[Dict], List[Dict]]:
        """
        復元依頼CSV読み込み・検証処理
        
        Returns:
            Tuple[List[Dict], List[Dict]]: (有効な復元依頼リスト, エラー項目リスト)
        """
        self.logger.info(f"復元依頼CSV読み込み開始: {csv_path}")
        
        valid_requests = []
        self.csv_errors = []  # エラーリストをリセット
        
        try:
            # CSV読み込み
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            self.logger.info(f"読み込み行数: {len(lines)}")
            
            for i, line in enumerate(lines):
                line_num = i + 1
                clean_line = line.strip()
                
                if not clean_line:
                    continue
                
                # ヘッダー行をスキップ
                if i == 0 and ('復元対象' in clean_line or 'S3パス' in clean_line or 's3://' not in clean_line):
                    self.logger.info(f"行 {line_num}: ヘッダー行をスキップ")
                    continue
                
                # CSV行を分割
                try:
                    row = next(csv.reader([clean_line]))
                    if len(row) < 2:
                        error_item = {
                            'line_number': line_num,
                            'content': clean_line,
                            'error_reason': 'カラム数が不足しています（元ファイルパス, 復元先ディレクトリが必要）',
                            'original_line': line.rstrip()
                        }
                        self.csv_errors.append(error_item)
                        self.logger.error(f"行 {line_num}: カラム数不足 ✗")
                        continue
                    
                    original_file_path = row[0].strip()
                    restore_dir = row[1].strip()
                    
                    # 復元依頼の検証
                    validation_result = self._validate_restore_request(original_file_path, restore_dir)
                    
                    if validation_result['valid']:
                        request_item = {
                            'line_number': line_num,
                            'original_file_path': original_file_path,
                            'restore_directory': restore_dir,
                            's3_path': None,  # データベース検索で取得
                            'bucket': None,   # S3パス取得後に設定
                            'key': None       # S3パス取得後に設定
                        }
                        valid_requests.append(request_item)
                        self.logger.info(f"行 {line_num}: 有効な復元依頼追加 ✓")
                    else:
                        error_item = {
                            'line_number': line_num,
                            'content': f"{original_file_path} -> {restore_dir}",
                            'error_reason': validation_result['error_reason'],
                            'original_line': line.rstrip()
                        }
                        self.csv_errors.append(error_item)
                        self.logger.error(f"行 {line_num}: {validation_result['error_reason']} ✗")
                        
                except Exception as e:
                    error_item = {
                        'line_number': line_num,
                        'content': clean_line,
                        'error_reason': f'CSV解析エラー: {str(e)}',
                        'original_line': line.rstrip()
                    }
                    self.csv_errors.append(error_item)
                    self.logger.error(f"行 {line_num}: CSV解析エラー ✗")
            
        except Exception as e:
            self.logger.error(f"CSV読み込みエラー: {str(e)}")
            return [], []
        
        self.logger.info(f"CSV読み込み完了")
        self.logger.info(f"  - 有効な復元依頼数: {len(valid_requests)}")
        self.logger.info(f"  - エラー項目数: {len(self.csv_errors)}")
        
        return valid_requests, self.csv_errors
    
    def _validate_restore_request(self, original_file_path: str, restore_dir: str) -> Dict:
        """復元依頼の検証"""
        try:
            # 元ファイルパスの形式チェック
            if not original_file_path:
                return {'valid': False, 'error_reason': '元ファイルパスが指定されていません'}
            
            # パス長制限チェック
            if len(original_file_path) > 260:
                return {'valid': False, 'error_reason': 'ファイルパスが長すぎます（260文字制限）'}
            
            # 復元先ディレクトリの検証
            if not restore_dir:
                return {'valid': False, 'error_reason': '復元先ディレクトリが指定されていません'}
            
            # 復元先ディレクトリの存在チェック
            if not os.path.exists(restore_dir):
                return {'valid': False, 'error_reason': '復元先ディレクトリが存在しません'}
            
            if not os.path.isdir(restore_dir):
                return {'valid': False, 'error_reason': '復元先がディレクトリではありません'}
            
            # 書き込み権限チェック
            if not os.access(restore_dir, os.W_OK):
                return {'valid': False, 'error_reason': '復元先ディレクトリへの書き込み権限がありません'}
            
            return {'valid': True, 'error_reason': None}
            
        except Exception as e:
            return {'valid': False, 'error_reason': f'検証エラー: {str(e)}'}
    
    def lookup_s3_paths_from_database(self, restore_requests: List[Dict]) -> List[Dict]:
        """データベースから元ファイルパスに対応するS3パスを検索"""
        self.logger.info("データベースからS3パス検索開始")
        
        try:
            # データベース接続
            conn = self._connect_database()
            
            with conn:
                with conn.cursor() as cursor:
                    for request in restore_requests:
                        original_path = request['original_file_path']
                        
                        # S3パス検索
                        cursor.execute(
                            "SELECT s3_path, archive_date FROM archive_history WHERE original_file_path = %s",
                            (original_path,)
                        )
                        
                        result = cursor.fetchone()
                        if result:
                            s3_path, archive_date = result
                            request['s3_path'] = s3_path
                            request['bucket'] = self._extract_bucket_from_s3_path(s3_path)
                            request['key'] = self._extract_key_from_s3_path(s3_path)
                            request['archive_date'] = str(archive_date)
                            self.logger.info(f"S3パス見つかりました: {original_path} -> {s3_path}")
                        else:
                            request['s3_path'] = None
                            request['error'] = 'データベースにアーカイブ履歴が見つかりません'
                            self.logger.error(f"S3パス見つからず: {original_path}")
            
            self.logger.info("データベースS3パス検索完了")
            return restore_requests
            
        except Exception as e:
            self.logger.error(f"データベース検索エラー: {str(e)}")
            # エラー時は全リクエストにエラーマーク
            for request in restore_requests:
                request['s3_path'] = None
                request['error'] = f'データベース接続エラー: {str(e)}'
            return restore_requests
        
        finally:
            try:
                if 'conn' in locals():
                    conn.close()
            except Exception:
                pass
    
    def _connect_database(self):
        """データベース接続（アーカイブスクリプトと共通）"""
        try:
            import psycopg2
            
            # データベース設定取得
            db_config = self.config.get('database', {})
            
            # 接続パラメータ
            conn_params = {
                'host': db_config.get('host', 'localhost'),
                'port': db_config.get('port', 5432),
                'database': db_config.get('database', 'archive_system'),
                'user': db_config.get('user', 'postgres'),
                'password': db_config.get('password', ''),
                'connect_timeout': db_config.get('timeout', 30)
            }
            
            self.logger.info(f"データベース接続: {conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
            
            # 接続実行
            conn = psycopg2.connect(**conn_params)
            conn.autocommit = False
            
            # 接続テスト
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            self.logger.info("データベース接続成功")
            return conn
            
        except ImportError:
            raise Exception("psycopg2がインストールされていません。pip install psycopg2-binary を実行してください。")
        except Exception as e:
            raise Exception(f"データベース接続失敗: {str(e)}")
    
    def _extract_bucket_from_s3_path(self, s3_path: str) -> str:
        """S3パスからバケット名を抽出"""
        # s3://bucket/key/path -> bucket
        parts = s3_path.replace('s3://', '').split('/')
        return parts[0] if parts else ''
    
    def _extract_key_from_s3_path(self, s3_path: str) -> str:
        """S3パスからキーを抽出"""
        # s3://bucket/key/path -> key/path
        parts = s3_path.replace('s3://', '').split('/', 1)
        return parts[1] if len(parts) > 1 else ''
    
    def request_restore(self, restore_requests: List[Dict]) -> List[Dict]:
        """S3復元リクエスト送信"""
        self.logger.info("S3復元リクエスト送信開始")
        
        if not restore_requests:
            self.logger.info("復元リクエスト対象がありません")
            return restore_requests
        
        try:
            # S3クライアント初期化
            s3_client = self._initialize_s3_client()
            
            # 復元設定
            restore_config = self.config.get('restore', {})
            restore_tier = restore_config.get('restore_tier', 'Standard')  # Standard, Expedited, Bulk
            
            self.logger.info(f"S3復元リクエスト送信")
            self.logger.info(f"復元ティア: {restore_tier}")
            self.logger.info(f"処理対象ファイル数: {len(restore_requests)}")
            
            successful_requests = 0
            failed_requests = 0
            
            for request in restore_requests:
                bucket = request.get('bucket')
                key = request.get('key')
                original_path = request.get('original_file_path')
                
                if not bucket or not key:
                    request['restore_status'] = 'failed'
                    request['error'] = 'S3パス情報が不完全です'
                    failed_requests += 1
                    self.logger.error(f"✗ 復元リクエスト失敗: {original_path} - S3パス情報不完全")
                    continue
                
                try:
                    # S3復元リクエスト送信
                    self.logger.info(f"復元リクエスト送信中: {bucket}/{key}")
                    
                    s3_client.restore_object(
                        Bucket=bucket,
                        Key=key,
                        RestoreRequest={
                            'Days': 7,  # 復元後の保持日数
                            'GlacierJobParameters': {
                                'Tier': restore_tier
                            }
                        }
                    )
                    
                    # 成功
                    request['restore_status'] = 'requested'
                    request['restore_request_time'] = datetime.datetime.now().isoformat()
                    request['restore_tier'] = restore_tier
                    successful_requests += 1
                    self.logger.info(f"✓ 復元リクエスト送信成功: {original_path}")
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # 既に復元中の場合は正常として扱う
                    if 'RestoreAlreadyInProgress' in error_msg:
                        request['restore_status'] = 'already_in_progress'
                        request['restore_request_time'] = datetime.datetime.now().isoformat()
                        successful_requests += 1
                        self.logger.info(f"✓ 復元リクエスト既に進行中: {original_path}")
                    else:
                        request['restore_status'] = 'failed'
                        request['error'] = error_msg
                        failed_requests += 1
                        self.logger.error(f"✗ 復元リクエスト失敗: {original_path} - {error_msg}")
            
            # 統計更新
            self.stats['restore_requested'] = successful_requests
            self.stats['failed_files'] += failed_requests
            
            self.logger.info("S3復元リクエスト送信完了")
            self.logger.info(f"  - 成功: {successful_requests}件")
            self.logger.info(f"  - 失敗: {failed_requests}件")
            
            return restore_requests
            
        except Exception as e:
            self.logger.error(f"S3復元リクエスト処理でエラーが発生: {str(e)}")
            # 全リクエストを失敗としてマーク
            for request in restore_requests:
                request['restore_status'] = 'failed'
                request['error'] = f'S3初期化エラー: {str(e)}'
            return restore_requests
    
    def _initialize_s3_client(self):
        """S3クライアント初期化（アーカイブスクリプトと共通）"""
        try:
            import boto3
            from botocore.config import Config
            
            # AWS設定の取得
            aws_config = self.config.get('aws', {})
            region = aws_config.get('region', 'ap-northeast-1').strip()
            vpc_endpoint_url = aws_config.get('vpc_endpoint_url', '').strip()
            
            # boto3設定
            config = Config(
                region_name=region,
                retries={
                    'max_attempts': 3,
                    'mode': 'adaptive'
                }
            )
            
            # S3クライアント作成
            if vpc_endpoint_url:
                self.logger.info(f"VPCエンドポイント経由で接続: {vpc_endpoint_url}")
                s3_client = boto3.client('s3', endpoint_url=vpc_endpoint_url, config=config)
            else:
                self.logger.info("標準エンドポイント経由で接続")
                s3_client = boto3.client('s3', config=config)
            
            self.logger.info("S3クライアント初期化成功")
            return s3_client
            
        except ImportError:
            raise Exception("boto3がインストールされていません。pip install boto3 を実行してください。")
        except Exception as e:
            raise Exception(f"S3クライアント初期化失敗: {str(e)}")
    
    def check_restore_completion(self, restore_requests: List[Dict]) -> List[Dict]:
        """復元完了確認処理"""
        self.logger.info("復元完了確認開始")
        
        if not restore_requests:
            self.logger.info("復元確認対象がありません")
            return restore_requests
        
        # 復元リクエスト済みのファイルのみ確認
        pending_requests = [req for req in restore_requests 
                           if req.get('restore_status') in ['requested', 'already_in_progress']]
        
        if not pending_requests:
            self.logger.info("復元確認対象ファイルがありません")
            return restore_requests
        
        self.logger.info(f"復元ステータス確認対象: {len(pending_requests)}件")
        
        try:
            # S3クライアント初期化
            s3_client = self._initialize_s3_client()
            
            completed_count = 0
            still_pending_count = 0
            failed_count = 0
            
            for request in pending_requests:
                bucket = request.get('bucket')
                key = request.get('key')
                original_path = request.get('original_file_path')
                
                if not bucket or not key:
                    self.logger.error(f"S3パス情報不完全: {original_path}")
                    continue
                
                try:
                    # S3オブジェクトのメタデータを取得してrestoreステータスを確認
                    self.logger.debug(f"復元ステータス確認中: {bucket}/{key}")
                    
                    response = s3_client.head_object(Bucket=bucket, Key=key)
                    
                    # Restoreヘッダーの確認
                    restore_header = response.get('Restore')
                    
                    if restore_header is None:
                        # Restoreヘッダーがない = まだ復元リクエストが処理されていない
                        request['restore_status'] = 'pending'
                        request['restore_check_time'] = datetime.datetime.now().isoformat()
                        still_pending_count += 1
                        self.logger.info(f"復元処理中: {original_path}")
                        
                    elif 'ongoing-request="true"' in restore_header:
                        # 復元処理が進行中
                        request['restore_status'] = 'in_progress'
                        request['restore_check_time'] = datetime.datetime.now().isoformat()
                        still_pending_count += 1
                        self.logger.info(f"復元進行中: {original_path}")
                        
                    elif 'ongoing-request="false"' in restore_header:
                        # 復元完了
                        request['restore_status'] = 'completed'
                        request['restore_completed_time'] = datetime.datetime.now().isoformat()
                        request['restore_check_time'] = datetime.datetime.now().isoformat()
                        completed_count += 1
                        self.logger.info(f"✓ 復元完了: {original_path}")
                        
                        # 復元有効期限の抽出（可能であれば）
                        try:
                            # 例: 'ongoing-request="false", expiry-date="Fri, 21 Dec 2012 00:00:00 GMT"'
                            if 'expiry-date=' in restore_header:
                                expiry_part = restore_header.split('expiry-date=')[1]
                                expiry_date = expiry_part.split('"')[1]
                                request['restore_expiry'] = expiry_date
                                self.logger.debug(f"復元有効期限: {expiry_date}")
                        except Exception:
                            pass  # 有効期限の抽出に失敗しても処理継続
                            
                    else:
                        # 不明なステータス
                        request['restore_status'] = 'unknown'
                        request['restore_check_time'] = datetime.datetime.now().isoformat()
                        request['error'] = f"不明な復元ステータス: {restore_header}"
                        still_pending_count += 1
                        self.logger.warning(f"不明な復元ステータス: {original_path} - {restore_header}")
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # 特定のエラーハンドリング
                    if 'NoSuchKey' in error_msg:
                        request['restore_status'] = 'failed'
                        request['error'] = 'S3にファイルが見つかりません'
                    elif 'InvalidObjectState' in error_msg:
                        request['restore_status'] = 'failed'
                        request['error'] = 'オブジェクトがGlacierストレージクラスではありません'
                    else:
                        request['restore_status'] = 'check_failed'
                        request['error'] = f'復元ステータス確認エラー: {error_msg}'
                    
                    request['restore_check_time'] = datetime.datetime.now().isoformat()
                    failed_count += 1
                    self.logger.error(f"✗ 復元ステータス確認失敗: {original_path} - {error_msg}")
            
            # 統計更新
            self.stats['restore_completed'] = completed_count
            
            self.logger.info("復元完了確認完了")
            self.logger.info(f"  - 復元完了: {completed_count}件")
            self.logger.info(f"  - 処理中: {still_pending_count}件")
            self.logger.info(f"  - 確認失敗: {failed_count}件")
            
            # 完了ファイルがある場合の案内
            if completed_count > 0:
                self.logger.info(f"復元完了ファイルがあります。ダウンロード処理を実行してください:")
                self.logger.info(f"python restore_script_main.py [csv_path] {self.request_id} --download-only")
            
            if still_pending_count > 0:
                self.logger.info(f"まだ処理中のファイルがあります。しばらく待ってから再度確認してください")
            
            return restore_requests
            
        except Exception as e:
            self.logger.error(f"復元完了確認処理でエラーが発生: {str(e)}")
            # 全リクエストにエラーマーク
            for request in pending_requests:
                request['restore_status'] = 'check_failed'
                request['error'] = f'S3接続エラー: {str(e)}'
                request['restore_check_time'] = datetime.datetime.now().isoformat()
            return restore_requests
        
    def download_and_place_files(self, restore_requests: List[Dict]) -> List[Dict]:
        """ファイルダウンロード・配置処理"""
        self.logger.info("ファイルダウンロード・配置開始")
        
        # 復元完了ファイルのみを対象とする
        completed_requests = [req for req in restore_requests 
                             if req.get('restore_status') == 'completed']
        
        if not completed_requests:
            self.logger.info("ダウンロード対象ファイルがありません")
            return restore_requests
        
        self.logger.info(f"ダウンロード対象ファイル数: {len(completed_requests)}件")
        
        try:
            # S3クライアント初期化
            s3_client = self._initialize_s3_client()
            
            # 設定値取得
            restore_config = self.config.get('restore', {})
            retry_count = restore_config.get('download_retry_count', 3)
            skip_existing = restore_config.get('skip_existing_files', True)
            temp_dir = restore_config.get('temp_download_directory', 'temp_downloads')
            
            # 一時ダウンロードディレクトリの作成
            temp_path = Path(temp_dir)
            temp_path.mkdir(exist_ok=True)
            
            self.logger.info(f"一時ダウンロード先: {temp_path}")
            self.logger.info(f"同名ファイルスキップ: {skip_existing}")
            
            successful_downloads = 0
            failed_downloads = 0
            skipped_files = 0
            
            for i, request in enumerate(completed_requests, 1):
                bucket = request.get('bucket')
                key = request.get('key')
                original_path = request.get('original_file_path')
                restore_dir = request.get('restore_directory')
                
                if not all([bucket, key, original_path, restore_dir]):
                    self.logger.error(f"必要な情報が不足: {original_path}")
                    request['download_status'] = 'failed'
                    request['download_error'] = '必要な情報が不足しています'
                    failed_downloads += 1
                    continue
                
                # 進捗ログ
                self.logger.info(f"[{i}/{len(completed_requests)}] ダウンロード処理中: {original_path}")
                
                # 復元先ファイルパスの生成
                original_filename = os.path.basename(original_path)
                destination_path = os.path.join(restore_dir, original_filename)
                
                # 同名ファイルの存在チェック
                if skip_existing and os.path.exists(destination_path):
                    self.logger.info(f"同名ファイルが存在するためスキップ: {destination_path}")
                    request['download_status'] = 'skipped'
                    request['download_error'] = '同名ファイルが既に存在します'
                    request['destination_path'] = destination_path
                    skipped_files += 1
                    continue
                
                # 一時ファイルパスの生成
                temp_filename = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{original_filename}"
                temp_file_path = temp_path / temp_filename
                
                # S3からダウンロード（リトライ付き）
                download_result = self._download_file_with_retry(
                    s3_client, bucket, key, str(temp_file_path), retry_count
                )
                
                if not download_result['success']:
                    self.logger.error(f"✗ ダウンロード失敗: {original_path} - {download_result['error']}")
                    request['download_status'] = 'failed'
                    request['download_error'] = download_result['error']
                    failed_downloads += 1
                    continue
                
                # ファイルサイズ確認
                try:
                    downloaded_size = os.path.getsize(temp_file_path)
                    self.logger.debug(f"ダウンロードサイズ: {downloaded_size:,} bytes")
                    request['downloaded_size'] = downloaded_size
                except Exception as e:
                    self.logger.warning(f"ダウンロードサイズ確認エラー: {e}")
                
                # 最終配置（一時ファイル → 復元先）
                placement_result = self._place_file_to_destination(
                    str(temp_file_path), destination_path
                )
                
                if placement_result['success']:
                    # 成功
                    request['download_status'] = 'completed'
                    request['destination_path'] = destination_path
                    request['download_completed_time'] = datetime.datetime.now().isoformat()
                    successful_downloads += 1
                    self.logger.info(f"✓ ダウンロード完了: {original_path} -> {destination_path}")
                    
                    # 一時ファイルのクリーンアップ
                    try:
                        os.remove(temp_file_path)
                    except Exception as e:
                        self.logger.warning(f"一時ファイル削除エラー: {e}")
                    
                else:
                    # 配置失敗
                    self.logger.error(f"✗ ファイル配置失敗: {original_path} - {placement_result['error']}")
                    request['download_status'] = 'failed'
                    request['download_error'] = f"ファイル配置失敗: {placement_result['error']}"
                    failed_downloads += 1
                    
                    # 一時ファイルのクリーンアップ
                    try:
                        os.remove(temp_file_path)
                    except Exception as e:
                        self.logger.warning(f"一時ファイル削除エラー: {e}")
            
            # 統計更新
            self.stats['restore_completed'] = successful_downloads
            
            self.logger.info("ファイルダウンロード・配置完了")
            self.logger.info(f"  - 成功: {successful_downloads}件")
            self.logger.info(f"  - スキップ: {skipped_files}件")
            self.logger.info(f"  - 失敗: {failed_downloads}件")
            
            return restore_requests
            
        except Exception as e:
            self.logger.error(f"ダウンロード・配置処理でエラーが発生: {str(e)}")
            # 全ての完了リクエストを失敗としてマーク
            for request in completed_requests:
                if 'download_status' not in request:
                    request['download_status'] = 'failed'
                    request['download_error'] = f'処理エラー: {str(e)}'
            return restore_requests

    def _download_file_with_retry(self, s3_client, bucket: str, key: str, 
                                 local_path: str, max_retries: int) -> Dict:
        """S3からファイルダウンロード（リトライ付き）"""
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"ダウンロード試行 {attempt + 1}/{max_retries}: s3://{bucket}/{key}")
                
                # S3からダウンロード
                s3_client.download_file(bucket, key, local_path)
                
                # ダウンロード成功確認（0バイトファイルも成功として扱う）
                if os.path.exists(local_path):
                    file_size = os.path.getsize(local_path)
                    self.logger.debug(f"ダウンロード完了: ファイルサイズ {file_size} bytes")
                    return {'success': True, 'error': None, 'file_size': file_size}
                else:
                    raise Exception("ダウンロード後にファイルが作成されませんでした")
                    
            except Exception as e:
                error_msg = str(e)
                
                # S3側のエラー詳細をログ出力
                if hasattr(e, 'response'):
                    error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                    error_message = e.response.get('Error', {}).get('Message', 'Unknown')
                    self.logger.error(f"S3エラー: {error_code} - {error_message}")
                
                # 特定のエラーはリトライしない
                if any(err in error_msg for err in ['NoSuchKey', 'AccessDenied', 'InvalidObjectState']):
                    return {'success': False, 'error': error_msg}
                
                # 最後の試行でも失敗した場合
                if attempt == max_retries - 1:
                    return {'success': False, 'error': f'最大リトライ回数到達: {error_msg}'}
                
                # リトライ可能なエラーの場合は次の試行へ
                self.logger.warning(f"ダウンロード失敗 (試行 {attempt + 1}/{max_retries}): {error_msg}")
                
                # 失敗した一時ファイルがあれば削除
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                except Exception:
                    pass
                
                # 少し待機してからリトライ
                import time
                time.sleep(2 ** attempt)  # 指数バックオフ
        
        return {'success': False, 'error': '不明なエラー'}

    def _place_file_to_destination(self, temp_path: str, destination_path: str) -> Dict:
        """一時ファイルを最終配置先に移動"""
        try:
            # 配置先ディレクトリの確認・作成
            destination_dir = os.path.dirname(destination_path)
            if not os.path.exists(destination_dir):
                self.logger.warning(f"配置先ディレクトリが存在しません: {destination_dir}")
                return {'success': False, 'error': '配置先ディレクトリが存在しません'}
            
            # 書き込み権限確認
            if not os.access(destination_dir, os.W_OK):
                return {'success': False, 'error': '配置先ディレクトリへの書き込み権限がありません'}
            
            # ファイル移動
            import shutil
            shutil.move(temp_path, destination_path)
            
            # 移動成功確認
            if os.path.exists(destination_path):
                return {'success': True, 'error': None}
            else:
                return {'success': False, 'error': 'ファイル移動後に配置先にファイルが見つかりません'}
                
        except PermissionError:
            return {'success': False, 'error': '権限エラー: ファイル移動ができません'}
        except OSError as e:
            return {'success': False, 'error': f'OS エラー: {str(e)}'}
        except Exception as e:
            return {'success': False, 'error': f'予期しないエラー: {str(e)}'}
        
    def wait_for_restore_completion(self, restore_requests: List[Dict]) -> List[Dict]:
        """復元完了待機処理"""
        self.logger.info("復元完了待機開始")
        
        # TODO: 実装
        self.logger.info("復元完了待機完了")
        return restore_requests
        
    def generate_restore_error_csv(self, original_csv_path: str) -> Optional[str]:
        """復元エラーCSV生成"""
        if not self.csv_errors:
            return None
            
        self.logger.info("復元エラーCSV生成開始")
        
        try:
            # logsディレクトリにエラーCSVを出力
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            # ファイル名生成
            original_path = Path(original_csv_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            error_csv_path = log_dir / f"{original_path.stem}_restore_errors_{timestamp}.csv"
            
            # エラーCSVの生成
            with open(error_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                fieldnames = ['行番号', '内容', 'エラー理由', '元の行']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for item in self.csv_errors:
                    writer.writerow({
                        '行番号': item['line_number'],
                        '内容': item['content'],
                        'エラー理由': item['error_reason'],
                        '元の行': item['original_line']
                    })
            
            self.logger.info(f"復元エラーCSV生成完了: {error_csv_path}")
            return str(error_csv_path)
            
        except Exception as e:
            self.logger.error(f"復元エラーCSV生成失敗: {str(e)}")
            return None
        
    def print_statistics(self) -> None:
        """処理統計の表示"""
        elapsed_time = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info("=== 復元処理統計 ===")
        self.logger.info(f"処理時間: {elapsed_time}")
        self.logger.info(f"CSV検証エラー数: {len(self.csv_errors)}")
        self.logger.info(f"総復元依頼数: {self.stats['total_files']}")
        self.logger.info(f"復元リクエスト送信数: {self.stats['restore_requested']}")
        self.logger.info(f"復元完了数: {self.stats['restore_completed']}")
        self.logger.info(f"失敗数: {self.stats['failed_files']}")
        
    def run(self, csv_path: str, request_id: str, mode: str = 'request') -> int:
        """
        メイン処理の実行
        
        Args:
            csv_path: CSVファイルパス
            request_id: 復元依頼ID
            mode: 実行モード
                - 'request': 復元リクエスト送信のみ
                - 'download': 復元ステータス確認 + ダウンロード実行
        """
        self.stats['start_time'] = datetime.datetime.now()
        self.request_id = request_id
        
        try:
            self.logger.info(f"復元処理開始 - Request ID: {request_id}, Mode: {mode}")
            
            if mode == 'request':
                return self._run_restore_request(csv_path)
            elif mode == 'download':
                return self._run_download_files(csv_path)
            else:
                self.logger.error(f"無効なモード: {mode}")
                return 1
                
        except Exception as e:
            self.logger.error(f"復元処理中にエラーが発生しました: {str(e)}")
            return 1
            
        finally:
            self.stats['end_time'] = datetime.datetime.now()
            self.print_statistics()
    
    def _run_restore_request(self, csv_path: str) -> int:
        """復元リクエスト送信処理"""
        self.logger.info("=== 復元リクエスト送信モード ===")
        
        # 1. CSV読み込み・検証
        restore_requests, csv_errors = self.validate_csv_input(csv_path)
        
        # CSV検証エラー処理
        if csv_errors:
            error_csv_path = self.generate_restore_error_csv(csv_path)
            self.logger.warning(f"CSV検証エラーが発生しました: {error_csv_path}")
        
        if not restore_requests:
            self.logger.error("有効な復元依頼が見つかりません")
            return 1
            
        self.stats['total_files'] = len(restore_requests)
        
        # 2. データベースからS3パス検索
        restore_requests = self.lookup_s3_paths_from_database(restore_requests)
        
        # S3パスが見つからないファイルをフィルタリング
        valid_restore_requests = [req for req in restore_requests if req.get('s3_path')]
        failed_requests = [req for req in restore_requests if not req.get('s3_path')]
        
        if failed_requests:
            self.logger.warning(f"S3パスが見つからないファイル: {len(failed_requests)}件")
            for req in failed_requests:
                self.logger.warning(f"  - {req['original_file_path']}: {req.get('error', '不明')}")
        
        if not valid_restore_requests:
            self.logger.error("復元可能なファイルが見つかりません")
            return 1
        
        # 3. S3復元リクエスト送信
        restore_requests = self.request_restore(valid_restore_requests)
        
        # 4. ステータスファイル保存（失敗分も含む）
        all_requests = valid_restore_requests + failed_requests
        self._save_restore_status(all_requests)
        
        self.logger.info(f"復元リクエスト送信完了 - {self.stats['restore_requested']}件")
        if failed_requests:
            self.logger.warning(f"復元不可ファイル - {len(failed_requests)}件")
        self.logger.info("48時間後にダウンロード処理を実行してください:")
        self.logger.info(f"python restore_script_main.py {csv_path} {self.request_id} --download-only")
        
        return 0
    
    def _run_download_files(self, csv_path: str) -> int:
        """ダウンロード実行処理（復元ステータス確認込み）"""
        self.logger.info("=== ダウンロード実行モード ===")
        
        # 1. ステータスファイル読み込み
        restore_requests = self._load_restore_status()
        if not restore_requests:
            self.logger.error("復元ステータスファイルが見つかりません")
            self.logger.error("先に復元リクエスト送信を実行してください")
            return 1
        
        self.stats['total_files'] = len(restore_requests)
        
        # 2. 復元完了確認（最新ステータス取得）
        self.logger.info("復元ステータスを確認しています...")
        restore_requests = self.check_restore_completion(restore_requests)
        
        # 3. 復元完了ファイルのフィルタリング
        completed_requests = [req for req in restore_requests 
                             if req.get('restore_status') == 'completed']
        
        if not completed_requests:
            self.logger.info("復元完了ファイルがありません")
            pending_count = len([req for req in restore_requests 
                               if req.get('restore_status') in ['pending', 'in_progress']])
            if pending_count > 0:
                self.logger.info(f"復元処理中のファイル: {pending_count}件")
                self.logger.info("しばらく待ってから再度実行してください")
            return 0
        
        self.logger.info(f"復元完了ファイル: {len(completed_requests)}件をダウンロードします")
        
        # 4. ファイルダウンロード・配置
        restore_requests = self.download_and_place_files(restore_requests)
        
        # 5. ステータスファイル更新
        self._save_restore_status(restore_requests)
        
        downloaded_count = len([req for req in restore_requests 
                              if req.get('download_status') == 'completed'])
        skipped_count = len([req for req in restore_requests 
                           if req.get('download_status') == 'skipped'])
        
        self.logger.info(f"ダウンロード処理完了")
        self.logger.info(f"  - ダウンロード成功: {downloaded_count}件")
        self.logger.info(f"  - スキップ: {skipped_count}件")
        
        # 未完了ファイルがある場合の案内
        remaining_count = len([req for req in restore_requests 
                             if req.get('restore_status') in ['pending', 'in_progress']])
        if remaining_count > 0:
            self.logger.info(f"復元処理中のファイル: {remaining_count}件")
            self.logger.info("復元完了後に再度ダウンロード処理を実行してください")
        
        return 0
    
    def _save_restore_status(self, restore_requests: List[Dict]) -> None:
        """復元ステータスをファイルに保存"""
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            status_file = log_dir / f"restore_status_{self.request_id}.json"
            
            status_data = {
                "request_id": self.request_id,
                "request_date": datetime.datetime.now().isoformat(),
                "total_requests": len(restore_requests),
                "restore_requests": restore_requests
            }
            
            with open(status_file, 'w', encoding='utf-8') as f:
                json.dump(status_data, f, ensure_ascii=False, indent=2, default=str)
            
            self.logger.info(f"復元ステータス保存: {status_file}")
            
        except Exception as e:
            self.logger.error(f"復元ステータス保存エラー: {str(e)}")
    
    def _load_restore_status(self) -> List[Dict]:
        """復元ステータスをファイルから読み込み"""
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            status_file = log_dir / f"restore_status_{self.request_id}.json"
            
            if not status_file.exists():
                self.logger.error(f"復元ステータスファイルが存在しません: {status_file}")
                return []
            
            with open(status_file, 'r', encoding='utf-8') as f:
                status_data = json.load(f)
            
            restore_requests = status_data.get('restore_requests', [])
            self.logger.info(f"復元ステータス読み込み完了: {len(restore_requests)}件")
            
            return restore_requests
            
        except Exception as e:
            self.logger.error(f"復元ステータス読み込みエラー: {str(e)}")
            return []


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='ファイル復元処理')
    parser.add_argument('csv_path', help='復元依頼を記載したCSVファイルのパス')
    parser.add_argument('request_id', help='復元依頼ID')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH, 
                       help=f'設定ファイルのパス (デフォルト: {DEFAULT_CONFIG_PATH})')
    
    # 実行モード指定
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--request-only', action='store_true',
                           help='復元リクエスト送信のみ実行')
    mode_group.add_argument('--download-only', action='store_true',
                           help='復元ステータス確認 + ダウンロード実行')
    
    args = parser.parse_args()
    
    # CSVファイルの存在チェック
    if not os.path.exists(args.csv_path):
        print(f"CSVファイルが見つかりません: {args.csv_path}")
        sys.exit(1)
    
    # 実行モード決定
    if args.request_only:
        mode = 'request'
    elif args.download_only:
        mode = 'download'
    else:
        # このケースは発生しないはず（mutually_exclusive_group + required=True）
        print("実行モードを指定してください: --request-only または --download-only")
        sys.exit(1)
        
    # 復元処理の実行
    processor = RestoreProcessor(args.config)
    exit_code = processor.run(args.csv_path, args.request_id, mode)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()