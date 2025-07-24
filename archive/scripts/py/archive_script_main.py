#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
確実に動作するアーカイブスクリプト（最終版）
"""

import os
import sys
import json
import logging
import argparse
import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import csv

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = "config/archive_config.json"

class ArchiveProcessor:
    """アーカイブ処理のメインクラス"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
        self.csv_errors = []  # CSV検証エラーを記録
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'total_size': 0,
            'start_time': None,
            'end_time': None
        }
        
    def load_config(self, config_path: str) -> Dict:
        """設定ファイルを読み込み"""
        # デフォルト設定
        default_config = {
            "logging": {
                "log_directory": "logs",
                "log_level": "INFO"
            },
            "file_server": {
                "exclude_extensions": [".tmp", ".lock", ".bak"],
                "archived_suffix": "_archived.txt"
            },
            "processing": {
                "max_file_size": 10737418240,
                "chunk_size": 8388608,
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
        logger = logging.getLogger('archive_processor')
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
            
            log_file = log_dir / f"archive_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            logger.info(f"ログファイル: {log_file}")
        except Exception as e:
            logger.warning(f"ログファイル設定エラー: {e}")
        
        return logger
        
    def validate_csv_input(self, csv_path: str) -> Tuple[List[str], List[Dict]]:
        """
        CSVファイルの読み込み・検証処理（エラー項目記録版）
        
        Returns:
            Tuple[List[str], List[Dict]]: (有効なディレクトリリスト, エラー項目リスト)
        """
        self.logger.info(f"CSVファイルの読み込み開始: {csv_path}")
        
        valid_directories = []
        self.csv_errors = []  # エラーリストをリセット
        
        try:
            # シンプルに行単位で読み込み
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            self.logger.info(f"読み込み行数: {len(lines)}")
            
            for i, line in enumerate(lines):
                line_num = i + 1
                clean_line = line.strip()
                
                if not clean_line:
                    continue
                
                # ヘッダー行をスキップ
                if i == 0 and any(keyword in clean_line.lower() for keyword in ['directory', 'path']):
                    self.logger.info(f"行 {line_num}: ヘッダー行をスキップ")
                    continue
                
                # パスとして処理
                path = clean_line
                path_preview = path[:50] + "..." if len(path) > 50 else path
                self.logger.info(f"行 {line_num}: パス検証中 - {path_preview}")
                
                # 最低限の長さチェック
                if len(path) < 3:
                    error_item = {
                        'line_number': line_num,
                        'path': path,
                        'error_reason': 'パスが短すぎます',
                        'original_line': line.rstrip()
                    }
                    self.csv_errors.append(error_item)
                    self.logger.warning(f"行 {line_num}: パスが短すぎます")
                    continue
                
                # パスの妥当性チェック
                validation_result = self._validate_directory_path_with_details(path)
                
                if validation_result['valid']:
                    valid_directories.append(path)
                    self.logger.info(f"行 {line_num}: 有効なパス追加 ✓")
                else:
                    # エラー項目として記録（処理は継続）
                    error_item = {
                        'line_number': line_num,
                        'path': path,
                        'error_reason': validation_result['error_reason'],
                        'original_line': line.rstrip()
                    }
                    self.csv_errors.append(error_item)
                    self.logger.error(f"行 {line_num}: {validation_result['error_reason']} ✗")
            
        except Exception as e:
            self.logger.error(f"CSV読み込みエラー: {str(e)}")
            # CSVファイル自体が読み込めない場合は処理中断
            return [], []
        
        self.logger.info(f"CSV読み込み完了")
        self.logger.info(f"  - 有効ディレクトリ数: {len(valid_directories)}")
        self.logger.info(f"  - エラー項目数: {len(self.csv_errors)}")
        
        return valid_directories, self.csv_errors

    def _validate_directory_path_with_details(self, path: str) -> Dict:
        """
        ディレクトリパスの詳細検証
        
        Returns:
            Dict: {'valid': bool, 'error_reason': str}
        """
        try:
            if not path or path.strip() == '':
                return {'valid': False, 'error_reason': '空のパス'}
            
            # 不正な文字チェック
            invalid_chars = ['<', '>', ':', '"', '|', '?', '*']
            check_path = path[2:] if path.startswith('\\\\') else path
            for char in invalid_chars:
                if char in check_path:
                    return {'valid': False, 'error_reason': f'不正な文字が含まれています: {char}'}
            
            # パスの長さチェック
            if len(path) > 260:
                return {'valid': False, 'error_reason': f'パスが長すぎます: {len(path)} > 260'}
            
            # 存在チェック
            if not os.path.exists(path):
                return {'valid': False, 'error_reason': 'ディレクトリが存在しません'}
            
            # ディレクトリチェック
            if not os.path.isdir(path):
                return {'valid': False, 'error_reason': 'ディレクトリではありません（ファイルです）'}
            
            # 権限チェック
            if not os.access(path, os.R_OK):
                return {'valid': False, 'error_reason': '読み取り権限がありません'}
            
            return {'valid': True, 'error_reason': None}
            
        except Exception as e:
            return {'valid': False, 'error_reason': f'パス検証エラー: {str(e)}'}
    
    def _validate_directory_path(self, path: str) -> bool:
        """ディレクトリパスの妥当性チェック（旧版互換）"""
        result = self._validate_directory_path_with_details(path)
        return result['valid']
        
    def collect_files(self, directories: List[str]) -> List[Dict]:
        """ファイル列挙・収集処理"""
        self.logger.info("ファイル収集開始")
        
        files = []
        exclude_extensions = self.config.get('file_server', {}).get('exclude_extensions', [])
        max_file_size = self.config.get('processing', {}).get('max_file_size', 10737418240)
        
        for directory in directories:
            dir_preview = directory[:50] + "..." if len(directory) > 50 else directory
            self.logger.info(f"ディレクトリ処理開始: {dir_preview}")
            
            try:
                file_count = 0
                for root, dirs, filenames in os.walk(directory):
                    for filename in filenames:
                        file_path = os.path.join(root, filename)
                        
                        # 拡張子チェック
                        _, ext = os.path.splitext(filename)
                        if ext.lower() in exclude_extensions:
                            continue
                        
                        try:
                            stat_info = os.stat(file_path)
                            file_size = stat_info.st_size
                            
                            if file_size > max_file_size:
                                continue
                            
                            file_info = {
                                'path': file_path,
                                'size': file_size,
                                'modified_time': datetime.datetime.fromtimestamp(stat_info.st_mtime),
                                'directory': directory
                            }
                            
                            files.append(file_info)
                            file_count += 1
                            
                        except OSError:
                            continue
                
                self.logger.info(f"ディレクトリ {dir_preview}: {file_count}個のファイルを収集")
                        
            except Exception as e:
                self.logger.error(f"ディレクトリ処理エラー: {str(e)}")
                continue
        
        self.logger.info(f"ファイル収集完了 - 総ファイル数: {len(files)}")
        return files
        
    def archive_to_s3(self, files: List[Dict]) -> List[Dict]:
        """S3アップロード処理"""
        self.logger.info("S3アップロード開始")
        
        try:
            # boto3 S3クライアントの初期化
            s3_client = self._initialize_s3_client()
            
            # 設定値の取得
            bucket_name = self.config['aws']['s3_bucket']
            storage_class = self.config['aws'].get('storage_class', 'STANDARD')
            max_retries = self.config['processing'].get('retry_count', 3)
            
            # ストレージクラスの検証・調整
            storage_class = self._validate_storage_class(storage_class)
            
            self.logger.info(f"S3バケット: {bucket_name}")
            self.logger.info(f"ストレージクラス: {storage_class}")
            self.logger.info(f"処理対象ファイル数: {len(files)}")
            
            results = []
            successful_uploads = 0
            failed_uploads = 0
            
            for i, file_info in enumerate(files, 1):
                file_path = file_info['path']
                file_size = file_info['size']
                
                # 進捗ログ
                self.logger.info(f"[{i}/{len(files)}] アップロード中: {file_path} ({file_size:,} bytes)")
                
                # S3キーの生成
                s3_key = self._generate_s3_key(file_path)
                
                # アップロード実行（リトライ付き）
                upload_result = self._upload_file_with_retry(
                    s3_client, file_path, bucket_name, s3_key, storage_class, max_retries
                )
                
                if upload_result['success']:
                    successful_uploads += 1
                    self.logger.info(f"✓ アップロード成功: {s3_key}")
                else:
                    failed_uploads += 1
                    self.logger.error(f"✗ アップロード失敗: {file_path} - {upload_result['error']}")
                
                # 結果をリストに追加
                result = {
                    'file_path': file_path,
                    'file_size': file_size,
                    'directory': file_info['directory'],
                    'success': upload_result['success'],
                    'error': upload_result.get('error'),
                    's3_key': s3_key if upload_result['success'] else None,
                    'modified_time': file_info['modified_time']
                }
                results.append(result)
            
            self.logger.info(f"S3アップロード完了")
            self.logger.info(f"  - 成功: {successful_uploads}件")
            self.logger.info(f"  - 失敗: {failed_uploads}件")
            
            return results
            
        except Exception as e:
            self.logger.error(f"S3アップロード処理でエラーが発生: {str(e)}")
            # 全てのファイルを失敗として記録
            return [
                {
                    'file_path': f['path'],
                    'file_size': f['size'],
                    'directory': f['directory'],
                    'success': False,
                    'error': f"S3初期化エラー: {str(e)}",
                    's3_key': None,
                    'modified_time': f['modified_time']
                }
                for f in files
            ]
    
    def _validate_storage_class(self, storage_class: str) -> str:
        """ストレージクラスの検証と調整"""
        # GLACIER_DEEP_ARCHIVE -> DEEP_ARCHIVE の自動変換
        if storage_class == 'GLACIER_DEEP_ARCHIVE':
            self.logger.info(f"'{storage_class}' を 'DEEP_ARCHIVE' に自動変換")
            return 'DEEP_ARCHIVE'
        
        # 有効なストレージクラスのリスト
        valid_classes = [
            'STANDARD',
            'STANDARD_IA', 
            'GLACIER',
            'DEEP_ARCHIVE'
        ]
        
        # 設定値をそのまま試す
        if storage_class in valid_classes:
            self.logger.info(f"ストレージクラス '{storage_class}' を使用")
            return storage_class
        
        # フォールバック: STANDARDを使用
        self.logger.warning(f"無効なストレージクラス '{storage_class}' のため 'STANDARD' に変更")
        return 'STANDARD'

    def _initialize_s3_client(self):
        """S3クライアントの初期化"""
        try:
            import boto3
            from botocore.config import Config
            
            # AWS設定の取得（スペース除去）
            aws_config = self.config.get('aws', {})
            region = aws_config.get('region', 'ap-northeast-1').strip()
            bucket_name = aws_config.get('s3_bucket', '').strip()
            vpc_endpoint_url = aws_config.get('vpc_endpoint_url', '').strip()
            
            # バケット名の検証
            if not bucket_name:
                raise Exception("S3バケット名が設定されていません")
            
            self.logger.info(f"S3バケット名: '{bucket_name}'")  # デバッグ用に引用符で囲む
            
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
            
            # 接続テスト
            self._test_s3_connection(s3_client, bucket_name)
            
            return s3_client
            
        except ImportError:
            raise Exception("boto3がインストールされていません。pip install boto3 を実行してください。")
        except Exception as e:
            raise Exception(f"S3クライアント初期化失敗: {str(e)}")
    
    def _test_s3_connection(self, s3_client, bucket_name):
        """S3接続テスト"""
        try:
            # バケットの存在確認
            response = s3_client.head_bucket(Bucket=bucket_name)
            self.logger.info(f"S3バケット接続確認OK: '{bucket_name}'")
        except Exception as e:
            raise Exception(f"S3バケット接続失敗: '{bucket_name}' - {str(e)}")
    
    def _generate_s3_key(self, file_path: str) -> str:
        """ファイルパスからS3キーを生成"""
        try:
            # Windowsパスを正規化
            normalized_path = file_path.replace('\\', '/')
            
            # UNCパスの場合、サーバ名を抽出
            if normalized_path.startswith('//'):
                # //server/share/path/file.txt -> server/share/path/file.txt
                parts = normalized_path[2:].split('/')
                if len(parts) > 0:
                    server_name = parts[0]
                    if len(parts) > 1:
                        # サーバ名をトップレベルフォルダとして使用
                        relative_path = '/'.join(parts[1:])
                        s3_key = f"{server_name}/{relative_path}"
                    else:
                        s3_key = f"{server_name}/root"
                else:
                    s3_key = "unknown_server/unknown_path"
            # ドライブレター除去（C:/path/file.txt -> local/path/file.txt）
            elif len(normalized_path) > 2 and normalized_path[1] == ':':
                drive_letter = normalized_path[0].lower()
                relative_path = normalized_path[3:]  # C:/ を除去
                s3_key = f"local_{drive_letter}/{relative_path}"
            else:
                # その他の場合
                s3_key = f"other/{normalized_path}"
            
            # 先頭のスラッシュを除去
            s3_key = s3_key.lstrip('/')
            
            # 空の部分を除去
            s3_key = '/'.join(part for part in s3_key.split('/') if part)
            
            return s3_key
            
        except Exception as e:
            self.logger.error(f"S3キー生成エラー: {file_path} - {str(e)}")
            # フォールバック: ファイル名のみ使用
            import os
            filename = os.path.basename(file_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            return f"fallback/{timestamp}/{filename}"
    
    def _upload_file_with_retry(self, s3_client, file_path: str, bucket_name: str, 
                               s3_key: str, storage_class: str, max_retries: int) -> Dict:
        """ファイルアップロード（リトライ付き）"""
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"アップロード試行 {attempt + 1}/{max_retries}: {s3_key}")
                
                # アップロード実行
                s3_client.upload_file(
                    file_path,
                    bucket_name,
                    s3_key,
                    ExtraArgs={
                        'StorageClass': storage_class
                    }
                )
                
                # 成功
                return {'success': True, 'error': None}
                
            except FileNotFoundError:
                # ファイルが見つからない場合はリトライしない
                return {'success': False, 'error': 'ファイルが見つかりません'}
                
            except PermissionError:
                # 権限エラーの場合はリトライしない
                return {'success': False, 'error': 'ファイルアクセス権限がありません'}
                
            except Exception as e:
                error_msg = str(e)
                
                # 最後の試行でも失敗した場合
                if attempt == max_retries - 1:
                    return {'success': False, 'error': f'最大リトライ回数到達: {error_msg}'}
                
                # リトライ可能なエラーの場合は次の試行へ
                self.logger.warning(f"アップロード失敗 (試行 {attempt + 1}/{max_retries}): {error_msg}")
                
                # 少し待機してからリトライ
                import time
                time.sleep(2 ** attempt)  # 指数バックオフ
        
        return {'success': False, 'error': '不明なエラー'}
        
    def create_archived_files(self, results: List[Dict]) -> List[Dict]:
        """アーカイブ後処理（空ファイル作成→元ファイル削除）"""
        self.logger.info("アーカイブ後処理開始")
        
        # 成功したファイルのみ処理
        successful_results = [r for r in results if r.get('success', False)]
        
        if not successful_results:
            self.logger.info("S3アップロード成功ファイルがないため、アーカイブ後処理をスキップ")
            return results
        
        self.logger.info(f"アーカイブ後処理対象: {len(successful_results)}件")
        
        archived_suffix = self.config.get('file_server', {}).get('archived_suffix', '_archived')
        processed_results = []
        
        for result in results:
            if not result.get('success', False):
                # 失敗したファイルはそのまま
                processed_results.append(result)
                continue
            
            file_path = result['file_path']
            
            try:
                # 1. 空ファイル作成
                archived_file_path = f"{file_path}{archived_suffix}"
                
                self.logger.info(f"空ファイル作成: {archived_file_path}")
                
                # 完全に空のファイル（0バイト）を作成
                with open(archived_file_path, 'w') as f:
                    pass  # 何も書かない（空ファイル）
                
                # 空ファイル作成確認
                if not os.path.exists(archived_file_path):
                    raise Exception("空ファイルの作成に失敗しました")
                
                # 2. 空ファイル作成成功後に元ファイル削除
                self.logger.info(f"元ファイル削除: {file_path}")
                
                os.remove(file_path)
                
                # 元ファイル削除確認
                if os.path.exists(file_path):
                    raise Exception("元ファイルの削除に失敗しました")
                
                # 成功
                result['archived_file_path'] = archived_file_path
                result['archive_completed'] = True
                self.logger.info(f"✓ アーカイブ後処理完了: {file_path}")
                
            except Exception as e:
                # アーカイブ後処理失敗
                error_msg = f"アーカイブ後処理失敗: {str(e)}"
                self.logger.error(f"✗ {error_msg}: {file_path}")
                
                # 失敗時のクリーンアップ
                try:
                    # 作成済みの空ファイルがあれば削除
                    if 'archived_file_path' in locals() and os.path.exists(archived_file_path):
                        os.remove(archived_file_path)
                        self.logger.info(f"作成済み空ファイルを削除: {archived_file_path}")
                except Exception as cleanup_error:
                    self.logger.warning(f"空ファイルクリーンアップ失敗: {cleanup_error}")
                
                # 結果を失敗に変更
                result['success'] = False
                result['error'] = error_msg
                result['archive_completed'] = False
            
            processed_results.append(result)
        
        # 処理結果のサマリー
        completed_count = len([r for r in processed_results if r.get('archive_completed', False)])
        failed_count = len([r for r in processed_results if r.get('success', False) and not r.get('archive_completed', False)])
        
        self.logger.info("アーカイブ後処理完了")
        self.logger.info(f"  - 完了: {completed_count}件")
        self.logger.info(f"  - 失敗: {failed_count}件")
        
        return processed_results
        
    def save_to_database(self, results: List[Dict]) -> None:
        """データベース登録処理"""
        self.logger.info("データベース登録開始")
        
        # アーカイブ後処理完了ファイルのみ登録
        completed_results = [r for r in results if r.get('archive_completed', False)]
        
        if not completed_results:
            self.logger.info("データベース登録対象ファイルがありません")
            return
        
        self.logger.info(f"データベース登録対象: {len(completed_results)}件")
        
        try:
            # データベース接続
            conn = self._connect_database()
            
            # トランザクション開始
            with conn:
                with conn.cursor() as cursor:
                    # 設定から依頼情報を取得（コマンドライン引数を優先）
                    request_config = self.config.get('request', {})
                    request_id = self.request_id  # コマンドライン引数を使用
                    requester = request_config.get('requester', '00000000')
                    
                    # デバッグ用ログ追加
                    self.logger.info(f"デバッグ: request_id='{request_id}' (長さ:{len(request_id)})")
                    self.logger.info(f"デバッグ: requester='{requester}' (長さ:{len(requester)})")
                    
                    # 現在時刻
                    current_time = datetime.datetime.now()
                    
                    # バケット名を取得（S3 URL生成用）
                    bucket_name = self.config.get('aws', {}).get('s3_bucket', '')
                    
                    # バッチ挿入用のデータ準備
                    insert_data = []
                    for result in completed_results:
                        # S3完全URLの生成
                        s3_key = result.get('s3_key', '')
                        s3_url = f"s3://{bucket_name}/{s3_key}" if s3_key else ''
                        
                        record = (
                            request_id,
                            requester,
                            current_time,  # request_date
                            result['file_path'],  # original_file_path
                            s3_url,  # s3_path
                            current_time,  # archive_date
                            result['file_size']
                        )
                        insert_data.append(record)
                    
                    # バッチ挿入実行
                    insert_query = """
                        INSERT INTO archive_history (
                            request_id, requester, request_date,
                            original_file_path, s3_path, archive_date, file_size
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.executemany(insert_query, insert_data)
                    
                    # 挿入件数確認
                    inserted_count = cursor.rowcount
                    self.logger.info(f"データベース挿入完了: {inserted_count}件")
                    
                    # コミットは with文で自動実行
            
            self.logger.info("データベース登録完了")
            
        except Exception as e:
            self.logger.error(f"データベース登録エラー: {str(e)}")
            # エラーでも処理は継続（アーカイブ自体は成功しているため）
            
        finally:
            # 接続クローズ
            try:
                if 'conn' in locals():
                    conn.close()
            except Exception:
                pass
    
    def _connect_database(self):
        """データベース接続"""
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
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
            
            # 自動コミットを無効化（トランザクション管理のため）
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
        
    def generate_csv_error_file(self, original_csv_path: str) -> Optional[str]:
        """CSV検証エラー用のエラーファイル生成（再試行用フォーマット）"""
        if not self.csv_errors:
            return None
            
        self.logger.info("CSV検証エラーファイル生成開始")
        
        try:
            # logsディレクトリにエラーCSVを出力
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            # ファイル名生成
            original_path = Path(original_csv_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            error_csv_path = log_dir / f"{original_path.stem}_csv_retry_{timestamp}.csv"
            
            # 元CSVのヘッダーを取得
            original_header = self._get_original_csv_header(original_csv_path)
            
            # 再試行用CSVの生成（元のフォーマットと同じ）
            with open(error_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                
                # ヘッダー行を書き込み
                if original_header:
                    writer.writerow([original_header])
                
                # エラーが発生したパスのみを書き込み
                for item in self.csv_errors:
                    writer.writerow([item['path']])
                    # 詳細なエラー理由はログに出力済み
            
            self.logger.info(f"CSV検証エラー（再試行用）ファイル生成完了: {error_csv_path}")
            self.logger.info(f"再試行対象パス数: {len(self.csv_errors)}")
            return str(error_csv_path)
            
        except Exception as e:
            self.logger.error(f"CSV検証エラーファイル生成失敗: {str(e)}")
            return None
    
    def generate_error_csv(self, failed_items: List[Dict], original_csv_path: str) -> Optional[str]:
        """アーカイブ処理失敗ファイル用の再試行CSVファイル生成"""
        if not failed_items:
            return None
            
        self.logger.info("アーカイブエラー（再試行用）ファイル生成開始")
        
        try:
            # logsディレクトリにエラーCSVを出力
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            # ファイル名生成
            original_path = Path(original_csv_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            error_csv_path = log_dir / f"{original_path.stem}_archive_retry_{timestamp}.csv"
            
            # 元CSVのヘッダーを取得
            original_header = self._get_original_csv_header(original_csv_path)
            
            # 失敗したファイルのディレクトリを収集（重複除去）
            failed_directories = set()
            for item in failed_items:
                directory = item.get('directory')
                if directory:
                    failed_directories.add(directory)
            
            # 再試行用CSVの生成（元のフォーマットと同じ）
            with open(error_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                
                # ヘッダー行を書き込み
                if original_header:
                    writer.writerow([original_header])
                
                # 失敗したディレクトリのみを書き込み
                for directory in sorted(failed_directories):
                    writer.writerow([directory])
                    # 詳細なエラー理由はログに出力済み
            
            self.logger.info(f"アーカイブエラー（再試行用）ファイル生成完了: {error_csv_path}")
            self.logger.info(f"再試行対象ディレクトリ数: {len(failed_directories)}")
            self.logger.info(f"失敗ファイル数: {len(failed_items)}")
            
            # エラー理由の統計をログに出力
            error_summary = {}
            for item in failed_items:
                error_type = item.get('error', '不明なエラー')
                error_summary[error_type] = error_summary.get(error_type, 0) + 1
            
            self.logger.info("エラー理由の内訳:")
            for error_type, count in error_summary.items():
                self.logger.info(f"  - {error_type}: {count}件")
            
            return str(error_csv_path)
            
        except Exception as e:
            self.logger.error(f"アーカイブエラーファイル生成失敗: {str(e)}")
            return None
    
    def _get_original_csv_header(self, csv_path: str) -> Optional[str]:
        """元CSVファイルのヘッダー行を取得"""
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                # ヘッダー行と判定される場合のみ返却
                if any(keyword in first_line.lower() for keyword in ['directory', 'path']):
                    return first_line
                return "Directory Path"  # デフォルトヘッダー
        except Exception:
            return "Directory Path"  # デフォルトヘッダー
        
    def print_statistics(self) -> None:
        """処理統計の表示"""
        elapsed_time = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info("=== 処理統計 ===")
        self.logger.info(f"処理時間: {elapsed_time}")
        self.logger.info(f"CSV検証エラー数: {len(self.csv_errors)}")
        self.logger.info(f"総ファイル数: {self.stats['total_files']}")
        self.logger.info(f"成功ファイル数: {self.stats['processed_files']}")
        self.logger.info(f"失敗ファイル数: {self.stats['failed_files']}")
        self.logger.info(f"総ファイルサイズ: {self.stats['total_size']:,} bytes")
        
    def run(self, csv_path: str, request_id: str) -> int:
        """メイン処理の実行"""
        self.stats['start_time'] = datetime.datetime.now()
        
        # request_idをインスタンス変数として保存
        self.request_id = request_id
        
        try:
            self.logger.info(f"アーカイブ処理開始 - Request ID: {request_id}")
            
            # 1. CSVファイル読み込み・検証
            directories, csv_errors = self.validate_csv_input(csv_path)
            
            # CSV検証エラーがあった場合はエラーファイルを生成
            if csv_errors:
                error_csv_path = self.generate_csv_error_file(csv_path)
                self.logger.warning(f"CSV検証エラーが発生しました: {error_csv_path}")
            
            if not directories:
                self.logger.error("処理対象のディレクトリが見つかりません")
                return 1
                
            # 2. ファイル収集
            files = self.collect_files(directories)
            if not files:
                self.logger.warning("処理対象のファイルが見つかりません")
                return 0
                
            self.stats['total_files'] = len(files)
            self.stats['total_size'] = sum(f['size'] for f in files)
            
            # 3. S3アップロード
            upload_results = self.archive_to_s3(files)
            
            # 4. アーカイブ後処理
            processed_results = self.create_archived_files(upload_results)
            
            # 5. データベース登録
            self.save_to_database(processed_results)
            
            # 6. アーカイブ処理エラー処理
            failed_items = [r for r in processed_results if not r.get('success', False)]
            if failed_items:
                archive_error_csv = self.generate_error_csv(failed_items, csv_path)
                if archive_error_csv:
                    self.logger.warning(f"アーカイブエラーが発生したファイルがあります: {archive_error_csv}")
                else:
                    self.logger.error("アーカイブエラーCSVの生成に失敗しました")
            else:
                self.logger.info("全てのファイルが正常にアーカイブされました")
                
            self.stats['processed_files'] = len([r for r in processed_results if r.get('success', False)])
            self.stats['failed_files'] = len(failed_items)
            
            self.logger.info("アーカイブ処理完了")
            return 0
            
        except Exception as e:
            self.logger.error(f"アーカイブ処理中にエラーが発生しました: {str(e)}")
            return 1
            
        finally:
            self.stats['end_time'] = datetime.datetime.now()
            self.print_statistics()


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='ファイルアーカイブ処理')
    parser.add_argument('csv_path', help='対象ディレクトリを記載したCSVファイルのパス')
    parser.add_argument('request_id', help='アーカイブ依頼ID')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH, 
                       help=f'設定ファイルのパス (デフォルト: {DEFAULT_CONFIG_PATH})')
    
    args = parser.parse_args()
    
    # CSVファイルの存在チェック
    if not os.path.exists(args.csv_path):
        print(f"CSVファイルが見つかりません: {args.csv_path}")
        sys.exit(1)
        
    # アーカイブ処理の実行
    processor = ArchiveProcessor(args.config)
    exit_code = processor.run(args.csv_path, args.request_id)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()