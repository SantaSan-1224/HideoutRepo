#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アーカイブスクリプト並列処理版 v4 - Minimal（S3アップロード並列化）
v3_fixedをベースに、S3アップロード部分のみ並列処理化したシンプル版
"""

import argparse
import csv
import datetime
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = "config/archive_config.json"

class FlushingFileHandler(logging.FileHandler):
    """ログを即座にフラッシュするFileHandler"""
    
    def emit(self, record):
        super().emit(record)
        self.flush()

class SimpleProgressTracker:
    """シンプルな進捗追跡クラス（v4用）"""
    
    def __init__(self, total_files: int, total_size: int):
        self.total_files = total_files
        self.total_size = total_size
        self.processed_files = 0
        self.success_files = 0
        self.failed_files = 0
        self.processed_size = 0
        self.start_time = datetime.datetime.now()
        self.lock = threading.Lock()
        self.last_update_time = time.time()
        
        print(f"\n{'='*60}")
        print(f"📊 アーカイブ処理開始 (v4 - 並列処理版)")
        print(f"📁 総ファイル数: {self.total_files:,}")
        print(f"💾 総ファイルサイズ: {self._format_size(self.total_size)}")
        print(f"⏰ 開始時刻: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
    
    def update_success(self, file_size: int):
        """成功時の更新（スレッドセーフ）"""
        with self.lock:
            self.processed_files += 1
            self.success_files += 1
            self.processed_size += file_size
            self._display_progress()
    
    def update_failure(self, file_size: int):
        """失敗時の更新（スレッドセーフ）"""
        with self.lock:
            self.processed_files += 1
            self.failed_files += 1
            self._display_progress()
    
    def _display_progress(self):
        """進捗表示（ロック内で呼び出し）"""
        current_time = time.time()
        if current_time - self.last_update_time < 0.5:  # 0.5秒間隔で更新
            return
        
        file_progress = (self.processed_files / self.total_files) * 100
        size_progress = (self.processed_size / self.total_size) * 100 if self.total_size > 0 else 0
        
        # 経過時間・推定完了時間
        elapsed = datetime.datetime.now() - self.start_time
        if self.processed_files > 0:
            avg_time_per_file = elapsed.total_seconds() / self.processed_files
            remaining_files = self.total_files - self.processed_files
            eta_seconds = avg_time_per_file * remaining_files
            eta = str(datetime.timedelta(seconds=int(eta_seconds)))
        else:
            eta = "計算中"
        
        # プログレスバー
        progress_bar = self._create_progress_bar(file_progress)
        
        # 1行表示
        display_line = (f"\r{progress_bar} "
                       f"{self.processed_files}/{self.total_files} "
                       f"({file_progress:.1f}%) "
                       f"ETA:{eta} "
                       f"成功:{self.success_files} 失敗:{self.failed_files}")
        
        print(display_line, end="", flush=True)
        self.last_update_time = current_time
    
    def _create_progress_bar(self, percentage: float, width: int = 20) -> str:
        """プログレスバーの生成"""
        filled = int(width * percentage / 100)
        bar = "█" * filled + "░" * (width - filled)
        return f"[{bar}]"
    
    def _format_size(self, bytes_size: int) -> str:
        """ファイルサイズフォーマット"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} TB"
    
    def print_final_summary(self):
        """最終サマリー表示"""
        elapsed = datetime.datetime.now() - self.start_time
        success_rate = (self.success_files / self.total_files) * 100 if self.total_files > 0 else 0
        
        print(f"\n\n{'='*60}")
        print(f"📊 アーカイブ処理完了サマリー (v4)")
        print(f"{'='*60}")
        print(f"⏰ 処理時間: {elapsed}")
        print(f"📁 処理ファイル数: {self.processed_files:,} / {self.total_files:,}")
        print(f"✅ 成功: {self.success_files:,}")
        print(f"❌ 失敗: {self.failed_files:,}")
        print(f"📊 成功率: {success_rate:.1f}%")
        print(f"💾 処理済みサイズ: {self._format_size(self.processed_size)}")
        print(f"{'='*60}\n")

class ArchiveProcessorV4Minimal:
    """アーカイブ処理クラス（v4並列処理版）"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
        self.csv_errors = []
        self.progress_tracker = None
        
        # 並列処理設定
        self.parallel_workers = self._calculate_parallel_workers()
        
    def _calculate_parallel_workers(self) -> int:
        """並列処理数の自動計算"""
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()
        # CPU数×3、最大20並列
        parallel_workers = min(cpu_count * 3, 20)
        return max(1, parallel_workers)  # 最小1
    
    def set_parallel_workers(self, workers: int):
        """並列処理数の手動設定"""
        self.parallel_workers = max(1, min(workers, 50))  # 1-50の範囲
        
    def load_config(self, config_path: str) -> Dict:
        """設定ファイルを読み込み"""
        default_config = {
            "logging": {
                "log_directory": "logs",
                "log_level": "INFO"
            },
            "file_server": {
                "exclude_extensions": [".tmp", ".lock", ".bak", ".archived"],
                "archived_suffix": "_archived"
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
                
            for key, value in default_config.items():
                if key not in config:
                    config[key] = value
                elif isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        if sub_key not in config[key]:
                            config[key][sub_key] = sub_value
                            
            return config
        except Exception as e:
            print(f"設定ファイル読み込みエラー。デフォルト設定を使用: {e}")
            return default_config
            
    def setup_logger(self) -> logging.Logger:
        """ログ設定の初期化"""
        logger = logging.getLogger('archive_processor_v4')
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # コンソール出力（ERRORレベルのみ）
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # ファイル出力
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            log_file = log_dir / f"archive_v4_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            file_handler = FlushingFileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        except Exception as e:
            print(f"ログファイル設定エラー: {e}")
        
        return logger
    
    def validate_csv_input(self, csv_path: str) -> Tuple[List[str], List[Dict]]:
        """CSV読み込み・検証処理"""
        print(f"📄 CSV読み込み開始: {csv_path}")
        
        valid_directories = []
        self.csv_errors = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            print(f"📊 読み込み行数: {len(lines)}")
            
            for i, line in enumerate(lines):
                line_num = i + 1
                clean_line = line.strip()
                
                if not clean_line:
                    continue
                
                if i == 0 and any(keyword in clean_line.lower() for keyword in ['directory', 'path']):
                    continue
                
                path = clean_line
                validation_result = self._validate_directory_path_with_details(path)
                
                if validation_result['valid']:
                    valid_directories.append(path)
                else:
                    error_item = {
                        'line_number': line_num,
                        'path': path,
                        'error_reason': validation_result['error_reason'],
                        'original_line': line.rstrip()
                    }
                    self.csv_errors.append(error_item)
            
        except Exception as e:
            print(f"❌ CSV読み込みエラー: {str(e)}")
            return [], []
        
        print(f"✅ 有効ディレクトリ数: {len(valid_directories)}")
        if self.csv_errors:
            print(f"⚠️  エラー項目数: {len(self.csv_errors)}")
        
        return valid_directories, self.csv_errors

    def _validate_directory_path_with_details(self, path: str) -> Dict:
        """ディレクトリパスの詳細検証"""
        try:
            if not path or path.strip() == '':
                return {'valid': False, 'error_reason': '空のパス'}
            
            invalid_chars = ['<', '>', ':', '"', '|', '?', '*']
            check_path = path[2:] if path.startswith('\\\\') else path
            for char in invalid_chars:
                if char in check_path:
                    return {'valid': False, 'error_reason': f'不正な文字が含まれています: {char}'}
            
            if len(path) > 260:
                return {'valid': False, 'error_reason': f'パスが長すぎます: {len(path)} > 260'}
            
            if not os.path.exists(path):
                return {'valid': False, 'error_reason': 'ディレクトリが存在しません'}
            
            if not os.path.isdir(path):
                return {'valid': False, 'error_reason': 'ディレクトリではありません'}
            
            if not os.access(path, os.R_OK):
                return {'valid': False, 'error_reason': '読み取り権限がありません'}
            
            return {'valid': True, 'error_reason': None}
            
        except Exception as e:
            return {'valid': False, 'error_reason': f'パス検証エラー: {str(e)}'}
        
    def collect_files(self, directories: List[str]) -> List[Dict]:
        """ファイル収集処理"""
        print(f"📁 ファイル収集開始...")
        
        files = []
        exclude_extensions = self.config.get('file_server', {}).get('exclude_extensions', [])
        max_file_size = self.config.get('processing', {}).get('max_file_size', 10737418240)
        
        for dir_index, directory in enumerate(directories, 1):
            dir_preview = directory[:60] + "..." if len(directory) > 60 else directory
            print(f"📂 [{dir_index}/{len(directories)}] {dir_preview}")
            
            try:
                file_count = 0
                for root, dirs, filenames in os.walk(directory):
                    for filename in filenames:
                        file_path = os.path.join(root, filename)
                        
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
                
                print(f"   ✅ {file_count}個のファイルを収集")
                        
            except Exception as e:
                print(f"   ❌ ディレクトリ処理エラー: {str(e)}")
                continue
        
        total_size = sum(f['size'] for f in files)
        print(f"\n📊 ファイル収集完了")
        print(f"   📁 総ファイル数: {len(files):,}")
        print(f"   💾 総ファイルサイズ: {self._format_size(total_size)}")
        print(f"   🔄 並列処理数: {self.parallel_workers}")
        
        return files
        
    def archive_to_s3_parallel(self, files: List[Dict]) -> List[Dict]:
        """S3アップロード処理（並列処理版）"""
        if not files:
            print("⚠️  アップロード対象ファイルがありません")
            return []
        
        # 進捗トラッカー初期化
        total_size = sum(f['size'] for f in files)
        self.progress_tracker = SimpleProgressTracker(len(files), total_size)
        
        print(f"🔄 並列S3アップロード開始（{self.parallel_workers}並列）")
        
        try:
            # 設定値の取得
            bucket_name = self.config['aws']['s3_bucket']
            storage_class = self._validate_storage_class(
                self.config['aws'].get('storage_class', 'STANDARD')
            )
            max_retries = self.config['processing'].get('retry_count', 3)
            
            results = []
            
            # ThreadPoolExecutorで並列処理
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                # 各ファイルのアップロードタスクを投入
                future_to_file = {}
                
                for file_info in files:
                    future = executor.submit(
                        self._upload_single_file,
                        file_info,
                        bucket_name,
                        storage_class,
                        max_retries
                    )
                    future_to_file[future] = file_info
                
                # 完了したタスクから結果を取得
                for future in as_completed(future_to_file):
                    file_info = future_to_file[future]
                    
                    try:
                        result = future.result()
                        results.append(result)
                        
                        # 進捗更新
                        if result['success']:
                            self.progress_tracker.update_success(file_info['size'])
                        else:
                            self.progress_tracker.update_failure(file_info['size'])
                            
                    except Exception as e:
                        # 予期しないエラー
                        error_result = {
                            'file_path': file_info['path'],
                            'file_size': file_info['size'],
                            'directory': file_info['directory'],
                            'success': False,
                            'error': f"予期しないエラー: {str(e)}",
                            's3_key': None,
                            'modified_time': file_info['modified_time']
                        }
                        results.append(error_result)
                        self.progress_tracker.update_failure(file_info['size'])
                        
                        self.logger.error(f"予期しないエラー: {file_info['path']} - {str(e)}")
            
            # 最終サマリー表示
            self.progress_tracker.print_final_summary()
            
            return results
            
        except Exception as e:
            print(f"\n❌ S3アップロード処理でエラーが発生: {str(e)}")
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
    
    def _upload_single_file(self, file_info: Dict, bucket_name: str, 
                           storage_class: str, max_retries: int) -> Dict:
        """単一ファイルのアップロード処理（スレッド内実行）"""
        file_path = file_info['path']
        file_size = file_info['size']
        
        try:
            # 各スレッドで独立したS3クライアントを作成
            s3_client = self._initialize_s3_client()
            
            # S3キーの生成
            s3_key = self._generate_s3_key(file_path)
            
            # アップロード実行（リトライ付き）
            upload_result = self._upload_file_with_retry(
                s3_client, file_path, bucket_name, s3_key, storage_class, max_retries
            )
            
            # 結果を返却
            result = {
                'file_path': file_path,
                'file_size': file_size,
                'directory': file_info['directory'],
                'success': upload_result['success'],
                'error': upload_result.get('error'),
                's3_key': s3_key if upload_result['success'] else None,
                'modified_time': file_info['modified_time']
            }
            
            # ログ出力
            if upload_result['success']:
                self.logger.info(f"✓ アップロード成功: {s3_key}")
            else:
                self.logger.error(f"✗ アップロード失敗: {file_path} - {upload_result['error']}")
            
            return result
            
        except Exception as e:
            error_msg = f"アップロード処理エラー: {str(e)}"
            self.logger.error(f"✗ {error_msg}: {file_path}")
            
            return {
                'file_path': file_path,
                'file_size': file_size,
                'directory': file_info['directory'],
                'success': False,
                'error': error_msg,
                's3_key': None,
                'modified_time': file_info['modified_time']
            }
    
    def _initialize_s3_client(self):
        """S3クライアント初期化（スレッド毎に独立作成）"""
        try:
            import boto3
            from botocore.config import Config
            
            aws_config = self.config.get('aws', {})
            region = aws_config.get('region', 'ap-northeast-1').strip()
            bucket_name = aws_config.get('s3_bucket', '').strip()
            vpc_endpoint_url = aws_config.get('vpc_endpoint_url', '').strip()
            
            if not bucket_name:
                raise Exception("S3バケット名が設定されていません")
            
            config = Config(
                region_name=region,
                retries={'max_attempts': 3, 'mode': 'adaptive'}
            )
            
            if vpc_endpoint_url:
                s3_client = boto3.client('s3', endpoint_url=vpc_endpoint_url, config=config)
            else:
                s3_client = boto3.client('s3', config=config)
            
            return s3_client
            
        except ImportError:
            raise Exception("boto3がインストールされていません")
        except Exception as e:
            raise Exception(f"S3クライアント初期化失敗: {str(e)}")
    
    def _validate_storage_class(self, storage_class: str) -> str:
        """ストレージクラス検証・調整"""
        if storage_class == 'GLACIER_DEEP_ARCHIVE':
            return 'DEEP_ARCHIVE'
        
        valid_classes = ['STANDARD', 'STANDARD_IA', 'GLACIER', 'DEEP_ARCHIVE']
        
        if storage_class in valid_classes:
            return storage_class
        
        return 'STANDARD'

    def _generate_s3_key(self, file_path: str) -> str:
        """S3キー生成"""
        try:
            normalized_path = file_path.replace('\\', '/')
            
            if normalized_path.startswith('//'):
                parts = normalized_path[2:].split('/')
                if len(parts) > 0:
                    server_name = parts[0]
                    if len(parts) > 1:
                        relative_path = '/'.join(parts[1:])
                        s3_key = f"{server_name}/{relative_path}"
                    else:
                        s3_key = f"{server_name}/root"
                else:
                    s3_key = "unknown_server/unknown_path"
            elif len(normalized_path) > 2 and normalized_path[1] == ':':
                drive_letter = normalized_path[0].lower()
                relative_path = normalized_path[3:]
                s3_key = f"local_{drive_letter}/{relative_path}"
            else:
                s3_key = f"other/{normalized_path}"
            
            s3_key = s3_key.lstrip('/')
            s3_key = '/'.join(part for part in s3_key.split('/') if part)
            
            return s3_key
            
        except Exception as e:
            filename = os.path.basename(file_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            return f"fallback/{timestamp}/{filename}"
    
    def _upload_file_with_retry(self, s3_client, file_path: str, bucket_name: str, 
                               s3_key: str, storage_class: str, max_retries: int) -> Dict:
        """ファイルアップロード（リトライ付き）"""
        
        for attempt in range(max_retries):
            try:
                s3_client.upload_file(
                    file_path,
                    bucket_name,
                    s3_key,
                    ExtraArgs={'StorageClass': storage_class}
                )
                
                return {'success': True, 'error': None}
                
            except FileNotFoundError:
                return {'success': False, 'error': 'ファイルが見つかりません'}
                
            except PermissionError:
                return {'success': False, 'error': 'ファイルアクセス権限がありません'}
                
            except Exception as e:
                error_msg = str(e)
                
                if attempt == max_retries - 1:
                    return {'success': False, 'error': f'最大リトライ回数到達: {error_msg}'}
                
                time.sleep(2 ** attempt)  # 指数バックオフ
        
        return {'success': False, 'error': '不明なエラー'}
    
    def create_archived_files(self, results: List[Dict]) -> List[Dict]:
        """アーカイブ後処理（空ファイル作成→元ファイル削除）"""
        print(f"📄 アーカイブ後処理開始")
        
        successful_results = [r for r in results if r.get('success', False)]
        
        if not successful_results:
            print("⚠️  S3アップロード成功ファイルがないため、アーカイブ後処理をスキップ")
            return results
        
        print(f"📊 アーカイブ後処理対象: {len(successful_results)}件")
        
        archived_suffix = self.config.get('file_server', {}).get('archived_suffix', '_archived')
        processed_results = []
        
        for result in results:
            if not result.get('success', False):
                processed_results.append(result)
                continue
            
            file_path = result['file_path']
            
            try:
                # 1. 空ファイル作成
                archived_file_path = f"{file_path}{archived_suffix}"
                
                with open(archived_file_path, 'w') as f:
                    pass  # 空ファイル作成
                
                if not os.path.exists(archived_file_path):
                    raise Exception("空ファイルの作成に失敗しました")
                
                # 2. 元ファイル削除
                os.remove(file_path)
                
                if os.path.exists(file_path):
                    raise Exception("元ファイルの削除に失敗しました")
                
                # 成功
                result['archived_file_path'] = archived_file_path
                result['archive_completed'] = True
                
            except Exception as e:
                error_msg = f"アーカイブ後処理失敗: {str(e)}"
                
                # 失敗時のクリーンアップ
                try:
                    if 'archived_file_path' in locals() and os.path.exists(archived_file_path):
                        os.remove(archived_file_path)
                except Exception:
                    pass
                
                result['success'] = False
                result['error'] = error_msg
                result['archive_completed'] = False
            
            processed_results.append(result)
        
        completed_count = len([r for r in processed_results if r.get('archive_completed', False)])
        failed_count = len([r for r in processed_results if r.get('success', False) and not r.get('archive_completed', False)])
        
        print(f"✅ アーカイブ後処理完了: {completed_count}件")
        print(f"❌ アーカイブ後処理失敗: {failed_count}件")
        
        return processed_results
        
    def save_to_database(self, results: List[Dict]) -> None:
        """データベース登録処理"""
        print(f"🗄️  データベース登録開始")
        
        completed_results = [r for r in results if r.get('archive_completed', False)]
        
        if not completed_results:
            print("⚠️  データベース登録対象ファイルがありません")
            return
        
        print(f"📊 データベース登録対象: {len(completed_results)}件")
        
        try:
            conn = self._connect_database()
            
            with conn:
                with conn.cursor() as cursor:
                    request_config = self.config.get('request', {})
                    request_id = self.request_id
                    requester = request_config.get('requester', '00000000')
                    current_time = datetime.datetime.now()
                    bucket_name = self.config.get('aws', {}).get('s3_bucket', '')
                    
                    insert_data = []
                    for result in completed_results:
                        s3_key = result.get('s3_key', '')
                        s3_url = f"s3://{bucket_name}/{s3_key}" if s3_key else ''
                        
                        record = (
                            request_id,
                            requester,
                            current_time,
                            result['file_path'],
                            s3_url,
                            current_time,
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
                    inserted_count = cursor.rowcount
                    print(f"✅ データベース挿入完了: {inserted_count}件")
            
            print(f"🗄️  データベース登録完了")
            
        except Exception as e:
            print(f"❌ データベース登録エラー: {str(e)}")
            
        finally:
            try:
                if 'conn' in locals():
                    conn.close()
            except Exception:
                pass
    
    def _connect_database(self):
        """データベース接続"""
        try:
            import psycopg2

            db_config = self.config.get('database', {})
            
            conn_params = {
                'host': db_config.get('host', 'localhost'),
                'port': db_config.get('port', 5432),
                'database': db_config.get('database', 'archive_system'),
                'user': db_config.get('user', 'postgres'),
                'password': db_config.get('password', ''),
                'connect_timeout': db_config.get('timeout', 30)
            }
            
            print(f"🔌 データベース接続: {conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
            
            conn = psycopg2.connect(**conn_params)
            conn.autocommit = False
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            print(f"✅ データベース接続成功")
            return conn
            
        except ImportError:
            raise Exception("psycopg2がインストールされていません")
        except Exception as e:
            raise Exception(f"データベース接続失敗: {str(e)}")
        
    def _format_size(self, bytes_size: int) -> str:
        """ファイルサイズフォーマット"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} TB"
        
    def run(self, csv_path: str, request_id: str) -> int:
        """メイン処理実行"""
        try:
            print(f"🚀 アーカイブ処理開始 (v4並列版) - Request ID: {request_id}")
            print(f"📄 CSV: {csv_path}")
            print(f"🔄 並列処理数: {self.parallel_workers}")
            
            # request_idを保存
            self.request_id = request_id
            
            # 1. CSV読み込み・検証
            directories, csv_errors = self.validate_csv_input(csv_path)
            
            if not directories:
                print("❌ 処理対象のディレクトリが見つかりません")
                return 1
                
            # 2. ファイル収集
            files = self.collect_files(directories)
            if not files:
                print("⚠️  処理対象のファイルが見つかりません")
                return 0
                
            # 3. S3アップロード（並列処理）
            upload_results = self.archive_to_s3_parallel(files)
            
            # 4. アーカイブ後処理
            print(f"\n📄 アーカイブ後処理開始...")
            processed_results = self.create_archived_files(upload_results)
            
            # 5. データベース登録
            print(f"🗄️  データベース登録開始...")
            self.save_to_database(processed_results)
            
            # 6. 結果サマリー
            successful_results = [r for r in processed_results if r.get('success', False)]
            failed_results = [r for r in processed_results if not r.get('success', False)]
            
            print(f"🎉 アーカイブ処理完了! (v4並列版)")
            print(f"✅ 成功: {len(successful_results)}件")
            print(f"❌ 失敗: {len(failed_results)}件")
            print(f"🔄 並列処理数: {self.parallel_workers}")
            
            return 0
            
        except Exception as e:
            print(f"\n❌ アーカイブ処理中にエラーが発生しました: {str(e)}")
            return 1

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='アーカイブスクリプト並列処理版v4（S3アップロード並列化）')
    parser.add_argument('csv_path', help='対象ディレクトリを記載したCSVファイルのパス')
    parser.add_argument('request_id', help='アーカイブ依頼ID')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH, 
                       help=f'設定ファイルのパス (デフォルト: {DEFAULT_CONFIG_PATH})')
    
    # v4新機能: 並列処理数指定
    parser.add_argument('--parallel', type=int, metavar='NUM',
                       help='並列処理数を指定 (指定しない場合は自動設定: CPU数×3, 最大20)')
    
    args = parser.parse_args()
    
    # CSVファイルの存在チェック
    if not os.path.exists(args.csv_path):
        print(f"❌ CSVファイルが見つかりません: {args.csv_path}")
        sys.exit(1)
    
    print(f"🔍 アーカイブスクリプト並列処理版 v4")
    print(f"📋 機能: S3アップロード並列処理・シンプル設計")
    print(f"📄 CSV: {args.csv_path}")
    print(f"🆔 Request ID: {args.request_id}")
    print(f"⚙️  設定ファイル: {args.config}")
    
    # アーカイブ処理の実行
    processor = ArchiveProcessorV4Minimal(args.config)
    
    # 並列処理数の設定
    if args.parallel:
        processor.set_parallel_workers(args.parallel)
        print(f"🔄 並列処理数（手動設定）: {processor.parallel_workers}")
    else:
        print(f"🔄 並列処理数（自動設定）: {processor.parallel_workers}")
    
    exit_code = processor.run(args.csv_path, args.request_id)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()