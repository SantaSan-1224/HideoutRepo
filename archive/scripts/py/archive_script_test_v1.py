#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アーカイブスクリプト検証版 v1 - 進捗表示機能付き
既存のarchive_script_main.pyに進捗確認機能を追加
"""

import argparse
import csv
import datetime
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = "config/archive_config.json"

class ProgressTracker:
    """進捗追跡クラス"""
    
    def __init__(self, total_files: int, total_size: int):
        self.total_files = total_files
        self.total_size = total_size
        self.processed_files = 0
        self.processed_size = 0
        self.failed_files = 0
        self.start_time = datetime.datetime.now()
        self.current_file = ""
        self.last_update_time = time.time()
        
        # 進捗表示用の統計
        self.upload_times = []  # 各ファイルのアップロード時間
        self.file_sizes = []    # 処理済みファイルサイズ
        
        print(f"\n{'='*80}")
        print(f"📊 アーカイブ処理開始")
        print(f"📁 総ファイル数: {self.total_files:,}")
        print(f"💾 総ファイルサイズ: {self._format_size(self.total_size)}")
        print(f"⏰ 開始時刻: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
    
    def update_file_start(self, file_path: str, file_size: int):
        """ファイル処理開始時の更新"""
        self.current_file = os.path.basename(file_path)
        self.current_file_size = file_size
        self.current_file_start_time = time.time()
        
        # 進捗表示の更新間隔制御（0.5秒に1回）
        current_time = time.time()
        if current_time - self.last_update_time >= 0.5:
            self._display_progress("処理中")
            self.last_update_time = current_time
    
    def update_file_success(self, file_path: str, file_size: int, upload_time: float):
        """ファイル処理成功時の更新"""
        self.processed_files += 1
        self.processed_size += file_size
        self.upload_times.append(upload_time)
        self.file_sizes.append(file_size)
        
        self._display_progress("完了")
    
    def update_file_failure(self, file_path: str, file_size: int, error_msg: str):
        """ファイル処理失敗時の更新"""
        self.processed_files += 1  # 処理済みとしてカウント
        self.failed_files += 1
        
        self._display_progress(f"失敗: {error_msg[:30]}")
    
    def _display_progress(self, status: str = ""):
        """進捗表示"""
        # 基本統計計算
        file_progress = (self.processed_files / self.total_files) * 100
        size_progress = (self.processed_size / self.total_size) * 100 if self.total_size > 0 else 0
        
        # 経過時間・推定完了時間計算
        elapsed = datetime.datetime.now() - self.start_time
        elapsed_seconds = elapsed.total_seconds()
        
        if self.processed_files > 0:
            # ファイル数ベースのETA
            avg_time_per_file = elapsed_seconds / self.processed_files
            remaining_files = self.total_files - self.processed_files
            eta_seconds = avg_time_per_file * remaining_files
            eta = str(datetime.timedelta(seconds=int(eta_seconds)))
            
            # サイズベースのETA（アップロード速度ベース）
            if self.processed_size > 0:
                avg_upload_speed = self.processed_size / elapsed_seconds  # bytes/sec
                remaining_size = self.total_size - self.processed_size
                eta_size_seconds = remaining_size / avg_upload_speed if avg_upload_speed > 0 else 0
                eta_size = str(datetime.timedelta(seconds=int(eta_size_seconds)))
            else:
                eta_size = "計算中"
        else:
            eta = "計算中"
            eta_size = "計算中"
        
        # 現在の処理速度
        upload_speed = self._calculate_current_speed()
        
        # プログレスバー生成
        progress_bar = self._create_progress_bar(file_progress)
        
        # 進捗表示（1行でコンパクトに）
        print(f"\r{progress_bar} "
              f"{self.processed_files}/{self.total_files} "
              f"({file_progress:.1f}% | {size_progress:.1f}%) "
              f"ETA: {eta} | "
              f"速度: {upload_speed} | "
              f"{status[:20]:<20}", end="", flush=True)
    
    def _create_progress_bar(self, percentage: float, width: int = 20) -> str:
        """プログレスバーの生成"""
        filled = int(width * percentage / 100)
        bar = "█" * filled + "░" * (width - filled)
        return f"[{bar}]"
    
    def _calculate_current_speed(self) -> str:
        """現在のアップロード速度を計算"""
        if len(self.upload_times) < 2:
            return "計算中"
        
        # 最近の10ファイルの平均速度を計算
        recent_count = min(10, len(self.upload_times))
        recent_times = self.upload_times[-recent_count:]
        recent_sizes = self.file_sizes[-recent_count:]
        
        total_time = sum(recent_times)
        total_size = sum(recent_sizes)
        
        if total_time > 0:
            speed_bps = total_size / total_time
            return self._format_speed(speed_bps)
        else:
            return "計算中"
    
    def _format_size(self, bytes_size: int) -> str:
        """ファイルサイズフォーマット"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} TB"
    
    def _format_speed(self, bytes_per_second: float) -> str:
        """転送速度フォーマット"""
        return f"{self._format_size(bytes_per_second)}/s"
    
    def print_final_summary(self):
        """最終サマリーの表示"""
        elapsed = datetime.datetime.now() - self.start_time
        total_processed_size = sum(self.file_sizes)
        avg_speed = total_processed_size / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        
        print(f"\n\n{'='*80}")
        print(f"📊 アーカイブ処理完了サマリー")
        print(f"{'='*80}")
        print(f"⏰ 処理時間: {elapsed}")
        print(f"📁 処理ファイル数: {self.processed_files:,} / {self.total_files:,}")
        print(f"✅ 成功: {self.processed_files - self.failed_files:,}")
        print(f"❌ 失敗: {self.failed_files:,}")
        print(f"💾 処理済みサイズ: {self._format_size(total_processed_size)}")
        print(f"📈 平均速度: {self._format_speed(avg_speed)}")
        
        if self.failed_files > 0:
            success_rate = ((self.processed_files - self.failed_files) / self.processed_files) * 100
            print(f"📊 成功率: {success_rate:.1f}%")
        else:
            print(f"📊 成功率: 100.0%")
        
        # 性能統計
        if self.upload_times:
            avg_time_per_file = sum(self.upload_times) / len(self.upload_times)
            max_time = max(self.upload_times)
            min_time = min(self.upload_times)
            print(f"\n📈 ファイル別処理時間統計:")
            print(f"   平均: {avg_time_per_file:.2f}秒")
            print(f"   最大: {max_time:.2f}秒")
            print(f"   最小: {min_time:.2f}秒")
        
        print(f"{'='*80}\n")

class ArchiveProcessorTestV1:
    """アーカイブ処理クラス（検証版v1）"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
        self.csv_errors = []
        self.progress_tracker = None
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
        logger = logging.getLogger('archive_processor_test_v1')
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # コンソール出力（進捗表示と重複しないよう、ERRORレベルのみ）
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # ファイル出力
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            log_file = log_dir / f"archive_test_v1_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
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
                return {'valid': False, 'error_reason': 'ディレクトリではありません（ファイルです）'}
            
            if not os.access(path, os.R_OK):
                return {'valid': False, 'error_reason': '読み取り権限がありません'}
            
            return {'valid': True, 'error_reason': None}
            
        except Exception as e:
            return {'valid': False, 'error_reason': f'パス検証エラー: {str(e)}'}
        
    def collect_files(self, directories: List[str]) -> List[Dict]:
        """ファイル収集処理（進捗表示付き）"""
        print(f"📁 ファイル収集開始...")
        
        files = []
        exclude_extensions = self.config.get('file_server', {}).get('exclude_extensions', [])
        max_file_size = self.config.get('processing', {}).get('max_file_size', 10737418240)
        
        # ディレクトリ毎の進捗表示
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
        
        return files
        
    def archive_to_s3(self, files: List[Dict]) -> List[Dict]:
        """S3アップロード処理（進捗表示付き）"""
        if not files:
            print("⚠️  アップロード対象ファイルがありません")
            return []
        
        # 進捗トラッカー初期化
        total_size = sum(f['size'] for f in files)
        self.progress_tracker = ProgressTracker(len(files), total_size)
        
        try:
            s3_client = self._initialize_s3_client()
            bucket_name = self.config['aws']['s3_bucket']
            storage_class = self._validate_storage_class(
                self.config['aws'].get('storage_class', 'STANDARD')
            )
            max_retries = self.config['processing'].get('retry_count', 3)
            
            results = []
            
            for i, file_info in enumerate(files, 1):
                file_path = file_info['path']
                file_size = file_info['size']
                
                # 進捗表示更新（処理開始）
                self.progress_tracker.update_file_start(file_path, file_size)
                
                s3_key = self._generate_s3_key(file_path)
                
                # アップロード実行（時間測定付き）
                upload_start_time = time.time()
                upload_result = self._upload_file_with_retry(
                    s3_client, file_path, bucket_name, s3_key, storage_class, max_retries
                )
                upload_time = time.time() - upload_start_time
                
                # 結果記録
                result = {
                    'file_path': file_path,
                    'file_size': file_size,
                    'directory': file_info['directory'],
                    'success': upload_result['success'],
                    'error': upload_result.get('error'),
                    's3_key': s3_key if upload_result['success'] else None,
                    'modified_time': file_info['modified_time'],
                    'upload_time': upload_time
                }
                results.append(result)
                
                # 進捗表示更新（処理完了）
                if upload_result['success']:
                    self.progress_tracker.update_file_success(file_path, file_size, upload_time)
                else:
                    self.progress_tracker.update_file_failure(
                        file_path, file_size, upload_result.get('error', '不明なエラー')
                    )
            
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
                    'modified_time': f['modified_time'],
                    'upload_time': 0
                }
                for f in files
            ]
    
    def _initialize_s3_client(self):
        """S3クライアント初期化"""
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
            
            # 接続テスト
            s3_client.head_bucket(Bucket=bucket_name)
            
            return s3_client
            
        except ImportError:
            raise Exception("boto3がインストールされていません。pip install boto3 を実行してください。")
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
            import os
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
        
        # 成功したファイルのみ処理
        successful_results = [r for r in results if r.get('success', False)]
        
        if not successful_results:
            print("⚠️  S3アップロード成功ファイルがないため、アーカイブ後処理をスキップ")
            return results
        
        print(f"📊 アーカイブ後処理対象: {len(successful_results)}件")
        
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
                
                print(f"📄 空ファイル作成: {os.path.basename(archived_file_path)}")
                
                # 完全に空のファイル（0バイト）を作成
                with open(archived_file_path, 'w') as f:
                    pass  # 何も書かない（空ファイル）
                
                # 空ファイル作成確認
                if not os.path.exists(archived_file_path):
                    raise Exception("空ファイルの作成に失敗しました")
                
                # 2. 空ファイル作成成功後に元ファイル削除
                print(f"🗑️  元ファイル削除: {os.path.basename(file_path)}")
                
                os.remove(file_path)
                
                # 元ファイル削除確認
                if os.path.exists(file_path):
                    raise Exception("元ファイルの削除に失敗しました")
                
                # 成功
                result['archived_file_path'] = archived_file_path
                result['archive_completed'] = True
                
            except Exception as e:
                # アーカイブ後処理失敗
                error_msg = f"アーカイブ後処理失敗: {str(e)}"
                print(f"❌ {error_msg}: {os.path.basename(file_path)}")
                
                # 失敗時のクリーンアップ
                try:
                    # 作成済みの空ファイルがあれば削除
                    if 'archived_file_path' in locals() and os.path.exists(archived_file_path):
                        os.remove(archived_file_path)
                        print(f"🧹 作成済み空ファイルを削除: {os.path.basename(archived_file_path)}")
                except Exception as cleanup_error:
                    print(f"⚠️  空ファイルクリーンアップ失敗: {cleanup_error}")
                
                # 結果を失敗に変更
                result['success'] = False
                result['error'] = error_msg
                result['archive_completed'] = False
            
            processed_results.append(result)
        
        # 処理結果のサマリー
        completed_count = len([r for r in processed_results if r.get('archive_completed', False)])
        failed_count = len([r for r in processed_results if r.get('success', False) and not r.get('archive_completed', False)])
        
        print(f"✅ アーカイブ後処理完了: {completed_count}件")
        print(f"❌ アーカイブ後処理失敗: {failed_count}件")
        
        return processed_results
        
    def save_to_database(self, results: List[Dict]) -> None:
        """データベース登録処理"""
        print(f"🗄️  データベース登録開始")
        
        # アーカイブ後処理完了ファイルのみ登録
        completed_results = [r for r in results if r.get('archive_completed', False)]
        
        if not completed_results:
            print("⚠️  データベース登録対象ファイルがありません")
            return
        
        print(f"📊 データベース登録対象: {len(completed_results)}件")
        
        try:
            # データベース接続
            conn = self._connect_database()
            
            # トランザクション開始
            with conn:
                with conn.cursor() as cursor:
                    # 設定から依頼情報を取得
                    request_config = self.config.get('request', {})
                    request_id = self.request_id
                    requester = request_config.get('requester', '00000000')
                    
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
                    print(f"✅ データベース挿入完了: {inserted_count}件")
            
            print(f"🗄️  データベース登録完了")
            
        except Exception as e:
            print(f"❌ データベース登録エラー: {str(e)}")
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
            
            print(f"🔌 データベース接続: {conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
            
            # 接続実行
            conn = psycopg2.connect(**conn_params)
            
            # 自動コミットを無効化（トランザクション管理のため）
            conn.autocommit = False
            
            # 接続テスト
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            print(f"✅ データベース接続成功")
            return conn
            
        except ImportError:
            raise Exception("psycopg2がインストールされていません。pip install psycopg2-binary を実行してください。")
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
        self.stats['start_time'] = datetime.datetime.now()
        self.request_id = request_id
        
        try:
            print(f"🚀 アーカイブ処理開始 - Request ID: {request_id}")
            print(f"📄 CSV: {csv_path}")
            
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
                
            self.stats['total_files'] = len(files)
            self.stats['total_size'] = sum(f['size'] for f in files)
            
            # 3. S3アップロード（進捗表示付き）
            upload_results = self.archive_to_s3(files)
            
            # 4. アーカイブ後処理（空ファイル作成→元ファイル削除）
            print(f"\n📄 アーカイブ後処理開始...")
            processed_results = self.create_archived_files(upload_results)
            
            # 5. データベース登録
            print(f"🗄️  データベース登録開始...")
            self.save_to_database(processed_results)
            
            # 6. 結果サマリー
            successful_results = [r for r in processed_results if r.get('success', False)]
            failed_results = [r for r in processed_results if not r.get('success', False)]
            
            self.stats['processed_files'] = len(successful_results)
            self.stats['failed_files'] = len(failed_results)
            
            print(f"🎉 アーカイブ処理完了!")
            print(f"✅ 成功: {len(successful_results)}件")
            print(f"❌ 失敗: {len(failed_results)}件")
            
            return 0
            
        except Exception as e:
            print(f"\n❌ アーカイブ処理中にエラーが発生しました: {str(e)}")
            return 1
            
        finally:
            self.stats['end_time'] = datetime.datetime.now()

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='アーカイブスクリプト検証版v1（進捗表示機能付き）')
    parser.add_argument('csv_path', help='対象ディレクトリを記載したCSVファイルのパス')
    parser.add_argument('request_id', help='アーカイブ依頼ID')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH, 
                       help=f'設定ファイルのパス (デフォルト: {DEFAULT_CONFIG_PATH})')
    
    args = parser.parse_args()
    
    # CSVファイルの存在チェック
    if not os.path.exists(args.csv_path):
        print(f"❌ CSVファイルが見つかりません: {args.csv_path}")
        sys.exit(1)
    
    print(f"🔍 アーカイブスクリプト検証版 v1")
    print(f"📋 機能: リアルタイム進捗表示")
    print(f"📄 CSV: {args.csv_path}")
    print(f"🆔 Request ID: {args.request_id}")
    print(f"⚙️  設定ファイル: {args.config}")
    
    # アーカイブ処理の実行
    processor = ArchiveProcessorTestV1(args.config)
    exit_code = processor.run(args.csv_path, args.request_id)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()