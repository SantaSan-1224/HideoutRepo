#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アーカイブスクリプト検証版 v3 - エラーハンドリング強化版（出力ファイル修正版）
ログファイル・再試行CSV出力の問題を修正
"""

import argparse
import csv
import datetime
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = "config/archive_config.json"

class ErrorSimulator:
    """エラーシミュレーションクラス v3"""
    
    def __init__(self, simulate_file_lock: float = 0.0, 
                 simulate_permission_error: float = 0.0,
                 simulate_network_error: float = 0.0,
                 simulate_missing_file: float = 0.0):
        self.simulate_file_lock = simulate_file_lock
        self.simulate_permission_error = simulate_permission_error
        self.simulate_network_error = simulate_network_error
        self.simulate_missing_file = simulate_missing_file
        
        # エラー発生統計
        self.error_stats = {
            "file_lock": 0,
            "permission_error": 0,
            "network_error": 0,
            "missing_file": 0
        }
        
        print(f"🧪 エラーシミュレーション設定:")
        print(f"   ファイルロック: {simulate_file_lock*100:.1f}%")
        print(f"   権限エラー: {simulate_permission_error*100:.1f}%")
        print(f"   ネットワークエラー: {simulate_network_error*100:.1f}%")
        print(f"   ファイル消失: {simulate_missing_file*100:.1f}%")
        print()
    
    def should_simulate_error(self, error_type: str, file_path: str) -> bool:
        """エラー発生判定"""
        probability = getattr(self, f"simulate_{error_type}", 0.0)
        
        if probability > 0.0 and random.random() < probability:
            self.error_stats[error_type] += 1
            return True
        
        return False
    
    def simulate_file_lock_error(self, file_path: str) -> Exception:
        """ファイルロックエラーのシミュレーション"""
        error_msg = f"[Errno 13] Permission denied: '{file_path}' (ファイルが他のプロセスで使用中)"
        return PermissionError(error_msg)
    
    def simulate_permission_error(self, file_path: str) -> Exception:
        """権限エラーのシミュレーション"""
        error_msg = f"[Errno 13] Permission denied: '{file_path}' (アクセス権限がありません)"
        return PermissionError(error_msg)
    
    def simulate_network_error(self, file_path: str) -> Exception:
        """ネットワークエラーのシミュレーション"""
        error_messages = [
            "EndpointConnectionError: Could not connect to the endpoint URL",
            "ConnectTimeoutError: Connect timeout on endpoint URL",
            "ReadTimeoutError: Read timeout on endpoint URL",
            "NoCredentialsError: Unable to locate credentials"
        ]
        error_msg = random.choice(error_messages)
        return ConnectionError(error_msg)
    
    def simulate_missing_file_error(self, file_path: str) -> Exception:
        """ファイル消失エラーのシミュレーション"""
        error_msg = f"[Errno 2] No such file or directory: '{file_path}'"
        return FileNotFoundError(error_msg)
    
    def get_error_stats(self) -> Dict:
        """エラー統計取得"""
        total_errors = sum(self.error_stats.values())
        return {
            **self.error_stats,
            "total_simulated_errors": total_errors
        }

class ProgressTrackerV3:
    """進捗追跡クラス v3（エラーシミュレーション統計付き）"""
    
    def __init__(self, total_files: int, total_size: int, error_simulator: ErrorSimulator):
        self.total_files = total_files
        self.total_size = total_size
        self.error_simulator = error_simulator
        self.processed_files = 0
        self.processed_size = 0
        self.success_files = 0
        self.failed_files = 0
        self.start_time = datetime.datetime.now()
        self.current_file = ""
        self.last_update_time = time.time()
        
        # 進捗表示用の統計
        self.upload_times = []
        self.file_sizes = []
        
        # v3 エラー分類（詳細化）
        self.error_counts = {
            "ファイルロック": 0,
            "権限エラー": 0,
            "ネットワークエラー": 0,
            "ファイル不存在": 0,
            "S3エラー": 0,
            "その他エラー": 0
        }
        
        # v3 シミュレーション統計
        self.simulated_error_counts = {
            "file_lock": 0,
            "permission_error": 0,
            "network_error": 0,
            "missing_file": 0
        }
        
        # 性能統計
        self.max_speed = 0.0
        self.file_completion_times = []
        
        print(f"\n{'='*80}")
        print(f"📊 アーカイブ処理開始 (v3 - エラーシミュレーション付き)")
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
        self.success_files += 1
        self.processed_size += file_size
        self.upload_times.append(upload_time)
        self.file_sizes.append(file_size)
        self.file_completion_times.append(time.time())
        
        # 最高速度の更新
        if upload_time > 0:
            current_speed = file_size / upload_time
            if current_speed > self.max_speed:
                self.max_speed = current_speed
        
        self._display_progress("完了")
    
    def update_file_failure(self, file_path: str, file_size: int, error_msg: str, is_simulated: bool = False):
        """ファイル処理失敗時の更新"""
        self.processed_files += 1
        self.failed_files += 1
        
        # エラー分類（詳細化）
        error_type = self._classify_error(error_msg)
        self.error_counts[error_type] += 1
        
        # シミュレーションエラーの統計
        if is_simulated:
            simulated_type = self._classify_simulated_error(error_msg)
            if simulated_type:
                self.simulated_error_counts[simulated_type] += 1
        
        status_prefix = "🧪模擬" if is_simulated else "❌実"
        self._display_progress(f"{status_prefix}: {error_type}")
    
    def _classify_error(self, error_msg: str) -> str:
        """エラーメッセージの分類（v3詳細化）"""
        error_lower = error_msg.lower()
        
        # ファイルロック（他プロセス使用中）
        if any(keyword in error_lower for keyword in ["使用中", "lock", "sharing violation", "being used"]):
            return "ファイルロック"
        
        # 権限エラー
        elif any(keyword in error_lower for keyword in ["permission", "access", "権限", "アクセス"]):
            return "権限エラー"
        
        # ネットワークエラー
        elif any(keyword in error_lower for keyword in ["network", "connection", "timeout", "endpoint", "ネットワーク", "接続"]):
            return "ネットワークエラー"
        
        # ファイル不存在エラー
        elif any(keyword in error_lower for keyword in ["not found", "no such", "見つから", "存在しない"]):
            return "ファイル不存在"
        
        # S3エラー
        elif any(keyword in error_lower for keyword in ["s3", "bucket", "aws", "boto"]):
            return "S3エラー"
        
        # その他
        else:
            return "その他エラー"
    
    def _classify_simulated_error(self, error_msg: str) -> Optional[str]:
        """シミュレーションエラーの分類"""
        error_lower = error_msg.lower()
        
        if "使用中" in error_lower or "lock" in error_lower:
            return "file_lock"
        elif "permission denied" in error_lower and "アクセス権限" in error_lower:
            return "permission_error"
        elif any(keyword in error_lower for keyword in ["endpoint", "timeout", "credentials"]):
            return "network_error"
        elif "no such file" in error_lower:
            return "missing_file"
        
        return None
    
    def _display_progress(self, status: str = ""):
        """進捗表示"""
        # 基本統計計算
        file_progress = (self.processed_files / self.total_files) * 100
        size_progress = (self.processed_size / self.total_size) * 100 if self.total_size > 0 else 0
        
        # 経過時間・推定完了時間計算
        elapsed = datetime.datetime.now() - self.start_time
        elapsed_seconds = elapsed.total_seconds()
        
        if self.processed_files > 0:
            avg_time_per_file = elapsed_seconds / self.processed_files
            remaining_files = self.total_files - self.processed_files
            eta_seconds = avg_time_per_file * remaining_files
            eta = str(datetime.timedelta(seconds=int(eta_seconds)))
        else:
            eta = "計算中"
        
        # プログレスバー生成
        progress_bar = self._create_progress_bar(file_progress)
        
        # コンパクトな性能統計
        avg_speed = self._get_average_speed()
        
        # シミュレーションエラー統計
        sim_errors = sum(self.simulated_error_counts.values())
        
        # 1行でコンパクト表示（v3: シミュレーション統計追加）
        display_line = (f"\r{progress_bar} "
                       f"{self.processed_files}/{self.total_files} "
                       f"({file_progress:.1f}%|{size_progress:.1f}%) "
                       f"ETA:{eta} "
                       f"成功:{self.success_files} 失敗:{self.failed_files} "
                       f"(模擬:{sim_errors}) "
                       f"avg:{self._format_speed(avg_speed)} | "
                       f"{status[:20]:<20}")
        
        print(display_line, end="", flush=True)
    
    def _get_average_speed(self) -> float:
        """平均速度の計算"""
        if not self.upload_times:
            return 0.0
        
        total_time = sum(self.upload_times)
        total_size = sum(self.file_sizes)
        return total_size / total_time if total_time > 0 else 0.0
    
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
    
    def _format_speed(self, bytes_per_second: float) -> str:
        """転送速度フォーマット"""
        return f"{self._format_size(bytes_per_second)}/s"
    
    def print_final_summary(self):
        """最終サマリーの表示"""
        elapsed = datetime.datetime.now() - self.start_time
        total_processed_size = sum(self.file_sizes)
        avg_speed = total_processed_size / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        
        # 進捗表示の下に移動
        print(f"\n\n")
        
        print(f"{'='*80}")
        print(f"📊 アーカイブ処理完了サマリー (v3 - エラーシミュレーション付き)")
        print(f"{'='*80}")
        print(f"⏰ 処理時間: {elapsed}")
        print(f"📁 処理ファイル数: {self.processed_files:,} / {self.total_files:,}")
        print(f"✅ 成功: {self.success_files:,}")
        print(f"❌ 失敗: {self.failed_files:,}")
        print(f"💾 処理済みサイズ: {self._format_size(total_processed_size)}")
        
        # 詳細エラー統計（失敗時のみ）
        if self.failed_files > 0:
            print(f"\n📋 エラー分類詳細:")
            for error_type, count in self.error_counts.items():
                if count > 0:
                    print(f"   {error_type}: {count}件")
            
            # シミュレーションエラー詳細
            total_simulated = sum(self.simulated_error_counts.values())
            if total_simulated > 0:
                print(f"\n🧪 シミュレーションエラー詳細:")
                sim_mapping = {
                    "file_lock": "ファイルロック",
                    "permission_error": "権限エラー",
                    "network_error": "ネットワークエラー",
                    "missing_file": "ファイル消失"
                }
                for sim_type, count in self.simulated_error_counts.items():
                    if count > 0:
                        print(f"   {sim_mapping[sim_type]}: {count}件")
                print(f"   合計: {total_simulated}件")
            
            success_rate = (self.success_files / self.processed_files) * 100
            print(f"📊 成功率: {success_rate:.1f}%")
        else:
            print(f"📊 成功率: 100.0%")
        
        # 性能統計
        print(f"\n📈 性能統計:")
        print(f"   平均速度: {self._format_speed(avg_speed)}")
        print(f"   最高速度: {self._format_speed(self.max_speed)}")
        
        if self.upload_times:
            avg_time_per_file = sum(self.upload_times) / len(self.upload_times)
            max_time = max(self.upload_times)
            min_time = min(self.upload_times)
            print(f"   ファイル別処理時間:")
            print(f"     平均: {avg_time_per_file:.2f}秒")
            print(f"     最大: {max_time:.2f}秒")
            print(f"     最小: {min_time:.2f}秒")
        
        print(f"{'='*80}\n")

class ArchiveProcessorTestV3Fixed:
    """アーカイブ処理クラス（検証版v3修正版: 出力ファイル修正）"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH, error_simulator: ErrorSimulator = None):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
        self.error_simulator = error_simulator or ErrorSimulator()
        self.csv_errors = []
        self.progress_tracker = None
        self.failed_files = []  # 修正: 失敗ファイル記録用
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
        """ログ設定の初期化（修正版: ファイル出力を確実に）"""
        logger = logging.getLogger('archive_processor_test_v3_fixed')
        logger.setLevel(logging.DEBUG)  # 修正: DEBUGレベルに設定
        logger.handlers.clear()
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # コンソール出力（進捗表示と重複しないよう、ERRORレベルのみ）
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # ファイル出力（修正: 確実にログを書き込む）
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            log_file = log_dir / f"archive_test_v3_fixed_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)  # 修正: INFOレベル以上をファイル出力
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            # ログファイルパスを記録
            self.log_file_path = str(log_file)
            
            # 修正: ログ出力テスト
            logger.info("===== ログファイル出力テスト =====")
            logger.info(f"ログファイル: {log_file}")
            logger.info("ログ設定完了")
            
        except Exception as e:
            print(f"ログファイル設定エラー: {e}")
            self.log_file_path = None
        
        return logger
        
    def validate_csv_input(self, csv_path: str) -> Tuple[List[str], List[Dict]]:
        """CSV読み込み・検証処理"""
        self.logger.info(f"CSV読み込み開始: {csv_path}")  # 修正: ログ出力追加
        print(f"📄 CSV読み込み開始: {csv_path}")
        
        valid_directories = []
        self.csv_errors = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            self.logger.info(f"読み込み行数: {len(lines)}")  # 修正: ログ出力追加
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
                    self.logger.debug(f"有効パス追加: {path}")  # 修正: ログ出力追加
                else:
                    error_item = {
                        'line_number': line_num,
                        'path': path,
                        'error_reason': validation_result['error_reason'],
                        'original_line': line.rstrip()
                    }
                    self.csv_errors.append(error_item)
                    self.logger.warning(f"CSV検証エラー 行{line_num}: {validation_result['error_reason']}")  # 修正: ログ出力追加
            
        except Exception as e:
            error_msg = f"CSV読み込みエラー: {str(e)}"
            self.logger.error(error_msg)  # 修正: ログ出力追加
            print(f"❌ {error_msg}")
            return [], []
        
        self.logger.info(f"CSV読み込み完了 - 有効ディレクトリ数: {len(valid_directories)}, エラー項目数: {len(self.csv_errors)}")  # 修正: ログ出力追加
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
        self.logger.info("ファイル収集開始")  # 修正: ログ出力追加
        print(f"📁 ファイル収集開始...")
        
        files = []
        exclude_extensions = self.config.get('file_server', {}).get('exclude_extensions', [])
        max_file_size = self.config.get('processing', {}).get('max_file_size', 10737418240)
        
        # ディレクトリ毎の進捗表示
        for dir_index, directory in enumerate(directories, 1):
            dir_preview = directory[:60] + "..." if len(directory) > 60 else directory
            self.logger.info(f"ディレクトリ処理中 [{dir_index}/{len(directories)}]: {directory}")  # 修正: ログ出力追加
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
                
                self.logger.info(f"ディレクトリ {directory}: {file_count}個のファイルを収集")  # 修正: ログ出力追加
                print(f"   ✅ {file_count}個のファイルを収集")
                        
            except Exception as e:
                error_msg = f"ディレクトリ処理エラー: {str(e)}"
                self.logger.error(f"ディレクトリ {directory}: {error_msg}")  # 修正: ログ出力追加
                print(f"   ❌ {error_msg}")
                continue
        
        total_size = sum(f['size'] for f in files)
        self.logger.info(f"ファイル収集完了 - 総ファイル数: {len(files)}, 総サイズ: {total_size}")  # 修正: ログ出力追加
        print(f"\n📊 ファイル収集完了")
        print(f"   📁 総ファイル数: {len(files):,}")
        print(f"   💾 総ファイルサイズ: {self._format_size(total_size)}")
        
        return files
        
    def archive_to_s3(self, files: List[Dict]) -> List[Dict]:
        """S3アップロード処理（v3エラーシミュレーション付き）"""
        if not files:
            self.logger.warning("アップロード対象ファイルがありません")  # 修正: ログ出力追加
            print("⚠️  アップロード対象ファイルがありません")
            return []
        
        # v3 進捗トラッカー初期化
        total_size = sum(f['size'] for f in files)
        self.progress_tracker = ProgressTrackerV3(len(files), total_size, self.error_simulator)
        
        try:
            s3_client = self._initialize_s3_client()
            bucket_name = self.config['aws']['s3_bucket']
            storage_class = self._validate_storage_class(
                self.config['aws'].get('storage_class', 'STANDARD')
            )
            max_retries = self.config['processing'].get('retry_count', 3)
            
            self.logger.info(f"S3アップロード開始 - バケット: {bucket_name}, ストレージクラス: {storage_class}")  # 修正: ログ出力追加
            
            results = []
            
            for i, file_info in enumerate(files, 1):
                file_path = file_info['path']
                file_size = file_info['size']
                
                # 進捗表示更新（処理開始）
                self.progress_tracker.update_file_start(file_path, file_size)
                
                # v3 新機能: エラーシミュレーション実行
                simulated_error = None
                is_simulated = False
                
                # ファイル消失エラーシミュレーション（ファイルアクセス前）
                if self.error_simulator.should_simulate_error("missing_file", file_path):
                    simulated_error = self.error_simulator.simulate_missing_file_error(file_path)
                    is_simulated = True
                    self.logger.info(f"シミュレーションエラー発生 - ファイル消失: {file_path}")  # 修正: ログ出力追加
                
                # ファイルロックエラーシミュレーション（ファイルアクセス時）
                elif self.error_simulator.should_simulate_error("file_lock", file_path):
                    simulated_error = self.error_simulator.simulate_file_lock_error(file_path)
                    is_simulated = True
                    self.logger.info(f"シミュレーションエラー発生 - ファイルロック: {file_path}")  # 修正: ログ出力追加
                
                # 権限エラーシミュレーション（ファイルアクセス時）
                elif self.error_simulator.should_simulate_error("permission_error", file_path):
                    simulated_error = self.error_simulator.simulate_permission_error(file_path)
                    is_simulated = True
                    self.logger.info(f"シミュレーションエラー発生 - 権限エラー: {file_path}")  # 修正: ログ出力追加
                
                if simulated_error:
                    # シミュレーションエラーが発生した場合
                    upload_time = random.uniform(0.1, 0.5)  # 短い時間でエラー
                    
                    result = {
                        'file_path': file_path,
                        'file_size': file_size,
                        'directory': file_info['directory'],
                        'success': False,
                        'error': str(simulated_error),
                        's3_key': None,
                        'modified_time': file_info['modified_time'],
                        'upload_time': upload_time,
                        'is_simulated_error': True
                    }
                    results.append(result)
                    
                    # 修正: 失敗ファイルリストに追加
                    self.failed_files.append(result)
                    
                    # 進捗表示更新（処理失敗）
                    self.progress_tracker.update_file_failure(
                        file_path, file_size, str(simulated_error), is_simulated=True
                    )
                    continue
                
                # 実際のアップロード処理
                s3_key = self._generate_s3_key(file_path)
                
                # アップロード実行（時間測定付き）
                upload_start_time = time.time()
                upload_result = self._upload_file_with_retry(
                    s3_client, file_path, bucket_name, s3_key, storage_class, max_retries
                )
                upload_time = time.time() - upload_start_time
                
                # ネットワークエラーシミュレーション（アップロード中）
                if (upload_result['success'] and 
                    self.error_simulator.should_simulate_error("network_error", file_path)):
                    
                    simulated_error = self.error_simulator.simulate_network_error(file_path)
                    upload_result = {
                        'success': False,
                        'error': str(simulated_error)
                    }
                    is_simulated = True
                    self.logger.info(f"シミュレーションエラー発生 - ネットワークエラー: {file_path}")  # 修正: ログ出力追加
                
                # 結果記録
                result = {
                    'file_path': file_path,
                    'file_size': file_size,
                    'directory': file_info['directory'],
                    'success': upload_result['success'],
                    'error': upload_result.get('error'),
                    's3_key': s3_key if upload_result['success'] else None,
                    'modified_time': file_info['modified_time'],
                    'upload_time': upload_time,
                    'is_simulated_error': is_simulated
                }
                results.append(result)
                
                # 修正: 失敗ファイルリストに追加
                if not upload_result['success']:
                    self.failed_files.append(result)
                    self.logger.warning(f"アップロード失敗: {file_path} - {upload_result.get('error')}")  # 修正: ログ出力追加
                else:
                    self.logger.debug(f"アップロード成功: {file_path} -> {s3_key}")  # 修正: ログ出力追加
                
                # 進捗表示更新（処理完了）
                if upload_result['success']:
                    self.progress_tracker.update_file_success(file_path, file_size, upload_time)
                else:
                    self.progress_tracker.update_file_failure(
                        file_path, file_size, upload_result.get('error', '不明なエラー'), is_simulated=is_simulated
                    )
            
            # v3 最終サマリー表示
            self.progress_tracker.print_final_summary()
            
            self.logger.info(f"S3アップロード完了 - 成功: {len([r for r in results if r['success']])}, 失敗: {len([r for r in results if not r['success']])}")  # 修正: ログ出力追加
            
            return results
            
        except Exception as e:
            error_msg = f"S3アップロード処理でエラーが発生: {str(e)}"
            self.logger.error(error_msg)  # 修正: ログ出力追加
            print(f"\n❌ {error_msg}")
            return [
                {
                    'file_path': f['path'],
                    'file_size': f['size'],
                    'directory': f['directory'],
                    'success': False,
                    'error': f"S3初期化エラー: {str(e)}",
                    's3_key': None,
                    'modified_time': f['modified_time'],
                    'upload_time': 0,
                    'is_simulated_error': False
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
            
            self.logger.info(f"S3クライアント初期化成功 - バケット: {bucket_name}")  # 修正: ログ出力追加
            
            return s3_client
            
        except ImportError:
            raise Exception("boto3がインストールされていません。pip install boto3 を実行してください。")
        except Exception as e:
            raise Exception(f"S3クライアント初期化失敗: {str(e)}")
    
    def _validate_storage_class(self, storage_class: str) -> str:
        """ストレージクラス検証・調整"""
        if storage_class == 'GLACIER_DEEP_ARCHIVE':
            self.logger.info(f"ストレージクラス変換: {storage_class} -> DEEP_ARCHIVE")  # 修正: ログ出力追加
            return 'DEEP_ARCHIVE'
        
        valid_classes = ['STANDARD', 'STANDARD_IA', 'GLACIER', 'DEEP_ARCHIVE']
        
        if storage_class in valid_classes:
            return storage_class
        
        self.logger.warning(f"無効なストレージクラス '{storage_class}' のため 'STANDARD' に変更")  # 修正: ログ出力追加
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
            self.logger.error(f"S3キー生成エラー: {file_path} - {str(e)}")  # 修正: ログ出力追加
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
                
                self.logger.warning(f"アップロード失敗 (試行 {attempt + 1}/{max_retries}): {error_msg}")  # 修正: ログ出力追加
                time.sleep(2 ** attempt)  # 指数バックオフ
        
        return {'success': False, 'error': '不明なエラー'}
    
    def create_archived_files(self, results: List[Dict]) -> List[Dict]:
        """アーカイブ後処理（空ファイル作成→元ファイル削除）"""
        self.logger.info("アーカイブ後処理開始")  # 修正: ログ出力追加
        print(f"📄 アーカイブ後処理開始")
        
        # 成功したファイルのみ処理
        successful_results = [r for r in results if r.get('success', False)]
        
        if not successful_results:
            self.logger.warning("S3アップロード成功ファイルがないため、アーカイブ後処理をスキップ")  # 修正: ログ出力追加
            print("⚠️  S3アップロード成功ファイルがないため、アーカイブ後処理をスキップ")
            return results
        
        self.logger.info(f"アーカイブ後処理対象: {len(successful_results)}件")  # 修正: ログ出力追加
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
                
                # 完全に空のファイル（0バイト）を作成
                with open(archived_file_path, 'w') as f:
                    pass  # 何も書かない（空ファイル）
                
                # 空ファイル作成確認
                if not os.path.exists(archived_file_path):
                    raise Exception("空ファイルの作成に失敗しました")
                
                # 2. 空ファイル作成成功後に元ファイル削除
                os.remove(file_path)
                
                # 元ファイル削除確認
                if os.path.exists(file_path):
                    raise Exception("元ファイルの削除に失敗しました")
                
                # 成功
                result['archived_file_path'] = archived_file_path
                result['archive_completed'] = True
                self.logger.debug(f"アーカイブ後処理完了: {file_path}")  # 修正: ログ出力追加
                
            except Exception as e:
                # アーカイブ後処理失敗
                error_msg = f"アーカイブ後処理失敗: {str(e)}"
                self.logger.error(f"アーカイブ後処理失敗: {file_path} - {str(e)}")  # 修正: ログ出力追加
                
                # 失敗時のクリーンアップ
                try:
                    # 作成済みの空ファイルがあれば削除
                    if 'archived_file_path' in locals() and os.path.exists(archived_file_path):
                        os.remove(archived_file_path)
                except Exception:
                    pass
                
                # 結果を失敗に変更
                result['success'] = False
                result['error'] = error_msg
                result['archive_completed'] = False
            
            processed_results.append(result)
        
        # 処理結果のサマリー
        completed_count = len([r for r in processed_results if r.get('archive_completed', False)])
        failed_count = len([r for r in processed_results if r.get('success', False) and not r.get('archive_completed', False)])
        
        self.logger.info(f"アーカイブ後処理完了: 完了 {completed_count}件, 失敗 {failed_count}件")  # 修正: ログ出力追加
        print(f"✅ アーカイブ後処理完了: {completed_count}件")
        if failed_count > 0:
            print(f"❌ アーカイブ後処理失敗: {failed_count}件")
        
        return processed_results
        
    def save_to_database(self, results: List[Dict]) -> None:
        """データベース登録処理"""
        self.logger.info("データベース登録開始")  # 修正: ログ出力追加
        print(f"🗄️  データベース登録開始")
        
        # アーカイブ後処理完了ファイルのみ登録
        completed_results = [r for r in results if r.get('archive_completed', False)]
        
        if not completed_results:
            self.logger.warning("データベース登録対象ファイルがありません")  # 修正: ログ出力追加
            print("⚠️  データベース登録対象ファイルがありません")
            return
        
        self.logger.info(f"データベース登録対象: {len(completed_results)}件")  # 修正: ログ出力追加
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
                    self.logger.info(f"データベース挿入完了: {inserted_count}件")  # 修正: ログ出力追加
                    print(f"✅ データベース挿入完了: {inserted_count}件")
            
            self.logger.info("データベース登録完了")  # 修正: ログ出力追加
            print(f"🗄️  データベース登録完了")
            
        except Exception as e:
            error_msg = f"データベース登録エラー: {str(e)}"
            self.logger.error(error_msg)  # 修正: ログ出力追加
            print(f"❌ {error_msg}")
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
            
            # 接続実行
            conn = psycopg2.connect(**conn_params)
            
            # 自動コミットを無効化（トランザクション管理のため）
            conn.autocommit = False
            
            # 接続テスト
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            self.logger.info(f"データベース接続成功: {conn_params['host']}:{conn_params['port']}/{conn_params['database']}")  # 修正: ログ出力追加
            return conn
            
        except ImportError:
            raise Exception("psycopg2がインストールされていません。pip install psycopg2-binary を実行してください。")
        except Exception as e:
            raise Exception(f"データベース接続失敗: {str(e)}")
    
    def generate_error_csv(self, original_csv_path: str) -> Optional[str]:
        """修正: 再試行用CSV生成（失敗ファイル用）"""
        if not self.failed_files:
            self.logger.info("失敗ファイルがないため、再試行CSVをスキップ")  # 修正: ログ出力追加
            return None
            
        self.logger.info("再試行用CSV生成開始")  # 修正: ログ出力追加
        print(f"📄 再試行用CSV生成開始")
        
        try:
            # logsディレクトリにエラーCSVを出力
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            # ファイル名生成
            original_path = Path(original_csv_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            error_csv_path = log_dir / f"{original_path.stem}_retry_{timestamp}.csv"
            
            # 失敗したファイルのディレクトリを収集（重複除去）
            failed_directories = set()
            for failed_file in self.failed_files:
                directory = failed_file.get('directory')
                if directory:
                    failed_directories.add(directory)
            
            # 再試行用CSVの生成
            with open(error_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                
                # ヘッダー行を書き込み
                writer.writerow(['Directory Path'])
                
                # 失敗したディレクトリのみを書き込み
                for directory in sorted(failed_directories):
                    writer.writerow([directory])
            
            self.logger.info(f"再試行用CSV生成完了: {error_csv_path}")  # 修正: ログ出力追加
            self.logger.info(f"再試行対象ディレクトリ数: {len(failed_directories)}")  # 修正: ログ出力追加
            self.logger.info(f"失敗ファイル数: {len(self.failed_files)}")  # 修正: ログ出力追加
            
            print(f"✅ 再試行用CSV生成完了: {error_csv_path}")
            print(f"📊 再試行対象ディレクトリ数: {len(failed_directories)}")
            
            # エラー理由の統計をログに出力
            error_summary = {}
            for item in self.failed_files:
                error_type = item.get('error', '不明なエラー')
                error_summary[error_type] = error_summary.get(error_type, 0) + 1
            
            self.logger.info("エラー理由の内訳:")  # 修正: ログ出力追加
            for error_type, count in error_summary.items():
                self.logger.info(f"  - {error_type}: {count}件")  # 修正: ログ出力追加
            
            return str(error_csv_path)
            
        except Exception as e:
            error_msg = f"再試行用CSV生成失敗: {str(e)}"
            self.logger.error(error_msg)  # 修正: ログ出力追加
            print(f"❌ {error_msg}")
            return None
    
    def _format_size(self, bytes_size: int) -> str:
        """ファイルサイズフォーマット"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} TB"
        
    def run(self, csv_path: str, request_id: str) -> int:
        """メイン処理実行（修正版: 出力ファイル生成確実化）"""
        self.stats['start_time'] = datetime.datetime.now()
        self.request_id = request_id
        
        try:
            self.logger.info(f"===== アーカイブ処理開始 (v3修正版) =====")  # 修正: ログ出力追加
            self.logger.info(f"Request ID: {request_id}")  # 修正: ログ出力追加
            self.logger.info(f"CSV: {csv_path}")  # 修正: ログ出力追加
            
            print(f"🚀 アーカイブ処理開始 (v3修正版) - Request ID: {request_id}")
            print(f"📄 CSV: {csv_path}")
            
            # 1. CSV読み込み・検証
            directories, csv_errors = self.validate_csv_input(csv_path)
            
            if not directories:
                self.logger.error("処理対象のディレクトリが見つかりません")  # 修正: ログ出力追加
                print("❌ 処理対象のディレクトリが見つかりません")
                return 1
                
            # 2. ファイル収集
            files = self.collect_files(directories)
            if not files:
                self.logger.warning("処理対象のファイルが見つかりません")  # 修正: ログ出力追加
                print("⚠️  処理対象のファイルが見つかりません")
                return 0
                
            self.stats['total_files'] = len(files)
            self.stats['total_size'] = sum(f['size'] for f in files)
            
            # 3. S3アップロード（v3エラーシミュレーション付き）
            upload_results = self.archive_to_s3(files)
            
            # 4. アーカイブ後処理（空ファイル作成→元ファイル削除）
            print(f"\n📄 アーカイブ後処理開始...")
            processed_results = self.create_archived_files(upload_results)
            
            # 5. データベース登録
            print(f"🗄️  データベース登録開始...")
            self.save_to_database(processed_results)
            
            # 6. 修正: 再試行用CSV生成（失敗ファイルがある場合）
            if self.failed_files:
                self.logger.info(f"失敗ファイルが {len(self.failed_files)}件 あるため、再試行用CSV生成")  # 修正: ログ出力追加
                retry_csv_path = self.generate_error_csv(csv_path)
                if retry_csv_path:
                    print(f"📄 再試行用CSV生成: {retry_csv_path}")
                else:
                    print(f"❌ 再試行用CSV生成失敗")
            else:
                self.logger.info("失敗ファイルがないため、再試行用CSVはスキップ")  # 修正: ログ出力追加
                print(f"✅ 全ファイル成功のため、再試行用CSVは不要")
            
            # 7. 結果サマリー
            successful_results = [r for r in processed_results if r.get('success', False)]
            failed_results = [r for r in processed_results if not r.get('success', False)]
            simulated_results = [r for r in processed_results if r.get('is_simulated_error', False)]
            
            self.stats['processed_files'] = len(successful_results)
            self.stats['failed_files'] = len(failed_results)
            
            self.logger.info(f"===== アーカイブ処理完了 =====")  # 修正: ログ出力追加
            self.logger.info(f"成功: {len(successful_results)}件")  # 修正: ログ出力追加
            self.logger.info(f"失敗: {len(failed_results)}件")  # 修正: ログ出力追加
            if simulated_results:
                self.logger.info(f"シミュレーション失敗: {len(simulated_results)}件")  # 修正: ログ出力追加
            
            print(f"🎉 アーカイブ処理完了! (v3修正版)")
            print(f"✅ 成功: {len(successful_results)}件")
            print(f"❌ 失敗: {len(failed_results)}件")
            if simulated_results:
                print(f"🧪 シミュレーション失敗: {len(simulated_results)}件")
            
            # エラーシミュレーション統計
            error_stats = self.error_simulator.get_error_stats()
            if error_stats['total_simulated_errors'] > 0:
                self.logger.info("エラーシミュレーション統計:")  # 修正: ログ出力追加
                print(f"\n🧪 エラーシミュレーション統計:")
                for error_type, count in error_stats.items():
                    if error_type != 'total_simulated_errors' and count > 0:
                        self.logger.info(f"   {error_type}: {count}件")  # 修正: ログ出力追加
                        print(f"   {error_type}: {count}件")
                self.logger.info(f"   合計: {error_stats['total_simulated_errors']}件")  # 修正: ログ出力追加
                print(f"   合計: {error_stats['total_simulated_errors']}件")
            
            # 修正: ログファイルパス表示
            if hasattr(self, 'log_file_path') and self.log_file_path:
                print(f"\n📋 ログファイル: {self.log_file_path}")
            
            return 0
            
        except Exception as e:
            error_msg = f"アーカイブ処理中にエラーが発生しました: {str(e)}"
            self.logger.error(error_msg)  # 修正: ログ出力追加
            print(f"\n❌ {error_msg}")
            return 1
            
        finally:
            self.stats['end_time'] = datetime.datetime.now()
            self.logger.info(f"処理終了時刻: {self.stats['end_time']}")  # 修正: ログ出力追加

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='アーカイブスクリプト検証版v3（エラーハンドリング強化版・出力ファイル修正版）')
    parser.add_argument('csv_path', help='対象ディレクトリを記載したCSVファイルのパス')
    parser.add_argument('request_id', help='アーカイブ依頼ID')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH, 
                       help=f'設定ファイルのパス (デフォルト: {DEFAULT_CONFIG_PATH})')
    
    # v3 エラーシミュレーション設定
    parser.add_argument('--simulate-file-lock', type=float, default=0.0, metavar='0.0-1.0',
                       help='ファイルロックエラー発生率 (0.0-1.0, デフォルト: 0.0)')
    parser.add_argument('--simulate-permission-error', type=float, default=0.0, metavar='0.0-1.0',
                       help='権限エラー発生率 (0.0-1.0, デフォルト: 0.0)')
    parser.add_argument('--simulate-network-error', type=float, default=0.0, metavar='0.0-1.0',
                       help='ネットワークエラー発生率 (0.0-1.0, デフォルト: 0.0)')
    parser.add_argument('--simulate-missing-file', type=float, default=0.0, metavar='0.0-1.0',
                       help='ファイル消失エラー発生率 (0.0-1.0, デフォルト: 0.0)')
    
    args = parser.parse_args()
    
    # 引数検証
    for arg_name, value in [
        ('simulate-file-lock', args.simulate_file_lock),
        ('simulate-permission-error', args.simulate_permission_error),
        ('simulate-network-error', args.simulate_network_error),
        ('simulate-missing-file', args.simulate_missing_file)
    ]:
        if not (0.0 <= value <= 1.0):
            print(f"❌ {arg_name} は 0.0-1.0 の範囲で指定してください: {value}")
            sys.exit(1)
    
    # CSVファイルの存在チェック
    if not os.path.exists(args.csv_path):
        print(f"❌ CSVファイルが見つかりません: {args.csv_path}")
        sys.exit(1)
    
    print(f"🔍 アーカイブスクリプト検証版 v3（修正版）")
    print(f"📋 機能: エラーシミュレーション・詳細統計・進捗表示・出力ファイル修正")
    print(f"📄 CSV: {args.csv_path}")
    print(f"🆔 Request ID: {args.request_id}")
    print(f"⚙️  設定ファイル: {args.config}")
    
    # エラーシミュレーター初期化
    error_simulator = ErrorSimulator(
        simulate_file_lock=args.simulate_file_lock,
        simulate_permission_error=args.simulate_permission_error,
        simulate_network_error=args.simulate_network_error,
        simulate_missing_file=args.simulate_missing_file
    )
    
    # アーカイブ処理の実行
    processor = ArchiveProcessorTestV3Fixed(args.config, error_simulator)
    exit_code = processor.run(args.csv_path, args.request_id)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()