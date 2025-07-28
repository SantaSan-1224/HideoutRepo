#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アーカイブスクリプト検証版 v4 - S3アップロード並列処理版
v3にS3アップロードの並列処理機能を追加
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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import multiprocessing

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = "config/archive_config.json"

class FlushingFileHandler(logging.FileHandler):
    """毎回強制的にログをフラッシュするFileHandler"""
    
    def emit(self, record):
        super().emit(record)
        if hasattr(self, 'stream') and self.stream:
            self.stream.flush()
            # さらに確実にするためOS レベルでもフラッシュ
            try:
                if hasattr(self.stream, 'fileno'):
                    os.fsync(self.stream.fileno())
            except (OSError, AttributeError):
                pass

class ErrorSimulator:
    """エラーシミュレーション機能"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.simulation_enabled = False
        self.error_rates = {
            'file_lock': 0.0,      # ファイルロックエラー率
            'permission': 0.0,     # 権限エラー率
            'network': 0.0,        # ネットワークエラー率
            'file_missing': 0.0,   # ファイル消失エラー率
            's3_invalid': 0.0,     # S3エラー率
        }
        self.simulated_errors = []
        self._lock = threading.Lock()  # v4追加: スレッドセーフ
        
    def enable_simulation(self, error_type: str, error_rate: float):
        """エラーシミュレーションを有効化"""
        if error_type in self.error_rates and 0.0 <= error_rate <= 1.0:
            self.error_rates[error_type] = error_rate
            self.simulation_enabled = True
            print(f"🧪 エラーシミュレーション有効: {error_type} = {error_rate*100:.1f}%")
    
    def should_simulate_error(self, error_type: str) -> bool:
        """エラーを発生させるかどうかの判定（スレッドセーフ）"""
        if not self.simulation_enabled:
            return False
        
        rate = self.error_rates.get(error_type, 0.0)
        if rate > 0.0 and random.random() < rate:
            with self._lock:
                self.simulated_errors.append({
                    'type': error_type,
                    'timestamp': datetime.datetime.now().isoformat(),
                    'thread_id': threading.current_thread().ident
                })
            return True
        return False
    
    def get_simulation_stats(self) -> Dict:
        """シミュレーション統計を取得"""
        with self._lock:
            stats = {'total_simulated': len(self.simulated_errors)}
            for error_type in self.error_rates:
                stats[f'{error_type}_count'] = len([e for e in self.simulated_errors if e['type'] == error_type])
            return stats

class ProgressTrackerV4:
    """進捗追跡クラス v4（並列処理対応）"""
    
    def __init__(self, total_files: int, total_size: int):
        self.total_files = total_files
        self.total_size = total_size
        self.processed_files = 0
        self.processed_size = 0
        self.success_files = 0
        self.failed_files = 0
        self.start_time = datetime.datetime.now()
        self.current_file = ""
        self.last_update_time = time.time()
        
        # v4追加: スレッドセーフティ用Lock
        self._lock = threading.Lock()
        
        # 進捗表示用の統計
        self.upload_times = []
        self.file_sizes = []
        
        # v4 新機能: 並列処理統計
        self.concurrent_uploads = 0
        self.max_concurrent_uploads = 0
        self.thread_stats = {}  # スレッド毎の統計
        
        # v3 機能: 詳細エラー分類（シミュレーション対応）
        self.error_counts = {
            "権限エラー": 0,
            "ネットワークエラー": 0,
            "ファイル不存在": 0,
            "ファイルロック": 0,     
            "S3エラー": 0,           
            "シミュレーション": 0,   
            "その他エラー": 0
        }
        
        # エラー詳細記録
        self.error_details = []
        
        # 性能統計
        self.max_speed = 0.0
        self.file_completion_times = []
        
        print(f"\n{'='*80}")
        print(f"📊 アーカイブ処理開始 (v4 - 並列処理対応)")
        print(f"📁 総ファイル数: {self.total_files:,}")
        print(f"💾 総ファイルサイズ: {self._format_size(self.total_size)}")
        print(f"⏰ 開始時刻: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
    
    def update_file_start(self, file_path: str, file_size: int):
        """ファイル処理開始時の更新（スレッドセーフ）"""
        with self._lock:
            self.concurrent_uploads += 1
            if self.concurrent_uploads > self.max_concurrent_uploads:
                self.max_concurrent_uploads = self.concurrent_uploads
            
            thread_id = threading.current_thread().ident
            if thread_id not in self.thread_stats:
                self.thread_stats[thread_id] = {'files': 0, 'bytes': 0, 'errors': 0}
            
            self.current_file = os.path.basename(file_path)
            self.current_file_size = file_size
            self.current_file_start_time = time.time()
            
            # 進捗表示の更新間隔制御（0.5秒に1回）
            current_time = time.time()
            if current_time - self.last_update_time >= 0.5:
                self._display_progress("処理中")
                self.last_update_time = current_time
    
    def update_file_success(self, file_path: str, file_size: int, upload_time: float):
        """ファイル処理成功時の更新（スレッドセーフ）"""
        with self._lock:
            self.processed_files += 1
            self.success_files += 1
            self.processed_size += file_size
            self.upload_times.append(upload_time)
            self.file_sizes.append(file_size)
            self.file_completion_times.append(time.time())
            self.concurrent_uploads -= 1
            
            # スレッド統計更新
            thread_id = threading.current_thread().ident
            if thread_id in self.thread_stats:
                self.thread_stats[thread_id]['files'] += 1
                self.thread_stats[thread_id]['bytes'] += file_size
            
            # 最高速度の更新
            if upload_time > 0:
                current_speed = file_size / upload_time
                if current_speed > self.max_speed:
                    self.max_speed = current_speed
            
            self._display_progress("完了")
    
    def update_file_failure(self, file_path: str, file_size: int, error_msg: str, 
                           is_simulated: bool = False):
        """ファイル処理失敗時の更新（スレッドセーフ）"""
        with self._lock:
            self.processed_files += 1
            self.failed_files += 1
            self.concurrent_uploads -= 1
            
            # スレッド統計更新
            thread_id = threading.current_thread().ident
            if thread_id in self.thread_stats:
                self.thread_stats[thread_id]['errors'] += 1
            
            # エラー分類
            error_type = self._classify_error_v3(error_msg, is_simulated)
            self.error_counts[error_type] += 1
            
            # エラー詳細記録
            error_detail = {
                'file_path': os.path.basename(file_path),
                'error_type': error_type,
                'error_message': error_msg,
                'is_simulated': is_simulated,
                'thread_id': thread_id,
                'timestamp': datetime.datetime.now().isoformat()
            }
            self.error_details.append(error_detail)
            
            status_display = f"失敗: {error_type}"
            if is_simulated:
                status_display += " (シミュ)"
            
            self._display_progress(status_display)
    
    def _classify_error_v3(self, error_msg: str, is_simulated: bool) -> str:
        """エラーメッセージの詳細分類"""
        if is_simulated:
            return "シミュレーション"
        
        error_lower = error_msg.lower()
        
        if any(keyword in error_lower for keyword in 
               ["sharing violation", "file is being used", "lock", "ロック", "使用中"]):
            return "ファイルロック"
        elif any(keyword in error_lower for keyword in 
                ["s3", "bucket", "aws", "botocore", "endpoint"]):
            return "S3エラー"
        elif any(keyword in error_lower for keyword in 
                ["permission", "access", "権限", "アクセス"]):
            return "権限エラー"
        elif any(keyword in error_lower for keyword in 
                ["network", "connection", "timeout", "ネットワーク", "接続"]):
            return "ネットワークエラー"
        elif any(keyword in error_lower for keyword in 
                ["not found", "no such", "見つから", "存在しない"]):
            return "ファイル不存在"
        else:
            return "その他エラー"
    
    def _display_progress(self, status: str = ""):
        """進捗表示（v4版: 並列処理情報追加）"""
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
        
        # v4: 並列処理情報を含めた表示
        concurrent_info = f"[{self.concurrent_uploads}並列]" if self.concurrent_uploads > 0 else ""
        sim_count = self.error_counts.get("シミュレーション", 0)
        sim_display = f" sim:{sim_count}" if sim_count > 0 else ""
        
        # 1行でコンパクト表示（v4版）
        display_line = (f"\r{progress_bar} "
                       f"{self.processed_files}/{self.total_files} "
                       f"({file_progress:.1f}%|{size_progress:.1f}%) "
                       f"ETA:{eta} {concurrent_info} "
                       f"成功:{self.success_files} 失敗:{self.failed_files}{sim_display} "
                       f"avg:{self._format_speed(avg_speed)} | "
                       f"{status[:15]:<15}")
        
        print(display_line, end="", flush=True)
    
    def _get_average_speed(self) -> float:
        """平均速度の計算"""
        if not self.upload_times:
            return 0.0
        
        total_time = sum(self.upload_times)
        total_size = sum(self.file_sizes)
        return total_size / total_time if total_time > 0 else 0.0
    
    def _calculate_throughput(self) -> float:
        """スループット計算（ファイル数/分）"""
        if len(self.file_completion_times) < 2:
            return 0.0
        
        current_time = time.time()
        one_minute_ago = current_time - 60
        
        recent_completions = [t for t in self.file_completion_times if t >= one_minute_ago]
        
        if len(recent_completions) > 0:
            time_span = current_time - max(one_minute_ago, min(recent_completions))
            if time_span > 0:
                return (len(recent_completions) / time_span) * 60
        
        elapsed_minutes = (current_time - self.start_time.timestamp()) / 60
        if elapsed_minutes > 0:
            return len(self.file_completion_times) / elapsed_minutes
        
        return 0.0
    
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
    
    def print_final_summary(self, error_simulator: ErrorSimulator = None, parallel_workers: int = 1):
        """最終サマリーの表示（v4拡張版: 並列処理統計）"""
        elapsed = datetime.datetime.now() - self.start_time
        total_processed_size = sum(self.file_sizes)
        avg_speed = total_processed_size / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        
        print(f"\n\n")
        
        print(f"{'='*80}")
        print(f"📊 アーカイブ処理完了サマリー (v4 - 並列処理対応)")
        print(f"{'='*80}")
        print(f"⏰ 処理時間: {elapsed}")
        print(f"📁 処理ファイル数: {self.processed_files:,} / {self.total_files:,}")
        print(f"✅ 成功: {self.success_files:,}")
        print(f"❌ 失敗: {self.failed_files:,}")
        print(f"💾 処理済みサイズ: {self._format_size(total_processed_size)}")
        
        # v4 新機能: 並列処理統計
        print(f"\n🔄 並列処理統計:")
        print(f"   設定並列度: {parallel_workers}")
        print(f"   最大同時実行数: {self.max_concurrent_uploads}")
        print(f"   アクティブスレッド数: {len(self.thread_stats)}")
        
        # スレッド別統計
        if len(self.thread_stats) > 1:
            print(f"   スレッド別統計:")
            for thread_id, stats in self.thread_stats.items():
                print(f"     Thread-{thread_id}: {stats['files']}ファイル, {self._format_size(stats['bytes'])}, {stats['errors']}エラー")
        
        # エラー統計
        if self.failed_files > 0:
            print(f"\n📋 エラー分類詳細:")
            for error_type, count in self.error_counts.items():
                if count > 0:
                    percentage = (count / self.failed_files) * 100
                    icon = "🧪" if error_type == "シミュレーション" else "❌"
                    print(f"   {icon} {error_type}: {count}件 ({percentage:.1f}%)")
            
            success_rate = (self.success_files / self.processed_files) * 100
            print(f"📊 成功率: {success_rate:.1f}%")
        else:
            print(f"📊 成功率: 100.0%")
        
        # シミュレーション統計
        if error_simulator and error_simulator.simulation_enabled:
            sim_stats = error_simulator.get_simulation_stats()
            print(f"\n🧪 エラーシミュレーション統計:")
            print(f"   総シミュレーション回数: {sim_stats['total_simulated']}回")
            for error_type, rate in error_simulator.error_rates.items():
                if rate > 0:
                    actual_count = sim_stats.get(f'{error_type}_count', 0)
                    print(f"   {error_type}: 設定{rate*100:.1f}% → 実際{actual_count}回")
        
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
        
        final_throughput = self._calculate_throughput()
        print(f"   最終スループット: {final_throughput:.1f}ファイル/分")
        
        # v4: 並列処理効果の推定
        if parallel_workers > 1:
            sequential_estimate = elapsed.total_seconds() * parallel_workers
            improvement_ratio = sequential_estimate / elapsed.total_seconds()
            print(f"   並列処理効果: 約{improvement_ratio:.1f}倍高速化 (推定)")
        
        print(f"{'='*80}\n")

class ArchiveProcessorTestV4:
    """アーカイブ処理クラス（検証版v4 - 並列処理対応）"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
        self.csv_errors = []
        self.progress_tracker = None
        self.error_simulator = ErrorSimulator(self.config)
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
                "exclude_extensions": [".tmp", ".lock", ".bak", ".archived"],
                "archived_suffix": ".archived"
            },
            "processing": {
                "max_file_size": 10737418240,
                "chunk_size": 8388608,
                "retry_count": 3,
                "parallel_workers": 0  # v4追加: 0=自動設定
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
        logger = logging.getLogger('archive_processor_test_v4')
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(thread)d] - %(message)s')
        
        # コンソール出力
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # ファイル出力
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = log_dir / f"archive_test_v4_{timestamp}.log"
            
            file_handler = FlushingFileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            logger.info("=" * 60)
            logger.info("アーカイブスクリプト v4 (並列処理版) ログ開始")
            logger.info(f"ログファイル: {log_file}")
            logger.info(f"開始時刻: {datetime.datetime.now()}")
            logger.info("=" * 60)
            
            print(f"📝 ログファイル作成: {log_file}")
            
        except Exception as e:
            print(f"ログファイル設定エラー: {e}")
        
        return logger
    
    def enable_error_simulation(self, sim_args: Dict):
        """エラーシミュレーションの設定"""
        for error_type, rate in sim_args.items():
            if rate > 0:
                self.error_simulator.enable_simulation(error_type, rate)
    
    def determine_parallel_workers(self, override_workers: Optional[int] = None) -> int:
        """並列度の決定（v4新機能）"""
        if override_workers is not None and override_workers > 0:
            workers = override_workers
            source = "コマンドライン指定"
        else:
            config_workers = self.config.get('processing', {}).get('parallel_workers', 0)
            if config_workers > 0:
                workers = config_workers
                source = "設定ファイル指定"
            else:
                # 自動設定: CPU数 × 3 (I/Oバウンドなため)
                cpu_count = multiprocessing.cpu_count()
                workers = min(cpu_count * 3, 20)  # 最大20並列
                source = f"自動設定 (CPU数{cpu_count} × 3)"
        
        self.logger.info(f"並列度決定: {workers} ({source})")
        print(f"🔄 並列度: {workers} ({source})")
        return workers
        
    def validate_csv_input(self, csv_path: str) -> Tuple[List[str], List[Dict]]:
        """CSV読み込み・検証処理"""
        self.logger.info(f"CSV読み込み開始: {csv_path}")
        print(f"📄 CSV読み込み開始: {csv_path}")
        
        valid_directories = []
        self.csv_errors = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            self.logger.info(f"読み込み行数: {len(lines)}")
            print(f"📊 読み込み行数: {len(lines)}")
            
            for i, line in enumerate(lines):
                line_num = i + 1
                clean_line = line.strip()
                
                if not clean_line:
                    continue
                
                if i == 0 and any(keyword in clean_line.lower() for keyword in ['directory', 'path']):
                    self.logger.info(f"行 {line_num}: ヘッダー行をスキップ")
                    continue
                
                path = clean_line
                validation_result = self._validate_directory_path_with_details(path)
                
                if validation_result['valid']:
                    valid_directories.append(path)
                    self.logger.info(f"行 {line_num}: 有効なパス追加 - {path[:50]}...")
                else:
                    error_item = {
                        'line_number': line_num,
                        'path': path,
                        'error_reason': validation_result['error_reason'],
                        'original_line': line.rstrip()
                    }
                    self.csv_errors.append(error_item)
                    self.logger.error(f"行 {line_num}: {validation_result['error_reason']} - {path[:50]}...")
            
        except Exception as e:
            error_msg = f"CSV読み込みエラー: {str(e)}"
            self.logger.error(error_msg)
            print(f"❌ {error_msg}")
            return [], []
        
        self.logger.info(f"CSV読み込み完了 - 有効: {len(valid_directories)}, エラー: {len(self.csv_errors)}")
        print(f"✅ 有効ディレクトリ数: {len(valid_directories)}")
        if self.csv_errors:
            print(f"⚠️  エラー項目数: {len(self.csv_errors)}")
        
        return valid_directories, self.csv_errors

    def _validate_directory_path_with_details(self, path: str) -> Dict:
        """ディレクトリパスの詳細検証"""
        try:
            if not path or path.strip() == '':
                return {'valid': False, 'error_reason': f'パス検証エラー: {str(e)}'}
        
    def _is_archived_file(self, filename: str, archived_suffix: str) -> bool:
        """アーカイブ済みファイル判定"""
        try:
            if filename.endswith(archived_suffix):
                self.logger.debug(f"アーカイブ済みファイルをスキップ: {filename}")
                return True
            return False
        except Exception as e:
            self.logger.warning(f"アーカイブ済みファイル判定エラー: {filename} - {str(e)}")
            return False
    
    def collect_files(self, directories: List[str]) -> List[Dict]:
        """ファイル収集処理"""
        self.logger.info("ファイル収集開始")
        print(f"📁 ファイル収集開始...")
        
        files = []
        exclude_extensions = self.config.get('file_server', {}).get('exclude_extensions', [])
        max_file_size = self.config.get('processing', {}).get('max_file_size', 10737418240)
        archived_suffix = self.config.get('file_server', {}).get('archived_suffix', '.archived')
        
        self.logger.info(f"除外拡張子: {exclude_extensions}")
        self.logger.info(f"アーカイブ済みサフィックス: {archived_suffix}")
        
        for dir_index, directory in enumerate(directories, 1):
            dir_preview = directory[:60] + "..." if len(directory) > 60 else directory
            self.logger.info(f"ディレクトリ処理開始: [{dir_index}/{len(directories)}] {directory}")
            print(f"📂 [{dir_index}/{len(directories)}] {dir_preview}")
            
            try:
                file_count = 0
                archived_count = 0
                for root, dirs, filenames in os.walk(directory):
                    for filename in filenames:
                        file_path = os.path.join(root, filename)
                        
                        # アーカイブ済みファイルの除外
                        if self._is_archived_file(filename, archived_suffix):
                            archived_count += 1
                            continue
                        
                        _, ext = os.path.splitext(filename)
                        if ext.lower() in exclude_extensions:
                            continue
                        
                        try:
                            stat_info = os.stat(file_path)
                            file_size = stat_info.st_size
                            
                            if file_size > max_file_size:
                                self.logger.debug(f"サイズ超過でスキップ: {file_path} ({file_size} > {max_file_size})")
                                continue
                            
                            file_info = {
                                'path': file_path,
                                'size': file_size,
                                'modified_time': datetime.datetime.fromtimestamp(stat_info.st_mtime),
                                'directory': directory
                            }
                            
                            files.append(file_info)
                            file_count += 1
                            
                        except OSError as e:
                            self.logger.warning(f"ファイル情報取得エラー: {file_path} - {str(e)}")
                            continue
                
                self.logger.info(f"ディレクトリ {directory}: {file_count}個のファイルを収集, {archived_count}個のアーカイブ済みファイルをスキップ")
                print(f"   ✅ {file_count}個のファイルを収集")
                if archived_count > 0:
                    print(f"   📄 {archived_count}個のアーカイブ済みファイルをスキップ")
                        
            except Exception as e:
                error_msg = f"ディレクトリ処理エラー: {directory} - {str(e)}"
                self.logger.error(error_msg)
                print(f"   ❌ {error_msg}")
                continue
        
        total_size = sum(f['size'] for f in files)
        self.logger.info(f"ファイル収集完了 - 総ファイル数: {len(files)}, 総サイズ: {total_size}")
        print(f"\n📊 ファイル収集完了")
        print(f"   📁 総ファイル数: {len(files):,}")
        print(f"   💾 総ファイルサイズ: {self._format_size(total_size)}")
        
        return files
        
    def archive_to_s3(self, files: List[Dict], parallel_workers: int = 1) -> List[Dict]:
        """S3アップロード処理（v4並列処理対応）"""
        if not files:
            self.logger.warning("アップロード対象ファイルがありません")
            print("⚠️  アップロード対象ファイルがありません")
            return []
        
        # v4 進捗トラッカー初期化
        total_size = sum(f['size'] for f in files)
        self.progress_tracker = ProgressTrackerV4(len(files), total_size)
        self.logger.info(f"S3アップロード開始 - ファイル数: {len(files)}, 総サイズ: {total_size}, 並列度: {parallel_workers}")
        
        try:
            # S3クライアント設定情報の取得
            bucket_name = self.config['aws']['s3_bucket']
            storage_class = self._validate_storage_class(
                self.config['aws'].get('storage_class', 'STANDARD')
            )
            max_retries = self.config['processing'].get('retry_count', 3)
            
            self.logger.info(f"S3設定 - バケット: {bucket_name}, ストレージクラス: {storage_class}, リトライ: {max_retries}")
            
            if parallel_workers == 1:
                # シーケンシャル処理（従来版）
                self.logger.info("シーケンシャル処理モードで実行")
                return self._sequential_upload(files, bucket_name, storage_class, max_retries)
            else:
                # 並列処理（v4新機能）
                self.logger.info(f"並列処理モードで実行: {parallel_workers}並列")
                return self._parallel_upload(files, bucket_name, storage_class, max_retries, parallel_workers)
            
        except Exception as e:
            error_msg = f"S3アップロード処理でエラーが発生: {str(e)}"
            self.logger.error(error_msg)
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
                    'is_simulated': False
                }
                for f in files
            ]
    
    def _sequential_upload(self, files: List[Dict], bucket_name: str, 
                          storage_class: str, max_retries: int) -> List[Dict]:
        """シーケンシャル処理によるS3アップロード"""
        s3_client = self._initialize_s3_client()
        results = []
        
        for i, file_info in enumerate(files, 1):
            result = self._upload_single_file(
                s3_client, file_info, bucket_name, storage_class, max_retries
            )
            results.append(result)
        
        # 最終サマリー表示
        self.progress_tracker.print_final_summary(self.error_simulator, 1)
        
        success_count = len([r for r in results if r['success']])
        failed_count = len([r for r in results if not r['success']])
        self.logger.info(f"シーケンシャルアップロード完了 - 成功: {success_count}, 失敗: {failed_count}")
        
        return results
    
    def _parallel_upload(self, files: List[Dict], bucket_name: str, 
                        storage_class: str, max_retries: int, workers: int) -> List[Dict]:
        """並列処理によるS3アップロード（v4新機能）"""
        self.logger.info(f"並列アップロード開始: {workers}並列")
        
        results = [None] * len(files)  # 結果を順序通りに格納
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # 各ファイルのアップロードタスクを送信
            future_to_index = {}
            
            for i, file_info in enumerate(files):
                # 各スレッド用にS3クライアントを作成
                future = executor.submit(
                    self._upload_single_file_thread_safe, 
                    file_info, bucket_name, storage_class, max_retries
                )
                future_to_index[future] = i
            
            # 完了したタスクから結果を取得
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    result = future.result()
                    results[index] = result
                except Exception as e:
                    self.logger.error(f"並列アップロードでエラー: {str(e)}")
                    # エラー時のフォールバック結果
                    file_info = files[index]
                    results[index] = {
                        'file_path': file_info['path'],
                        'file_size': file_info['size'],
                        'directory': file_info['directory'],
                        'success': False,
                        'error': f"並列処理エラー: {str(e)}",
                        's3_key': None,
                        'modified_time': file_info['modified_time'],
                        'upload_time': 0,
                        'is_simulated': False
                    }
        
        # 最終サマリー表示（並列処理版）
        self.progress_tracker.print_final_summary(self.error_simulator, workers)
        
        success_count = len([r for r in results if r and r['success']])
        failed_count = len([r for r in results if r and not r['success']])
        self.logger.info(f"並列アップロード完了 - 成功: {success_count}, 失敗: {failed_count}")
        
        return results
    
    def _upload_single_file_thread_safe(self, file_info: Dict, bucket_name: str, 
                                       storage_class: str, max_retries: int) -> Dict:
        """スレッドセーフな単一ファイルアップロード"""
        # 各スレッドで独立したS3クライアントを使用
        s3_client = self._initialize_s3_client()
        return self._upload_single_file(s3_client, file_info, bucket_name, storage_class, max_retries)
    
    def _upload_single_file(self, s3_client, file_info: Dict, bucket_name: str, 
                           storage_class: str, max_retries: int) -> Dict:
        """単一ファイルのアップロード処理"""
        file_path = file_info['path']
        file_size = file_info['size']
        
        # 進捗表示更新（処理開始）
        self.progress_tracker.update_file_start(file_path, file_size)
        
        s3_key = self._generate_s3_key(file_path)
        
        # エラーシミュレーション判定
        is_simulated_error = False
        simulated_error_type = None
        
        if self.error_simulator.should_simulate_error('file_lock'):
            is_simulated_error = True
            simulated_error_type = 'file_lock'
        elif self.error_simulator.should_simulate_error('s3_invalid'):
            is_simulated_error = True
            simulated_error_type = 's3_invalid'
        elif self.error_simulator.should_simulate_error('file_missing'):
            is_simulated_error = True
            simulated_error_type = 'file_missing'
        
        # シミュレーションエラーの場合
        if is_simulated_error:
            upload_time = random.uniform(0.1, 2.0)
            time.sleep(upload_time)
            
            error_messages = {
                'file_lock': f'ファイルロックエラー（シミュレーション）: {file_path} は他のプロセスで使用中です',
                's3_invalid': f'S3エラー（シミュレーション）: 無効なバケット名またはアクセス権限エラー',
                'file_missing': f'ファイル消失エラー（シミュレーション）: {file_path} が見つかりません'
            }
            
            error_msg = error_messages.get(simulated_error_type, 'シミュレーションエラー')
            self.logger.warning(f"シミュレーションエラー発生: {error_msg}")
            
            upload_result = {'success': False, 'error': error_msg}
        else:
            # 通常のアップロード処理
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
            'upload_time': upload_time,
            'is_simulated': is_simulated_error
        }
        
        # ログ出力
        if upload_result['success']:
            self.logger.info(f"アップロード成功: {os.path.basename(file_path)} -> s3://{bucket_name}/{s3_key}")
        else:
            self.logger.error(f"アップロード失敗: {os.path.basename(file_path)} - {upload_result.get('error', '不明なエラー')}")
        
        # 進捗表示更新（処理完了）
        if upload_result['success']:
            self.progress_tracker.update_file_success(file_path, file_size, upload_time)
        else:
            self.progress_tracker.update_file_failure(
                file_path, file_size, upload_result.get('error', '不明なエラー'), is_simulated_error
            )
        
        return result
    
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
            
            # 接続テスト（初回のみ）
            try:
                s3_client.head_bucket(Bucket=bucket_name)
            except Exception as e:
                # 並列処理では接続テスト失敗をワーニングレベルで記録
                self.logger.warning(f"S3バケット接続テスト: {str(e)}")
            
            return s3_client
            
        except ImportError:
            raise Exception("boto3がインストールされていません。pip install boto3 を実行してください。")
        except Exception as e:
            raise Exception(f"S3クライアント初期化失敗: {str(e)}")
    
    def _validate_storage_class(self, storage_class: str) -> str:
        """ストレージクラス検証・調整"""
        if storage_class == 'GLACIER_DEEP_ARCHIVE':
            self.logger.info("ストレージクラス自動変換: GLACIER_DEEP_ARCHIVE -> DEEP_ARCHIVE")
            return 'DEEP_ARCHIVE'
        
        valid_classes = ['STANDARD', 'STANDARD_IA', 'GLACIER', 'DEEP_ARCHIVE']
        
        if storage_class in valid_classes:
            return storage_class
        
        self.logger.warning(f"無効なストレージクラス '{storage_class}' のため 'STANDARD' に変更")
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
            self.logger.error(f"S3キー生成エラー: {file_path} - {str(e)}")
            filename = os.path.basename(file_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            return f"fallback/{timestamp}/{filename}"
    
    def _upload_file_with_retry(self, s3_client, file_path: str, bucket_name: str, 
                               s3_key: str, storage_class: str, max_retries: int) -> Dict:
        """ファイルアップロード（リトライ付き）"""
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"アップロード試行 {attempt + 1}/{max_retries}: {s3_key}")
                
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
                
                self.logger.warning(f"アップロード失敗 (試行 {attempt + 1}/{max_retries}): {error_msg}")
                time.sleep(2 ** attempt)  # 指数バックオフ
        
        return {'success': False, 'error': '不明なエラー'}
    
    def create_archived_files(self, results: List[Dict]) -> List[Dict]:
        """アーカイブ後処理（空ファイル作成→元ファイル削除）"""
        self.logger.info("アーカイブ後処理開始")
        print(f"📄 アーカイブ後処理開始")
        
        successful_results = [r for r in results if r and r.get('success', False)]
        
        if not successful_results:
            self.logger.info("S3アップロード成功ファイルがないため、アーカイブ後処理をスキップ")
            print("⚠️  S3アップロード成功ファイルがないため、アーカイブ後処理をスキップ")
            return results
        
        self.logger.info(f"アーカイブ後処理対象: {len(successful_results)}件")
        print(f"📊 アーカイブ後処理対象: {len(successful_results)}件")
        
        archived_suffix = self.config.get('file_server', {}).get('archived_suffix', '.archived')
        processed_results = []
        
        for result in results:
            if not result or not result.get('success', False):
                processed_results.append(result)
                continue
            
            file_path = result['file_path']
            
            try:
                archived_file_path = f"{file_path}{archived_suffix}"
                
                self.logger.info(f"空ファイル作成: {archived_file_path}")
                
                with open(archived_file_path, 'w') as f:
                    pass
                
                if not os.path.exists(archived_file_path):
                    raise Exception("空ファイルの作成に失敗しました")
                
                self.logger.info(f"元ファイル削除: {file_path}")
                os.remove(file_path)
                
                if os.path.exists(file_path):
                    raise Exception("元ファイルの削除に失敗しました")
                
                result['archived_file_path'] = archived_file_path
                result['archive_completed'] = True
                
            except Exception as e:
                error_msg = f"アーカイブ後処理失敗: {str(e)}"
                self.logger.error(f"{error_msg}: {file_path}")
                
                try:
                    if 'archived_file_path' in locals() and os.path.exists(archived_file_path):
                        os.remove(archived_file_path)
                        self.logger.info(f"作成済み空ファイルを削除: {archived_file_path}")
                except Exception as cleanup_error:
                    self.logger.warning(f"空ファイルクリーンアップ失敗: {cleanup_error}")
                
                result['success'] = False
                result['error'] = error_msg
                result['archive_completed'] = False
            
            processed_results.append(result)
        
        completed_count = len([r for r in processed_results if r and r.get('archive_completed', False)])
        failed_count = len([r for r in processed_results if r and r.get('success', False) and not r.get('archive_completed', False)])
        
        self.logger.info(f"アーカイブ後処理完了: {completed_count}件, 失敗: {failed_count}件")
        print(f"✅ アーカイブ後処理完了: {completed_count}件")
        print(f"❌ アーカイブ後処理失敗: {failed_count}件")
        
        return processed_results
        
    def save_to_database(self, results: List[Dict]) -> None:
        """データベース登録処理"""
        self.logger.info("データベース登録開始")
        print(f"🗄️  データベース登録開始")
        
        completed_results = [r for r in results if r and r.get('archive_completed', False)]
        
        if not completed_results:
            self.logger.info("データベース登録対象ファイルがありません")
            print("⚠️  データベース登録対象ファイルがありません")
            return
        
        self.logger.info(f"データベース登録対象: {len(completed_results)}件")
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
                    
                    insert_query = """
                        INSERT INTO archive_history (
                            request_id, requester, request_date,
                            original_file_path, s3_path, archive_date, file_size
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.executemany(insert_query, insert_data)
                    inserted_count = cursor.rowcount
                    self.logger.info(f"データベース挿入完了: {inserted_count}件")
                    print(f"✅ データベース挿入完了: {inserted_count}件")
            
            self.logger.info("データベース登録完了")
            print(f"🗄️  データベース登録完了")
            
        except Exception as e:
            error_msg = f"データベース登録エラー: {str(e)}"
            self.logger.error(error_msg)
            print(f"❌ {error_msg}")
            
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
            
            self.logger.info(f"データベース接続: {conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
            
            conn = psycopg2.connect(**conn_params)
            conn.autocommit = False
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            self.logger.info("データベース接続成功")
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
    
    def generate_csv_error_file(self, original_csv_path: str) -> Optional[str]:
        """CSV検証エラー用のエラーファイル生成"""
        if not self.csv_errors:
            return None
            
        self.logger.info("CSV検証エラーファイル生成開始")
        print(f"📄 CSV検証エラーファイル生成開始")
        
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            original_path = Path(original_csv_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            error_csv_path = log_dir / f"{original_path.stem}_csv_retry_{timestamp}.csv"
            
            original_header = self._get_original_csv_header(original_csv_path)
            
            with open(error_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                
                if original_header:
                    writer.writerow([original_header])
                
                for item in self.csv_errors:
                    writer.writerow([item['path']])
            
            self.logger.info(f"CSV検証エラー（再試行用）ファイル生成完了: {error_csv_path}")
            self.logger.info(f"再試行対象パス数: {len(self.csv_errors)}")
            print(f"✅ CSV検証エラー（再試行用）ファイル生成完了: {error_csv_path}")
            print(f"📊 再試行対象パス数: {len(self.csv_errors)}")
            return str(error_csv_path)
            
        except Exception as e:
            error_msg = f"CSV検証エラーファイル生成失敗: {str(e)}"
            self.logger.error(error_msg)
            print(f"❌ {error_msg}")
            return None
    
    def generate_error_csv(self, failed_items: List[Dict], original_csv_path: str) -> Optional[str]:
        """アーカイブ処理失敗ファイル用の再試行CSVファイル生成"""
        if not failed_items:
            return None
            
        self.logger.info("アーカイブエラー（再試行用）ファイル生成開始")
        print(f"📄 アーカイブエラー（再試行用）ファイル生成開始")
        
        try:
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            original_path = Path(original_csv_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            error_csv_path = log_dir / f"{original_path.stem}_archive_retry_{timestamp}.csv"
            
            original_header = self._get_original_csv_header(original_csv_path)
            
            failed_directories = set()
            for item in failed_items:
                directory = item.get('directory')
                if directory:
                    failed_directories.add(directory)
            
            with open(error_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                
                if original_header:
                    writer.writerow([original_header])
                
                for directory in sorted(failed_directories):
                    writer.writerow([directory])
            
            self.logger.info(f"アーカイブエラー（再試行用）ファイル生成完了: {error_csv_path}")
            self.logger.info(f"再試行対象ディレクトリ数: {len(failed_directories)}")
            self.logger.info(f"失敗ファイル数: {len(failed_items)}")
            print(f"✅ アーカイブエラー（再試行用）ファイル生成完了: {error_csv_path}")
            print(f"📊 再試行対象ディレクトリ数: {len(failed_directories)}")
            print(f"📊 失敗ファイル数: {len(failed_items)}")
            
            error_summary = {}
            for item in failed_items:
                error_type = item.get('error', '不明なエラー')
                error_summary[error_type] = error_summary.get(error_type, 0) + 1
            
            self.logger.info("エラー理由の内訳:")
            print(f"📋 エラー理由の内訳:")
            for error_type, count in error_summary.items():
                self.logger.info(f"  - {error_type}: {count}件")
                print(f"   - {error_type}: {count}件")
            
            return str(error_csv_path)
            
        except Exception as e:
            error_msg = f"アーカイブエラーファイル生成失敗: {str(e)}"
            self.logger.error(error_msg)
            print(f"❌ {error_msg}")
            return None
    
    def _get_original_csv_header(self, csv_path: str) -> Optional[str]:
        """元CSVファイルのヘッダー行を取得"""
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                if any(keyword in first_line.lower() for keyword in ['directory', 'path']):
                    return first_line
                return "Directory Path"
        except Exception:
            return "Directory Path"
        
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
        
    def run(self, csv_path: str, request_id: str, parallel_workers: Optional[int] = None) -> int:
        """メイン処理実行（v4: 並列度指定対応）"""
        self.stats['start_time'] = datetime.datetime.now()
        self.request_id = request_id
        
        try:
            # 並列度の決定
            workers = self.determine_parallel_workers(parallel_workers)
            
            self.logger.info(f"アーカイブ処理開始 - Request ID: {request_id}, 並列度: {workers}")
            print(f"🚀 アーカイブ処理開始 (v4) - Request ID: {request_id}")
            print(f"📄 CSV: {csv_path}")
            
            # 1. CSV読み込み・検証
            directories, csv_errors = self.validate_csv_input(csv_path)
            
            if csv_errors:
                error_csv_path = self.generate_csv_error_file(csv_path)
                if error_csv_path:
                    self.logger.warning(f"CSV検証エラーが発生しました: {error_csv_path}")
                    print(f"⚠️  CSV検証エラーが発生しました: {error_csv_path}")
            
            if not directories:
                self.logger.error("処理対象のディレクトリが見つかりません")
                print("❌ 処理対象のディレクトリが見つかりません")
                return 1
                
            # 2. ファイル収集
            files = self.collect_files(directories)
            if not files:
                self.logger.warning("処理対象のファイルが見つかりません")
                print("⚠️  処理対象のファイルが見つかりません")
                return 0
                
            self.stats['total_files'] = len(files)
            self.stats['total_size'] = sum(f['size'] for f in files)
            
            # 3. S3アップロード（v4並列処理対応）
            upload_results = self.archive_to_s3(files, workers)
            
            # 4. アーカイブ後処理
            print(f"\n📄 アーカイブ後処理開始...")
            processed_results = self.create_archived_files(upload_results)
            
            # 5. データベース登録
            print(f"🗄️  データベース登録開始...")
            self.save_to_database(processed_results)
            
            # 6. アーカイブ処理エラー処理
            failed_items = [r for r in processed_results if r and not r.get('success', False)]
            if failed_items:
                archive_error_csv = self.generate_error_csv(failed_items, csv_path)
                if archive_error_csv:
                    self.logger.warning(f"アーカイブエラーが発生したファイルがあります: {archive_error_csv}")
                    print(f"⚠️  アーカイブエラーが発生したファイルがあります: {archive_error_csv}")
                else:
                    self.logger.error("アーカイブエラーCSVの生成に失敗しました")
                    print(f"❌ アーカイブエラーCSVの生成に失敗しました")
            else:
                self.logger.info("全てのファイルが正常にアーカイブされました")
                print(f"🎉 全てのファイルが正常にアーカイブされました")
                
            # 結果サマリー（v4拡張）
            successful_results = [r for r in processed_results if r and r.get('success', False)]
            failed_results = [r for r in processed_results if r and not r.get('success', False)]
            simulated_results = [r for r in processed_results if r and r.get('is_simulated', False)]
            
            self.stats['processed_files'] = len(successful_results)
            self.stats['failed_files'] = len(failed_results)
            
            self.logger.info("アーカイブ処理完了")
            self.logger.info(f"成功: {len(successful_results)}件")
            self.logger.info(f"失敗: {len(failed_results)}件")
            if simulated_results:
                self.logger.info(f"シミュレーション: {len(simulated_results)}件")
            
            print(f"🎉 アーカイブ処理完了! (v4)")
            print(f"✅ 成功: {len(successful_results)}件")
            print(f"❌ 失敗: {len(failed_results)}件")
            if simulated_results:
                print(f"🧪 シミュレーション: {len(simulated_results)}件")
            print(f"🔄 並列度: {workers}")
            
            return 0
            
        except Exception as e:
            error_msg = f"アーカイブ処理中にエラーが発生しました: {str(e)}"
            self.logger.error(error_msg)
            print(f"\n❌ {error_msg}")
            return 1
            
        finally:
            self.stats['end_time'] = datetime.datetime.now()
            self.print_statistics()

def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='アーカイブスクリプト検証版v4（並列処理対応）')
    parser.add_argument('csv_path', help='対象ディレクトリを記載したCSVファイルのパス')
    parser.add_argument('request_id', help='アーカイブ依頼ID')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH, 
                       help=f'設定ファイルのパス (デフォルト: {DEFAULT_CONFIG_PATH})')
    
    # v4 新機能: 並列度指定
    parser.add_argument('--parallel', type=int, metavar='N',
                       help='並列度指定 (1=シーケンシャル, 2以上=並列処理, 0=自動設定)')
    
    # v3 機能: エラーシミュレーション用オプション
    sim_group = parser.add_argument_group('エラーシミュレーション')
    sim_group.add_argument('--simulate-file-lock', type=float, metavar='RATE',
                          help='ファイルロックエラーのシミュレーション率 (0.0-1.0)')
    sim_group.add_argument('--simulate-permission', type=float, metavar='RATE',
                          help='権限エラーのシミュレーション率 (0.0-1.0)')
    sim_group.add_argument('--simulate-network', type=float, metavar='RATE',
                          help='ネットワークエラーのシミュレーション率 (0.0-1.0)')
    sim_group.add_argument('--simulate-file-missing', type=float, metavar='RATE',
                          help='ファイル消失エラーのシミュレーション率 (0.0-1.0)')
    sim_group.add_argument('--simulate-s3-invalid', type=float, metavar='RATE',
                          help='S3エラーのシミュレーション率 (0.0-1.0)')
    
    args = parser.parse_args()
    
    # CSVファイルの存在チェック
    if not os.path.exists(args.csv_path):
        print(f"❌ CSVファイルが見つかりません: {args.csv_path}")
        sys.exit(1)
    
    print(f"🔍 アーカイブスクリプト検証版 v4")
    print(f"📋 機能: 並列処理・エラーシミュレーション・詳細エラー分類・性能統計")
    print(f"📄 CSV: {args.csv_path}")
    print(f"🆔 Request ID: {args.request_id}")
    print(f"⚙️  設定ファイル: {args.config}")
    
    # 並列度の表示
    if args.parallel is not None:
        if args.parallel == 1:
            print(f"🔄 実行モード: シーケンシャル処理")
        elif args.parallel > 1:
            print(f"🔄 実行モード: 並列処理 ({args.parallel}並列)")
        else:
            print(f"🔄 実行モード: 自動設定")
    
    # アーカイブ処理の実行
    processor = ArchiveProcessorTestV4(args.config)
    
    # エラーシミュレーション設定
    simulation_args = {}
    if args.simulate_file_lock is not None:
        simulation_args['file_lock'] = args.simulate_file_lock
    if args.simulate_permission is not None:
        simulation_args['permission'] = args.simulate_permission
    if args.simulate_network is not None:
        simulation_args['network'] = args.simulate_network
    if args.simulate_file_missing is not None:
        simulation_args['file_missing'] = args.simulate_file_missing
    if args.simulate_s3_invalid is not None:
        simulation_args['s3_invalid'] = args.simulate_s3_invalid
    
    if simulation_args:
        processor.enable_error_simulation(simulation_args)
    
    exit_code = processor.run(args.csv_path, args.request_id, args.parallel)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()error_reason': '空のパス'}
            
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
            return {'valid': False, '