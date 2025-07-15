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
                            'error_reason': 'カラム数が不足しています（S3パス, 復元先ディレクトリが必要）',
                            'original_line': line.rstrip()
                        }
                        self.csv_errors.append(error_item)
                        self.logger.error(f"行 {line_num}: カラム数不足 ✗")
                        continue
                    
                    s3_path = row[0].strip()
                    restore_dir = row[1].strip()
                    
                    # 復元依頼の検証
                    validation_result = self._validate_restore_request(s3_path, restore_dir)
                    
                    if validation_result['valid']:
                        request_item = {
                            'line_number': line_num,
                            's3_path': s3_path,
                            'restore_directory': restore_dir,
                            'bucket': self._extract_bucket_from_s3_path(s3_path),
                            'key': self._extract_key_from_s3_path(s3_path)
                        }
                        valid_requests.append(request_item)
                        self.logger.info(f"行 {line_num}: 有効な復元依頼追加 ✓")
                    else:
                        error_item = {
                            'line_number': line_num,
                            'content': f"{s3_path} -> {restore_dir}",
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
    
    def _validate_restore_request(self, s3_path: str, restore_dir: str) -> Dict:
        """復元依頼の検証"""
        try:
            # S3パスの形式チェック
            if not s3_path.startswith('s3://'):
                return {'valid': False, 'error_reason': 'S3パスはs3://で始まる必要があります'}
            
            # S3パスの構成要素チェック
            if s3_path.count('/') < 3:  # s3://bucket/key最低限
                return {'valid': False, 'error_reason': 'S3パスの形式が正しくありません'}
            
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
        
        # TODO: 実装
        self.logger.info("S3復元リクエスト送信完了")
        return restore_requests
        
    def wait_for_restore_completion(self, restore_requests: List[Dict]) -> List[Dict]:
        """復元完了待機"""
        self.logger.info("復元完了待機開始")
        
        # TODO: 実装
        self.logger.info("復元完了待機完了")
        return restore_requests
        
    def download_and_place_files(self, restore_requests: List[Dict]) -> List[Dict]:
        """ファイルダウンロード・配置"""
        self.logger.info("ファイルダウンロード・配置開始")
        
        # TODO: 実装
        self.logger.info("ファイルダウンロード・配置完了")
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
                - 'download': ダウンロード実行のみ
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
        
        # 2. S3復元リクエスト送信
        restore_requests = self.request_restore(restore_requests)
        
        # 3. ステータスファイル保存
        self._save_restore_status(restore_requests)
        
        self.logger.info(f"復元リクエスト送信完了 - {self.stats['restore_requested']}件")
        self.logger.info("48時間後にダウンロード処理を実行してください:")
        self.logger.info(f"python restore_script_main.py {csv_path} {self.request_id} --download-only")
        
        return 0
    
    def _run_download_files(self, csv_path: str) -> int:
        """ダウンロード実行処理"""
        self.logger.info("=== ダウンロード実行モード ===")
        
        # 1. ステータスファイル読み込み
        restore_requests = self._load_restore_status()
        if not restore_requests:
            self.logger.error("復元ステータスファイルが見つかりません")
            self.logger.error("先に復元リクエスト送信を実行してください")
            return 1
        
        self.stats['total_files'] = len(restore_requests)
        
        # 2. 復元完了確認
        restore_requests = self.check_restore_completion(restore_requests)
        
        # 3. ファイルダウンロード・配置
        restore_requests = self.download_and_place_files(restore_requests)
        
        # 4. ステータスファイル更新
        self._save_restore_status(restore_requests)
        
        self.logger.info(f"ダウンロード処理完了 - {self.stats['restore_completed']}件")
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
    
    def check_restore_completion(self, restore_requests: List[Dict]) -> List[Dict]:
        """復元完了確認"""
        self.logger.info("復元完了確認開始")
        
        # TODO: S3 APIで復元ステータス確認
        # TODO: 完了したファイルのみマークを更新
        
        self.logger.info("復元完了確認完了")
        return restore_requests


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
                           help='ダウンロード実行のみ実行')
    
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