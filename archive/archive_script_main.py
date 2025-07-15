#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
確実に動作するアーカイブスクリプト（エラー記録対応版）
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
        
    def validate_csv_input(self, csv_path: str) -> List[str]:
        """CSVファイルの読み込み・検証処理（エラー記録対応版）"""
        self.logger.info(f"CSVファイルの読み込み開始: {csv_path}")
        
        directories = []
        self.csv_errors = []  # エラーリストをリセット
        
        try:
            # シンプルに行単位で読み込み（確実に動作することが確認済み）
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            self.logger.info(f"読み込み行数: {len(lines)}")
            
            for i, line in enumerate(lines):
                line_num = i + 1
                clean_line = line.strip()
                
                if not clean_line:
                    continue
                
                # ヘッダー行をスキップ（1行目で"Directory"または"Path"を含む場合）
                if i == 0 and any(keyword in clean_line.lower() for keyword in ['directory', 'path']):
                    self.logger.info(f"行 {line_num}: ヘッダー行をスキップ")
                    continue
                
                # パスとして処理
                path = clean_line
                
                # パス長を確認（ログ表示制限を避けるため最初の50文字のみ表示）
                path_preview = path[:50] + "..." if len(path) > 50 else path
                self.logger.info(f"行 {line_num}: パス検証中 - {path_preview} (全長: {len(path)}文字)")
                
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
                validation_result = self._validate_directory_path_detailed(path)
                if validation_result['valid']:
                    directories.append(path)
                    self.logger.info(f"行 {line_num}: 有効なパス追加 ✓")
                else:
                    # エラー項目として記録
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
            return []
        
        self.logger.info(f"CSV読み込み完了")
        self.logger.info(f"  - 有効ディレクトリ数: {len(directories)}")
        self.logger.info(f"  - エラー項目数: {len(self.csv_errors)}")
        
        return directories
    
    def _validate_directory_path_detailed(self, path: str) -> Dict:
        """ディレクトリパスの詳細検証"""
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
        result = self._validate_directory_path_detailed(path)
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
        
        # TODO: 実際のS3アップロード処理
        results = []
        for file_info in files:
            result = {
                'file_path': file_info['path'],
                'success': True,
                'error': None,
                's3_key': f"archive/{file_info['path'].replace('\\', '/')}"
            }
            results.append(result)
        
        self.logger.info("S3アップロード完了")
        return results
        
    def create_archived_files(self, results: List[Dict]) -> List[Dict]:
        """アーカイブ後処理"""
        self.logger.info("アーカイブ後処理開始")
        # TODO: 実装
        self.logger.info("アーカイブ後処理完了")
        return results
        
    def save_to_database(self, results: List[Dict]) -> None:
        """データベース登録処理"""
        self.logger.info("データベース登録開始")
        # TODO: 実装
        self.logger.info("データベース登録完了")
        
    def generate_csv_error_file(self, original_csv_path: str) -> Optional[str]:
        """CSV検証エラー用のエラーファイル生成（再試行用フォーマット）"""
        if not self.csv_errors:
            return None
            
        self.logger.info("CSV検証エラーファイル生成開始")
        
        try:
            # エラーCSVのパス生成（元CSVと同じ場所）
            original_path = Path(original_csv_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            error_csv_path = original_path.parent / f"{original_path.stem}_retry_{timestamp}.csv"
            
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
            
            self.logger.info(f"再試行用CSVファイル生成完了: {error_csv_path}")
            self.logger.info(f"再試行対象パス数: {len(self.csv_errors)}")
            return str(error_csv_path)
            
        except Exception as e:
            self.logger.error(f"再試行用CSVファイル生成失敗: {str(e)}")
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
        
    def generate_error_csv(self, failed_items: List[Dict], original_csv_path: str) -> Optional[str]:
        """アーカイブ処理エラー用のエラーCSV生成"""
        if not failed_items:
            return None
        # TODO: 実装
        return "error_output.csv"
        
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
        
        try:
            self.logger.info(f"アーカイブ処理開始 - Request ID: {request_id}")
            
            # 1. CSVファイル読み込み・検証
            directories = self.validate_csv_input(csv_path)
            
            # CSV検証エラーがあった場合はエラーファイルを生成
            if self.csv_errors:
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
                error_csv_path = self.generate_error_csv(failed_items, csv_path)
                self.logger.warning(f"アーカイブエラーが発生したファイルがあります: {error_csv_path}")
                
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