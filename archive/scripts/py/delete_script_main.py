#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
復元ファイル削除スクリプト（シンプル版）
CSVで指定されたパスのファイル・ディレクトリを削除する
"""

import argparse
import csv
import datetime
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = "config/archive_config.json"

class SimpleDeletionProcessor:
    """シンプル削除処理クラス"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
        
    def load_config(self, config_path: str) -> Dict:
        """設定ファイルを読み込み"""
        default_config = {
            "logging": {
                "log_directory": "logs",
                "log_level": "INFO"
            }
        }
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            # デフォルト設定とマージ
            for key, value in default_config.items():
                if key not in config:
                    config[key] = value
            return config
        except Exception as e:
            print(f"設定ファイル読み込みエラー。デフォルト設定を使用: {e}")
            return default_config
            
    def setup_logger(self) -> logging.Logger:
        """ログ設定の初期化"""
        logger = logging.getLogger('simple_deletion')
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
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
            
            log_file = log_dir / f"deletion_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            logger.info(f"ログファイル: {log_file}")
        except Exception as e:
            logger.warning(f"ログファイル設定エラー: {e}")
        
        return logger
        
    def read_deletion_paths(self, csv_path: str) -> List[str]:
        """削除対象パスをCSVから読み込み"""
        paths = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines):
                clean_line = line.strip()
                if not clean_line:
                    continue
                    
                # ヘッダー行をスキップ
                if i == 0 and any(keyword in clean_line.lower() for keyword in ['削除', 'delete', 'path']):
                    continue
                
                if os.path.exists(clean_line):
                    paths.append(clean_line)
                    self.logger.info(f"削除対象: {clean_line}")
                else:
                    self.logger.warning(f"パスが存在しません: {clean_line}")
                    
        except Exception as e:
            self.logger.error(f"CSV読み込みエラー: {e}")
            
        return paths
    
    def calculate_size(self, path: str) -> int:
        """パス配下の総サイズを計算"""
        try:
            if os.path.isfile(path):
                return os.path.getsize(path)
            elif os.path.isdir(path):
                total_size = 0
                for dirpath, dirnames, filenames in os.walk(path):
                    for filename in filenames:
                        file_path = os.path.join(dirpath, filename)
                        try:
                            total_size += os.path.getsize(file_path)
                        except (OSError, FileNotFoundError):
                            continue
                return total_size
            return 0
        except Exception:
            return 0
    
    def format_size(self, size_bytes: int) -> str:
        """ファイルサイズフォーマット"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        size = float(size_bytes)
        
        while size >= 1024.0 and i < len(size_names) - 1:
            size /= 1024.0
            i += 1
        
        return f"{size:.1f} {size_names[i]}"
    
    def delete_paths(self, paths: List[str], dry_run: bool = False) -> Dict:
        """削除処理の実行"""
        results = {
            'successful': 0,
            'failed': 0,
            'total_size': 0,
            'errors': []
        }
        
        if dry_run:
            self.logger.info("=== ドライランモード ===")
        
        for path in paths:
            try:
                size = self.calculate_size(path)
                
                if dry_run:
                    self.logger.info(f"[ドライラン] 削除予定: {path} ({self.format_size(size)})")
                    results['successful'] += 1
                    results['total_size'] += size
                else:
                    if os.path.isfile(path):
                        os.remove(path)
                    elif os.path.isdir(path):
                        shutil.rmtree(path)
                    
                    self.logger.info(f"削除完了: {path} ({self.format_size(size)})")
                    results['successful'] += 1
                    results['total_size'] += size
                    
            except Exception as e:
                error_msg = f"削除失敗: {path} - {str(e)}"
                self.logger.error(error_msg)
                results['failed'] += 1
                results['errors'].append(error_msg)
        
        return results
    
    def run(self, csv_path: str, dry_run: bool = False, skip_confirmation: bool = False) -> int:
        """メイン処理"""
        try:
            self.logger.info("削除処理開始")
            
            # CSV読み込み
            paths = self.read_deletion_paths(csv_path)
            if not paths:
                self.logger.error("削除対象が見つかりません")
                return 1
            
            # 削除前確認
            if not skip_confirmation and not dry_run:
                print(f"\n削除対象 ({len(paths)}件):")
                total_size = 0
                for i, path in enumerate(paths, 1):
                    size = self.calculate_size(path)
                    total_size += size
                    print(f"  {i}. {path} ({self.format_size(size)})")
                
                print(f"\n削除予定総容量: {self.format_size(total_size)}")
                confirmation = input(f"\n{len(paths)}件を削除しますか？ (yes/no): ")
                if confirmation.lower() not in ['yes', 'y']:
                    self.logger.info("処理をキャンセルしました")
                    return 0
            
            # 削除実行
            results = self.delete_paths(paths, dry_run)
            
            # 結果表示
            mode = "ドライラン" if dry_run else "削除処理"
            self.logger.info(f"=== {mode}結果 ===")
            self.logger.info(f"成功: {results['successful']}件")
            self.logger.info(f"失敗: {results['failed']}件")
            
            if results['errors']:
                self.logger.info("エラー詳細:")
                for error in results['errors']:
                    self.logger.info(f"  {error}")
            
            return 0 if results['failed'] == 0 else 1
            
        except Exception as e:
            self.logger.error(f"処理中にエラーが発生: {e}")
            return 1


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='復元ファイル削除処理（シンプル版）')
    parser.add_argument('csv_path', help='削除対象パスを記載したCSVファイル')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH, 
                       help=f'設定ファイルのパス (デフォルト: {DEFAULT_CONFIG_PATH})')
    parser.add_argument('--dry-run', action='store_true',
                       help='ドライランモード（実際の削除は行わない）')
    parser.add_argument('--skip-confirmation', action='store_true',
                       help='削除前確認をスキップ')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_path):
        print(f"CSVファイルが見つかりません: {args.csv_path}")
        sys.exit(1)
        
    processor = SimpleDeletionProcessor(args.config)
    exit_code = processor.run(args.csv_path, 
                             dry_run=args.dry_run, 
                             skip_confirmation=args.skip_confirmation)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()