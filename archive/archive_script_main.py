#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アーカイブスクリプト メイン処理（修正版）
企業内ファイルサーバからAWS S3 Glacier Deep Archiveへのアーカイブ処理
"""

import os
import sys
import json
import logging
import argparse
import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = "config/archive_config.json"

class ArchiveProcessor:
    """アーカイブ処理のメインクラス"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
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
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            print(f"設定ファイルが見つかりません: {config_path}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"設定ファイルの形式が正しくありません: {e}")
            sys.exit(1)
            
    def setup_logger(self) -> logging.Logger:
        """ログ設定の初期化"""
        logger = logging.getLogger('archive_processor')
        logger.setLevel(logging.INFO)
        
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
        log_dir = Path(self.config.get('logging', {}).get('log_directory', 'logs'))
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"archive_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
        
    def validate_csv_input(self, csv_path: str) -> List[str]:
        """CSVファイルの読み込み・検証処理（修正版）"""
        self.logger.info(f"CSVファイルの読み込み開始: {csv_path}")
        
        directories = []
        invalid_paths = []
        duplicate_paths = []
        
        # 複数のエンコーディングを試行
        encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932', 'euc-jp']
        
        for encoding in encodings:
            try:
                import csv
                
                self.logger.info(f"エンコーディング '{encoding}' で読み込み試行中...")
                
                with open(csv_path, 'r', encoding=encoding) as csvfile:
                    # CSVファイルの方言を自動検出
                    sample = csvfile.read(1024)
                    csvfile.seek(0)
                    sniffer = csv.Sniffer()
                    
                    try:
                        delimiter = sniffer.sniff(sample).delimiter
                        self.logger.info(f"検出されたデリミタ: '{delimiter}'")
                    except:
                        delimiter = ','
                        self.logger.info(f"デリミタ検出失敗、カンマを使用")
                    
                    reader = csv.reader(csvfile, delimiter=delimiter)
                    
                    # ヘッダー行の処理
                    headers = next(reader, None)
                    if headers:
                        # ヘッダーの正規化（空白除去）
                        headers = [h.strip() for h in headers]
                        self.logger.info(f"CSVヘッダー: {headers}")
                        
                        # ディレクトリパスのカラム位置を特定
                        path_column_index = self._find_path_column(headers)
                        if path_column_index == -1:
                            self.logger.error("ディレクトリパスのカラムが見つかりません")
                            continue
                    else:
                        # ヘッダーなしの場合、最初のカラムをパスとして扱う
                        path_column_index = 0
                        csvfile.seek(0)
                        reader = csv.reader(csvfile, delimiter=delimiter)
                    
                    # データ行の処理
                    seen_paths = set()
                    row_number = 1 if headers else 0
                    
                    for row in reader:
                        row_number += 1
                        
                        if not row or len(row) <= path_column_index:
                            self.logger.warning(f"行 {row_number}: 空行またはデータ不足をスキップ")
                            continue
                        
                        # ディレクトリパスの取得と正規化
                        raw_path = row[path_column_index].strip()
                        if not raw_path:
                            self.logger.warning(f"行 {row_number}: 空のパスをスキップ")
                            continue
                        
                        # パスの正規化
                        normalized_path = self._normalize_path(raw_path)
                        
                        # 重複チェック
                        if normalized_path in seen_paths:
                            duplicate_paths.append(normalized_path)
                            self.logger.warning(f"行 {row_number}: 重複パス検出 - {normalized_path}")
                            continue
                        
                        seen_paths.add(normalized_path)
                        
                        # パスの妥当性チェック
                        if self._validate_directory_path(normalized_path):
                            directories.append(normalized_path)
                            self.logger.info(f"行 {row_number}: 有効なパス - {normalized_path}")
                        else:
                            invalid_paths.append(normalized_path)
                            self.logger.error(f"行 {row_number}: 無効なパス - {normalized_path}")
                    
                    # エンコーディングが成功したのでループを抜ける
                    self.logger.info(f"エンコーディング '{encoding}' で読み込み成功")
                    break
                            
            except UnicodeDecodeError as e:
                self.logger.warning(f"エンコーディング '{encoding}' で読み込み失敗: {str(e)}")
                continue
            except FileNotFoundError:
                self.logger.error(f"CSVファイルが見つかりません: {csv_path}")
                return []
            except Exception as e:
                self.logger.error(f"CSVファイル読み込みエラー ({encoding}): {str(e)}")
                continue
        else:
            # すべてのエンコーディングで失敗
            self.logger.error("すべてのエンコーディングでCSVファイルの読み込みに失敗しました")
            return []
        
        # 結果サマリー
        self.logger.info(f"CSVファイル読み込み完了")
        self.logger.info(f"  - 有効なディレクトリ数: {len(directories)}")
        self.logger.info(f"  - 無効なパス数: {len(invalid_paths)}")
        self.logger.info(f"  - 重複パス数: {len(duplicate_paths)}")
        
        if invalid_paths:
            self.logger.warning(f"無効なパス: {invalid_paths}")
        if duplicate_paths:
            self.logger.warning(f"重複パス: {duplicate_paths}")
            
        return directories
    
    def _find_path_column(self, headers: List[str]) -> int:
        """ディレクトリパスのカラムを特定"""
        # よくある列名のパターン
        path_patterns = [
            'path', 'directory', 'folder', 'dir',
            'パス', 'ディレクトリ', 'フォルダ',
            'target', 'source', 'location'
        ]
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            for pattern in path_patterns:
                if pattern in header_lower:
                    return i
        
        # パターンが見つからない場合は最初のカラムを使用
        return 0
    
    def _normalize_path(self, path: str) -> str:
        """パスの正規化"""
        # 前後の空白を削除
        path = path.strip()
        
        # バックスラッシュを統一
        path = path.replace('/', '\\')
        
        # 末尾のバックスラッシュを削除
        path = path.rstrip('\\')
        
        # UNCパスの場合、先頭の\\を確保
        if path.startswith('\\\\'):
            return path
        
        # 相対パスの場合は絶対パスに変換の警告
        if not (path.startswith('\\\\') or (len(path) > 1 and path[1] == ':')):
            self.logger.warning(f"相対パスは推奨されません: {path}")
        
        return path
    
    def _validate_directory_path(self, path: str) -> bool:
        """ディレクトリパスの妥当性チェック（修正版）"""
        try:
            # 空文字チェック
            if not path or path.strip() == '':
                self.logger.error(f"空のパスです: '{path}'")
                return False
            
            # 基本的なパス形式チェック
            self.logger.debug(f"パス検証開始: '{path}'")
            
            # 不正な文字チェック（UNCパス考慮）
            invalid_chars = ['<', '>', '"', '|', '?', '*']
            # UNCパスの場合、先頭の\\とドライブ文字の:は除外
            check_path = path[2:] if path.startswith('\\\\') else path
            
            # ドライブ文字のコロンは除外
            if len(check_path) > 1 and check_path[1] == ':':
                check_path = check_path[0] + check_path[2:]
            
            for char in invalid_chars:
                if char in check_path:
                    self.logger.error(f"不正な文字が含まれています: '{char}' in '{path}'")
                    return False
            
            # パスの長さチェック（Windowsの制限を緩和）
            if len(path) > 32767:  # Windowsの内部制限
                self.logger.error(f"パスが長すぎます: {len(path)} > 32767")
                return False
            
            # ディレクトリの存在チェック
            try:
                exists = os.path.exists(path)
                if not exists:
                    self.logger.error(f"ディレクトリが存在しません: {path}")
                    return False
                
                # ディレクトリかどうかの確認
                is_dir = os.path.isdir(path)
                if not is_dir:
                    self.logger.error(f"ディレクトリではありません: {path}")
                    return False
                
                # アクセス権限チェック
                can_read = os.access(path, os.R_OK)
                if not can_read:
                    self.logger.error(f"ディレクトリへの読み取り権限がありません: {path}")
                    return False
                
            except PermissionError as e:
                self.logger.error(f"ディレクトリへのアクセス権限がありません: {path} - {str(e)}")
                return False
            except OSError as e:
                self.logger.error(f"ディレクトリアクセスエラー: {path} - {str(e)}")
                return False
            except Exception as e:
                self.logger.error(f"ディレクトリ検証中に予期しないエラー: {path} - {str(e)}")
                return False
            
            self.logger.debug(f"パス検証成功: {path}")
            return True
            
        except Exception as e:
            self.logger.error(f"ディレクトリパス検証中にエラー: {path} - {str(e)}")
            return False
        
    def collect_files(self, directories: List[str]) -> List[Dict]:
        """ファイル列挙・収集処理"""
        self.logger.info("ファイル収集開始")
        
        files = []
        exclude_extensions = self.config.get('file_server', {}).get('exclude_extensions', [])
        max_file_size = self.config.get('processing', {}).get('max_file_size', 10737418240)  # 10GB
        
        for directory in directories:
            self.logger.info(f"ディレクトリ処理開始: {directory}")
            
            try:
                # ディレクトリ内のファイルを再帰的に収集
                for root, dirs, filenames in os.walk(directory):
                    for filename in filenames:
                        file_path = os.path.join(root, filename)
                        
                        # 拡張子チェック
                        _, ext = os.path.splitext(filename)
                        if ext.lower() in exclude_extensions:
                            self.logger.debug(f"除外拡張子: {file_path}")
                            continue
                        
                        try:
                            # ファイル情報取得
                            stat_info = os.stat(file_path)
                            file_size = stat_info.st_size
                            
                            # ファイルサイズチェック
                            if file_size > max_file_size:
                                self.logger.warning(f"ファイルサイズが制限を超えています: {file_path} ({file_size} bytes)")
                                continue
                            
                            file_info = {
                                'path': file_path,
                                'size': file_size,
                                'modified_time': datetime.datetime.fromtimestamp(stat_info.st_mtime),
                                'directory': directory
                            }
                            
                            files.append(file_info)
                            
                        except OSError as e:
                            self.logger.warning(f"ファイル情報取得エラー: {file_path} - {str(e)}")
                            continue
                        
            except Exception as e:
                self.logger.error(f"ディレクトリ処理エラー: {directory} - {str(e)}")
                continue
        
        self.logger.info(f"収集ファイル数: {len(files)}")
        return files
        
    def archive_to_s3(self, files: List[Dict]) -> List[Dict]:
        """S3アップロード処理"""
        self.logger.info("S3アップロード開始")
        
        # TODO: S3アップロード処理の実装
        # - boto3を使用したS3アップロード
        # - Glacier Deep Archive指定
        # - VPCエンドポイント経由でのアップロード
        # - 進捗管理
        
        results = []
        for file_info in files:
            # 仮の処理結果
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
        """アーカイブ後処理（元ファイル削除・空ファイル作成）"""
        self.logger.info("アーカイブ後処理開始")
        
        # TODO: アーカイブ後処理の実装
        # - 元ファイルの削除
        # - _archived.txtファイルの作成
        # - 処理結果の記録
        
        processed_results = results  # 仮の戻り値
        self.logger.info("アーカイブ後処理完了")
        return processed_results
        
    def save_to_database(self, results: List[Dict]) -> None:
        """データベース登録処理"""
        self.logger.info("データベース登録開始")
        
        # TODO: データベース登録処理の実装
        # - PostgreSQLへの接続
        # - archive_historyテーブルへの登録
        # - トランザクション管理
        
        self.logger.info("データベース登録完了")
        
    def generate_error_csv(self, failed_items: List[Dict], original_csv_path: str) -> str:
        """エラーCSVファイルの生成"""
        if not failed_items:
            return None
            
        self.logger.info("エラーCSVファイル生成開始")
        
        # TODO: エラーCSV生成処理の実装
        # - 元CSVと同じ場所にエラーCSVを出力
        # - 再試行を考慮した命名規則
        # - エラー理由の記録
        
        error_csv_path = "error_output.csv"  # 仮のパス
        self.logger.info(f"エラーCSVファイル生成完了: {error_csv_path}")
        return error_csv_path
        
    def print_statistics(self) -> None:
        """処理統計の表示"""
        elapsed_time = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info("=== 処理統計 ===")
        self.logger.info(f"処理時間: {elapsed_time}")
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
            if not directories:
                self.logger.error("処理対象のディレクトリが見つかりません")
                return 1
                
            # 2. ファイル収集
            files = self.collect_files(directories)
            if not files:
                self.logger.warning("処理対象のファイルが見つかりません")
                # ファイルが見つからない場合でも正常終了とする
                return 0
                
            self.stats['total_files'] = len(files)
            self.stats['total_size'] = sum(f['size'] for f in files)
            
            # 3. S3アップロード
            upload_results = self.archive_to_s3(files)
            
            # 4. アーカイブ後処理
            processed_results = self.create_archived_files(upload_results)
            
            # 5. データベース登録
            self.save_to_database(processed_results)
            
            # 6. エラー処理
            failed_items = [r for r in processed_results if not r.get('success', False)]
            if failed_items:
                error_csv_path = self.generate_error_csv(failed_items, csv_path)
                self.logger.warning(f"エラーが発生したファイルがあります: {error_csv_path}")
                
            self.stats['processed_files'] = len([r for r in processed_results if r.get('success', False)])
            self.stats['failed_files'] = len(failed_items)
            
            self.logger.info("アーカイブ処理完了")
            return 0
            
        except Exception as e:
            self.logger.error(f"アーカイブ処理中にエラーが発生しました: {str(e)}", exc_info=True)
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
    parser.add_argument('--debug', action='store_true', help='デバッグモードで実行')
    
    args = parser.parse_args()
    
    # デバッグモードの場合、ログレベルを変更
    if args.debug:
        logging.getLogger('archive_processor').setLevel(logging.DEBUG)
    
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