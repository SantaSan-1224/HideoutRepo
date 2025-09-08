#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
復元スクリプト（ディレクトリ・ファイル混合対応版）- PostgreSQLエスケープ対応
AWS S3 Glacier Deep Archiveからファイルサーバへの復元処理
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

class RestoreProcessor:
    """復元処理のメインクラス"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()
        self.csv_errors = []  # CSV検証エラーを記録
        self.stats = {
            'total_requests': 0,
            'directory_requests': 0,
            'file_requests': 0,
            'total_files_found': 0,
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
                "restore_tier": "Standard",  # Standard, Expedited, Bulk
                "download_retry_count": 3,
                "skip_existing_files": True,
                "temp_download_directory": "temp_downloads"
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
        復元依頼CSV読み込み・検証処理（ディレクトリ・ファイル混合対応）
        
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
                if i == 0 and ('復元対象' in clean_line or 'S3パス' in clean_line or 
                               'restore' in clean_line.lower() or 'path' in clean_line.lower()):
                    self.logger.info(f"行 {line_num}: ヘッダー行をスキップ")
                    continue
                
                # CSV行を分割
                try:
                    row = next(csv.reader([clean_line]))
                    
                    # 2列または3列の形式に対応
                    if len(row) == 2:
                        # 従来形式: 復元対象パス, 復元先ディレクトリ
                        restore_path = row[0].strip()
                        restore_dir = row[1].strip()
                        # 自動判定: パスがディレクトリ区切り文字で終わればディレクトリモード
                        if restore_path.endswith(('\\', '/')):
                            restore_mode = 'directory'
                        else:
                            restore_mode = 'file'
                    elif len(row) == 3:
                        # 新形式: 復元対象パス, 復元先ディレクトリ, 復元モード
                        restore_path = row[0].strip()
                        restore_dir = row[1].strip()
                        restore_mode = row[2].strip().lower()
                    else:
                        error_item = {
                            'line_number': line_num,
                            'content': clean_line,
                            'error_reason': 'カラム数が不正です（2列または3列が必要）',
                            'original_line': line.rstrip()
                        }
                        self.csv_errors.append(error_item)
                        self.logger.error(f"行 {line_num}: カラム数不正 ✗")
                        continue
                    
                    # 復元モードの検証
                    if restore_mode not in ['file', 'directory']:
                        error_item = {
                            'line_number': line_num,
                            'content': clean_line,
                            'error_reason': f'復元モードが不正です: {restore_mode} (file または directory が必要)',
                            'original_line': line.rstrip()
                        }
                        self.csv_errors.append(error_item)
                        self.logger.error(f"行 {line_num}: 復元モード不正 ✗")
                        continue
                    
                    # 復元依頼の検証
                    validation_result = self._validate_restore_request(restore_path, restore_dir, restore_mode)
                    
                    if validation_result['valid']:
                        request_item = {
                            'line_number': line_num,
                            'restore_path': restore_path,
                            'restore_directory': restore_dir,
                            'restore_mode': restore_mode,
                            'files_found': [],  # データベース検索で設定
                            'total_files_found': 0
                        }
                        valid_requests.append(request_item)
                        self.logger.info(f"行 {line_num}: 有効な復元依頼追加 ({restore_mode}モード) ✓")
                    else:
                        error_item = {
                            'line_number': line_num,
                            'content': f"{restore_path} -> {restore_dir} ({restore_mode})",
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
        
        # 統計更新
        directory_count = len([r for r in valid_requests if r['restore_mode'] == 'directory'])
        file_count = len([r for r in valid_requests if r['restore_mode'] == 'file'])
        
        self.logger.info(f"CSV読み込み完了")
        self.logger.info(f"  - 有効な復元依頼数: {len(valid_requests)}")
        self.logger.info(f"    - ディレクトリ復元: {directory_count}件")
        self.logger.info(f"    - ファイル復元: {file_count}件")
        self.logger.info(f"  - エラー項目数: {len(self.csv_errors)}")
        
        self.stats['total_requests'] = len(valid_requests)
        self.stats['directory_requests'] = directory_count
        self.stats['file_requests'] = file_count
        
        return valid_requests, self.csv_errors
    
    def _validate_restore_request(self, restore_path: str, restore_dir: str, restore_mode: str) -> Dict:
        """復元依頼の検証"""
        try:
            # 復元対象パスの形式チェック
            if not restore_path:
                return {'valid': False, 'error_reason': '復元対象パスが指定されていません'}
            
            # パス長制限チェック
            if len(restore_path) > 260:
                return {'valid': False, 'error_reason': '復元対象パスが長すぎます（260文字制限）'}
            
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
            
            # ディレクトリモードの場合の特別なチェック
            if restore_mode == 'directory':
                # ディレクトリパスが適切に終わっているかチェック
                if not restore_path.endswith(('\\', '/')):
                    # 自動的に区切り文字を追加（警告ログ出力）
                    self.logger.warning(f"ディレクトリパスに区切り文字を追加: {restore_path} -> {restore_path}\\")
            
            return {'valid': True, 'error_reason': None}
            
        except Exception as e:
            return {'valid': False, 'error_reason': f'検証エラー: {str(e)}'}
    
    def lookup_files_from_database(self, restore_requests: List[Dict]) -> List[Dict]:
        """データベースから復元対象ファイルを検索（PostgreSQLエスケープ対応版）"""
        self.logger.info("データベースからファイル検索開始")
        
        try:
            # データベース接続
            conn = self._connect_database()
            
            with conn:
                with conn.cursor() as cursor:
                    for request in restore_requests:
                        restore_path = request['restore_path']
                        restore_mode = request['restore_mode']
                        
                        self.logger.info(f"検索開始: {restore_path} ({restore_mode}モード)")
                        
                        if restore_mode == 'directory':
                            # ディレクトリ復元: 複数パターンでLIKE検索
                            search_patterns = self._generate_search_patterns(restore_path)
                            self.logger.info(f"生成された検索パターン数: {len(search_patterns)}")
                            
                            found_files = []
                            for i, pattern in enumerate(search_patterns, 1):
                                self.logger.info(f"検索パターン {i}: {pattern}")
                                
                                try:
                                    # 標準的なLIKE検索
                                    cursor.execute(
                                        "SELECT original_file_path, s3_path, archive_date, file_size FROM archive_history WHERE original_file_path LIKE %s ORDER BY original_file_path",
                                        (pattern,)
                                    )
                                    
                                    results = cursor.fetchall()
                                    self.logger.info(f"パターン {i} 結果: {len(results)}件")
                                    
                                    if results:
                                        self.logger.info(f"✓ パターン '{pattern}' で {len(results)}件発見")
                                        # 最初の3件のパスを表示
                                        for j, row in enumerate(results[:3]):
                                            self.logger.info(f"  発見 {j+1}: {row[0]}")
                                        if len(results) > 3:
                                            self.logger.info(f"  ... 他 {len(results)-3}件")
                                        found_files.extend(results)
                                        break  # 成功したらループを抜ける
                                    else:
                                        self.logger.debug(f"パターン '{pattern}' では見つからず")
                                        
                                except Exception as e:
                                    self.logger.warning(f"パターン {i} でエラー: {str(e)}")
                                    continue
                            
                            # パターン検索で見つからない場合の代替検索
                            if not found_files:
                                self.logger.info("代替検索を実行中...")
                                
                                # ディレクトリ名部分を抽出
                                path_parts = restore_path.replace('/', '\\').split('\\')
                                dir_name = None
                                for part in reversed(path_parts):
                                    if part.strip():
                                        dir_name = part
                                        break
                                
                                if dir_name:
                                    alternative_patterns = [
                                        f"%{dir_name}%",  # ディレクトリ名部分のみ
                                        f"%{dir_name}\\%"  # ディレクトリ名+区切り文字
                                    ]
                                    
                                    for alt_pattern in alternative_patterns:
                                        self.logger.info(f"代替パターン: {alt_pattern}")
                                        try:
                                            cursor.execute(
                                                "SELECT original_file_path, s3_path, archive_date, file_size FROM archive_history WHERE original_file_path LIKE %s ORDER BY original_file_path",
                                                (alt_pattern,)
                                            )
                                            
                                            results = cursor.fetchall()
                                            if results:
                                                # 元のパスと関連があるかチェック
                                                filtered_results = []
                                                for row in results:
                                                    original_path = row[0]
                                                    # より厳密なフィルタリング（サーバー名も一致するか）
                                                    if restore_path.split('\\')[0] in original_path:
                                                        filtered_results.append(row)
                                                
                                                if filtered_results:
                                                    self.logger.info(f"✓ 代替パターンで {len(filtered_results)}件発見")
                                                    found_files.extend(filtered_results)
                                                    break
                                        except Exception as e:
                                            self.logger.warning(f"代替パターンでエラー: {str(e)}")
                            
                            # 重複除去
                            if found_files:
                                unique_files = []
                                seen_paths = set()
                                for row in found_files:
                                    if row[0] not in seen_paths:
                                        unique_files.append(row)
                                        seen_paths.add(row[0])
                                results = unique_files
                                self.logger.info(f"重複除去後: {len(results)}件")
                            else:
                                results = []
                                
                        else:
                            # ファイル復元: 完全一致検索
                            self.logger.info(f"ファイル検索: {restore_path}")
                            
                            cursor.execute(
                                "SELECT original_file_path, s3_path, archive_date, file_size FROM archive_history WHERE original_file_path = %s",
                                (restore_path,)
                            )
                            results = cursor.fetchall()
                            self.logger.info(f"ファイル検索結果: {len(results)}件")
                        
                        if results:
                            files_found = []
                            for row in results:
                                original_path, s3_path, archive_date, file_size = row
                                
                                file_info = {
                                    'original_file_path': original_path,
                                    's3_path': s3_path,
                                    'bucket': self._extract_bucket_from_s3_path(s3_path),
                                    'key': self._extract_key_from_s3_path(s3_path),
                                    'archive_date': str(archive_date),
                                    'file_size': file_size,
                                    'restore_status': 'pending',
                                    'relative_path': self._calculate_relative_path(original_path, restore_path, restore_mode)
                                }
                                files_found.append(file_info)
                            
                            request['files_found'] = files_found
                            request['total_files_found'] = len(files_found)
                            
                            self.logger.info(f"✓ ファイル検索完了: {restore_path} -> {len(files_found)}件")
                            
                            # 検出ファイルの例を表示（最初の3件のみ）
                            for i, file_info in enumerate(files_found[:3]):
                                self.logger.info(f"  検出ファイル {i+1}: {file_info['original_file_path']}")
                            if len(files_found) > 3:
                                self.logger.info(f"  ... 他 {len(files_found)-3}件")
                            
                        else:
                            request['files_found'] = []
                            request['total_files_found'] = 0
                            request['error'] = 'データベースにアーカイブ履歴が見つかりません'
                            self.logger.error(f"ファイル見つからず: {restore_path}")
                            
                            # デバッグ情報
                            self.logger.info("=== 検索失敗時のデバッグ情報 ===")
                            try:
                                # ディレクトリ名での検索結果を表示
                                path_parts = restore_path.replace('/', '\\').split('\\')
                                dir_name = None
                                for part in reversed(path_parts):
                                    if part.strip():
                                        dir_name = part
                                        break
                                
                                if dir_name:
                                    cursor.execute("SELECT COUNT(*) FROM archive_history WHERE original_file_path LIKE %s", (f"%{dir_name}%",))
                                    related_count = cursor.fetchone()[0]
                                    self.logger.info(f"関連ファイル数（'{dir_name}'を含む）: {related_count}")
                            except Exception:
                                pass
                
                # 統計更新
                total_files = sum(req.get('total_files_found', 0) for req in restore_requests)
                self.stats['total_files_found'] = total_files
                
                self.logger.info("データベースファイル検索完了")
                self.logger.info(f"  - 総検出ファイル数: {total_files}件")
                
                return restore_requests
                
        except Exception as e:
            self.logger.error(f"データベース検索エラー: {str(e)}")
            # エラー時は全リクエストにエラーマーク
            for request in restore_requests:
                request['files_found'] = []
                request['total_files_found'] = 0
                request['error'] = f'データベース接続エラー: {str(e)}'
            return restore_requests
        
        finally:
            try:
                if 'conn' in locals():
                    conn.close()
            except Exception:
                pass

    def _generate_search_patterns(self, restore_path: str) -> List[str]:
        """ディレクトリ検索用の複数検索パターンを生成（PostgreSQLエスケープ対応）"""
        patterns = []
        
        # パスの正規化
        normalized_path = restore_path.replace('/', '\\')
        
        # PostgreSQLのLIKE検索でバックスラッシュをエスケープ
        # \\ -> \\\\（PostgreSQLでは\が特殊文字なので二重エスケープが必要）
        escaped_path = normalized_path.replace('\\', '\\\\')
        
        # パターン1: 末尾に\がある場合
        if normalized_path.endswith('\\'):
            patterns.append(f"{escaped_path}%")
            # 念のため、末尾の\を除去した版も追加
            patterns.append(f"{escaped_path[:-2]}\\\\%")  # 最後の\\\\を除去して\\\\%を追加
        else:
            # パターン2: 末尾に\がない場合
            patterns.append(f"{escaped_path}\\\\%")
            patterns.append(f"{escaped_path}%")
        
        # 追加パターン: 異なるエスケープ方法
        # 単一エスケープ版も試す
        single_escaped = normalized_path.replace('\\', '\\')  # そのまま
        if single_escaped.endswith('\\'):
            patterns.append(f"{single_escaped}%")
        else:
            patterns.append(f"{single_escaped}\\%")
        
        return patterns

    def _calculate_relative_path(self, original_path: str, restore_path: str, restore_mode: str) -> str:
        """復元時の相対パスを計算（階層構造保持用）"""
        try:
            if restore_mode == 'file':
                # ファイル復元の場合はファイル名のみ
                return os.path.basename(original_path)
            else:
                # ディレクトリ復元の場合は相対パスを計算
                # 例: original_path = "\\server\share\project\sub\file.txt"
                #     restore_path = "\\server\share\project\"
                #     -> relative_path = "sub\file.txt"
                
                # パスの正規化
                orig_normalized = original_path.replace('/', '\\')
                restore_normalized = restore_path.replace('/', '\\')
                
                if not restore_normalized.endswith('\\'):
                    restore_normalized += '\\'
                
                if orig_normalized.startswith(restore_normalized):
                    relative = orig_normalized[len(restore_normalized):]
                    return relative if relative else os.path.basename(original_path)
                else:
                    # パスが一致しない場合はファイル名のみ
                    return os.path.basename(original_path)
                    
        except Exception as e:
            self.logger.warning(f"相対パス計算エラー: {e}")
            return os.path.basename(original_path)
    
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
        
        # ファイルが見つかったリクエストのみ処理
        valid_requests = [req for req in restore_requests if req.get('total_files_found', 0) > 0]
        
        if not valid_requests:
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
            
            successful_requests = 0
            failed_requests = 0
            
            for request in valid_requests:
                self.logger.info(f"復元リクエスト処理中: {request['restore_path']} ({request['total_files_found']}件)")
                
                for file_info in request['files_found']:
                    bucket = file_info['bucket']
                    key = file_info['key']
                    original_path = file_info['original_file_path']
                    
                    try:
                        # S3復元リクエスト送信
                        self.logger.debug(f"復元リクエスト送信中: {bucket}/{key}")
                        
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
                        file_info['restore_status'] = 'requested'
                        file_info['restore_request_time'] = datetime.datetime.now().isoformat()
                        file_info['restore_tier'] = restore_tier
                        successful_requests += 1
                        self.logger.debug(f"✓ 復元リクエスト送信成功: {original_path}")
                        
                    except Exception as e:
                        error_msg = str(e)
                        
                        # 既に復元中の場合は正常として扱う
                        if 'RestoreAlreadyInProgress' in error_msg:
                            file_info['restore_status'] = 'already_in_progress'
                            file_info['restore_request_time'] = datetime.datetime.now().isoformat()
                            successful_requests += 1
                            self.logger.debug(f"✓ 復元リクエスト既に進行中: {original_path}")
                        else:
                            file_info['restore_status'] = 'failed'
                            file_info['error'] = error_msg
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
                for file_info in request.get('files_found', []):
                    file_info['restore_status'] = 'failed'
                    file_info['error'] = f'S3初期化エラー: {str(e)}'
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
        
        # 復元リクエスト済みのファイルを収集
        pending_files = []
        for request in restore_requests:
            for file_info in request.get('files_found', []):
                if file_info.get('restore_status') in ['requested', 'already_in_progress']:
                    pending_files.append(file_info)
        
        if not pending_files:
            self.logger.info("復元確認対象ファイルがありません")
            return restore_requests
        
        self.logger.info(f"復元ステータス確認対象: {len(pending_files)}件")
        
        try:
            # S3クライアント初期化
            s3_client = self._initialize_s3_client()
            
            completed_count = 0
            still_pending_count = 0
            failed_count = 0
            
            for file_info in pending_files:
                bucket = file_info['bucket']
                key = file_info['key']
                original_path = file_info['original_file_path']
                
                try:
                    # S3オブジェクトのメタデータを取得してrestoreステータスを確認
                    self.logger.debug(f"復元ステータス確認中: {bucket}/{key}")
                    
                    response = s3_client.head_object(Bucket=bucket, Key=key)
                    
                    # Restoreヘッダーの確認
                    restore_header = response.get('Restore')
                    
                    if restore_header is None:
                        # Restoreヘッダーがない = まだ復元リクエストが処理されていない
                        file_info['restore_status'] = 'pending'
                        file_info['restore_check_time'] = datetime.datetime.now().isoformat()
                        still_pending_count += 1
                        
                    elif 'ongoing-request="true"' in restore_header:
                        # 復元処理が進行中
                        file_info['restore_status'] = 'in_progress'
                        file_info['restore_check_time'] = datetime.datetime.now().isoformat()
                        still_pending_count += 1
                        
                    elif 'ongoing-request="false"' in restore_header:
                        # 復元完了
                        file_info['restore_status'] = 'completed'
                        file_info['restore_completed_time'] = datetime.datetime.now().isoformat()
                        file_info['restore_check_time'] = datetime.datetime.now().isoformat()
                        completed_count += 1
                        self.logger.debug(f"✓ 復元完了: {original_path}")
                        
                        # 復元有効期限の抽出（可能であれば）
                        try:
                            # 例: 'ongoing-request="false", expiry-date="Fri, 21 Dec 2012 00:00:00 GMT"'
                            if 'expiry-date=' in restore_header:
                                expiry_part = restore_header.split('expiry-date=')[1]
                                expiry_date = expiry_part.split('"')[1]
                                file_info['restore_expiry'] = expiry_date
                        except Exception:
                            pass  # 有効期限の抽出に失敗しても処理継続
                            
                    else:
                        # 不明なステータス
                        file_info['restore_status'] = 'unknown'
                        file_info['restore_check_time'] = datetime.datetime.now().isoformat()
                        file_info['error'] = f"不明な復元ステータス: {restore_header}"
                        still_pending_count += 1
                        self.logger.warning(f"不明な復元ステータス: {original_path} - {restore_header}")
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # 特定のエラーハンドリング
                    if 'NoSuchKey' in error_msg:
                        file_info['restore_status'] = 'failed'
                        file_info['error'] = 'S3にファイルが見つかりません'
                    elif 'InvalidObjectState' in error_msg:
                        file_info['restore_status'] = 'failed'
                        file_info['error'] = 'オブジェクトがGlacierストレージクラスではありません'
                    else:
                        file_info['restore_status'] = 'check_failed'
                        file_info['error'] = f'復元ステータス確認エラー: {error_msg}'
                    
                    file_info['restore_check_time'] = datetime.datetime.now().isoformat()
                    failed_count += 1
                    self.logger.error(f"✗ 復元ステータス確認失敗: {original_path} - {error_msg}")
            
            # 統計更新
            self.stats['restore_completed'] = completed_count
            
            self.logger.info("復元完了確認完了")
            self.logger.info(f"  - 復元完了: {completed_count}件")
            self.logger.info(f"  - 処理中: {still_pending_count}件")
            self.logger.info(f"  - 確認失敗: {failed_count}件")
            
            return restore_requests
            
        except Exception as e:
            self.logger.error(f"復元完了確認処理でエラーが発生: {str(e)}")
            # 全リクエストにエラーマーク
            for request in restore_requests:
                for file_info in request.get('files_found', []):
                    if file_info.get('restore_status') in ['requested', 'already_in_progress']:
                        file_info['restore_status'] = 'check_failed'
                        file_info['error'] = f'S3接続エラー: {str(e)}'
                        file_info['restore_check_time'] = datetime.datetime.now().isoformat()
            return restore_requests
        
    def download_and_place_files(self, restore_requests: List[Dict]) -> List[Dict]:
        """ファイルダウンロード・配置処理（階層構造保持対応）"""
        self.logger.info("ファイルダウンロード・配置開始")
        
        # 復元完了ファイルを収集
        completed_files = []
        for request in restore_requests:
            for file_info in request.get('files_found', []):
                if file_info.get('restore_status') == 'completed':
                    file_info['restore_directory'] = request['restore_directory']
                    file_info['restore_mode'] = request['restore_mode']
                    completed_files.append(file_info)
        
        if not completed_files:
            self.logger.info("ダウンロード対象ファイルがありません")
            return restore_requests
        
        self.logger.info(f"ダウンロード対象ファイル数: {len(completed_files)}件")
        
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
            
            for i, file_info in enumerate(completed_files, 1):
                bucket = file_info['bucket']
                key = file_info['key']
                original_path = file_info['original_file_path']
                restore_dir = file_info['restore_directory']
                relative_path = file_info['relative_path']
                restore_mode = file_info['restore_mode']
                
                # 進捗ログ
                self.logger.info(f"[{i}/{len(completed_files)}] ダウンロード処理中: {original_path}")
                
                # 復元先ファイルパスの生成（階層構造保持）
                if restore_mode == 'directory':
                    # ディレクトリ復元: 相対パスを使用して階層構造を保持
                    destination_path = os.path.join(restore_dir, relative_path)
                else:
                    # ファイル復元: ファイル名のみ
                    filename = os.path.basename(original_path)
                    destination_path = os.path.join(restore_dir, filename)
                
                # 配置先ディレクトリの作成（階層構造用）
                destination_dir = os.path.dirname(destination_path)
                try:
                    os.makedirs(destination_dir, exist_ok=True)
                except Exception as e:
                    self.logger.error(f"✗ 配置先ディレクトリ作成失敗: {destination_dir} - {e}")
                    file_info['download_status'] = 'failed'
                    file_info['download_error'] = f'ディレクトリ作成失敗: {str(e)}'
                    failed_downloads += 1
                    continue
                
                # 同名ファイルの存在チェック
                if skip_existing and os.path.exists(destination_path):
                    self.logger.info(f"同名ファイルが存在するためスキップ: {destination_path}")
                    file_info['download_status'] = 'skipped'
                    file_info['download_error'] = '同名ファイルが既に存在します'
                    file_info['destination_path'] = destination_path
                    skipped_files += 1
                    continue
                
                # 一時ファイルパスの生成
                temp_filename = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(original_path)}"
                temp_file_path = temp_path / temp_filename
                
                # S3からダウンロード（リトライ付）
                download_result = self._download_file_with_retry(
                    s3_client, bucket, key, str(temp_file_path), retry_count
                )
                
                if not download_result['success']:
                    self.logger.error(f"✗ ダウンロード失敗: {original_path} - {download_result['error']}")
                    file_info['download_status'] = 'failed'
                    file_info['download_error'] = download_result['error']
                    failed_downloads += 1
                    continue
                
                # ファイルサイズ確認
                try:
                    downloaded_size = os.path.getsize(temp_file_path)
                    file_info['downloaded_size'] = downloaded_size
                except Exception as e:
                    self.logger.warning(f"ダウンロードサイズ確認エラー: {e}")
                
                # 最終配置（一時ファイル → 復元先）
                placement_result = self._place_file_to_destination(
                    str(temp_file_path), destination_path
                )
                
                if placement_result['success']:
                    # 成功
                    file_info['download_status'] = 'completed'
                    file_info['destination_path'] = destination_path
                    file_info['download_completed_time'] = datetime.datetime.now().isoformat()
                    successful_downloads += 1
                    self.logger.info(f"✓ ダウンロード完了: {original_path} -> {destination_path}")
                    
                else:
                    # 配置失敗
                    self.logger.error(f"✗ ファイル配置失敗: {original_path} - {placement_result['error']}")
                    file_info['download_status'] = 'failed'
                    file_info['download_error'] = f"ファイル配置失敗: {placement_result['error']}"
                    failed_downloads += 1
                    
                # 一時ファイルのクリーンアップ
                try:
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)
                except Exception as e:
                    self.logger.debug(f"一時ファイル削除エラー: {e}")
            
            self.logger.info("ファイルダウンロード・配置完了")
            self.logger.info(f"  - 成功: {successful_downloads}件")
            self.logger.info(f"  - スキップ: {skipped_files}件")
            self.logger.info(f"  - 失敗: {failed_downloads}件")
            
            return restore_requests
            
        except Exception as e:
            self.logger.error(f"ダウンロード・配置処理でエラーが発生: {str(e)}")
            return restore_requests

    def _download_file_with_retry(self, s3_client, bucket: str, key: str, 
                                 local_path: str, max_retries: int) -> Dict:
        """S3からファイルダウンロード（リトライ付）"""
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"ダウンロード試行 {attempt + 1}/{max_retries}: s3://{bucket}/{key}")
                
                # S3からダウンロード
                s3_client.download_file(bucket, key, local_path)
                
                # ダウンロード成功確認（0バイトファイルも成功として扱う）
                if os.path.exists(local_path):
                    file_size = os.path.getsize(local_path)
                    return {'success': True, 'error': None, 'file_size': file_size}
                else:
                    raise Exception("ダウンロード後にファイルが作成されませんでした")
                    
            except Exception as e:
                error_msg = str(e)
                
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

    def generate_failed_files_retry_csv(self, restore_requests: List[Dict], original_csv_path: str) -> Optional[str]:
        """失敗ファイル用のリトライCSV生成"""
        failed_files = []
        
        # 失敗したファイルを収集
        for request in restore_requests:
            for file_info in request.get('files_found', []):
                if (file_info.get('restore_status') == 'failed' or 
                    file_info.get('download_status') == 'failed'):
                    failed_files.append({
                        'original_path': file_info['original_file_path'],
                        'restore_directory': request['restore_directory'],
                        'error_stage': self._determine_error_stage(file_info),
                        'error_reason': file_info.get('error', file_info.get('download_error', '不明なエラー'))
                    })
        
        if not failed_files:
            return None
            
        self.logger.info("失敗ファイル用リトライCSV生成開始")
        
        try:
            # logsディレクトリにリトライCSVを出力
            log_config = self.config.get('logging', {})
            log_dir = Path(log_config.get('log_directory', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            # ファイル名生成
            original_path = Path(original_csv_path)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            retry_csv_path = log_dir / f"{original_path.stem}_failed_retry_{timestamp}.csv"
            
            # リトライCSVの生成
            with open(retry_csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                fieldnames = ['復元対象パス', '復元先ディレクトリ', '復元モード', 'エラー段階', 'エラー理由']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for item in failed_files:
                    writer.writerow({
                        '復元対象パス': item['original_path'],
                        '復元先ディレクトリ': item['restore_directory'],
                        '復元モード': 'file',  # 失敗分は個別ファイルとして再試行
                        'エラー段階': item['error_stage'],
                        'エラー理由': item['error_reason']
                    })
            
            self.logger.info(f"失敗ファイル用リトライCSV生成完了: {retry_csv_path}")
            self.logger.info(f"リトライ対象ファイル数: {len(failed_files)}")
            
            return str(retry_csv_path)
            
        except Exception as e:
            self.logger.error(f"失敗ファイル用リトライCSV生成失敗: {str(e)}")
            return None

    def _determine_error_stage(self, file_info: Dict) -> str:
        """エラー段階の判定"""
        if file_info.get('restore_status') == 'failed':
            return 'restore_request'
        elif file_info.get('download_status') == 'failed':
            return 'download'
        else:
            return 'unknown'
        
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
        
    def print_statistics(self) -> None:
        """処理統計の表示"""
        elapsed_time = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info("=== 復元処理統計 ===")
        self.logger.info(f"処理時間: {elapsed_time}")
        self.logger.info(f"CSV検証エラー数: {len(self.csv_errors)}")
        self.logger.info(f"総復元依頼数: {self.stats['total_requests']}")
        self.logger.info(f"  - ディレクトリ復元: {self.stats['directory_requests']}件")
        self.logger.info(f"  - ファイル復元: {self.stats['file_requests']}件")
        self.logger.info(f"検出ファイル数: {self.stats['total_files_found']}")
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
        
        # 2. データベースからファイル検索
        restore_requests = self.lookup_files_from_database(restore_requests)
        
        # ファイルが見つからない依頼をフィルタリング
        valid_restore_requests = [req for req in restore_requests if req.get('total_files_found', 0) > 0]
        failed_requests = [req for req in restore_requests if req.get('total_files_found', 0) == 0]
        
        if failed_requests:
            self.logger.warning(f"ファイルが見つからない依頼: {len(failed_requests)}件")
            for req in failed_requests:
                self.logger.warning(f"  - {req['restore_path']}: {req.get('error', '不明')}")
        
        if not valid_restore_requests:
            self.logger.error("復元可能なファイルが見つかりません")
            return 1
        
        # 3. S3復元リクエスト送信
        restore_requests = self.request_restore(valid_restore_requests + failed_requests)
        
        # 4. ステータスファイル保存
        self._save_restore_status(restore_requests)
        
        self.logger.info(f"復元リクエスト送信完了 - {self.stats['restore_requested']}件")
        if failed_requests:
            self.logger.warning(f"復元不可依頼 - {len(failed_requests)}件")
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
        
        # 統計更新
        total_files = sum(req.get('total_files_found', 0) for req in restore_requests)
        self.stats['total_files_found'] = total_files
        
        # 2. 復元完了確認（最新ステータス取得）
        self.logger.info("復元ステータスを確認しています...")
        restore_requests = self.check_restore_completion(restore_requests)
        
        # 3. 復元完了ファイルの確認
        completed_files = []
        for request in restore_requests:
            for file_info in request.get('files_found', []):
                if file_info.get('restore_status') == 'completed':
                    completed_files.append(file_info)
        
        if not completed_files:
            self.logger.info("復元完了ファイルがありません")
            pending_files = []
            for request in restore_requests:
                for file_info in request.get('files_found', []):
                    if file_info.get('restore_status') in ['pending', 'in_progress']:
                        pending_files.append(file_info)
            
            if pending_files:
                self.logger.info(f"復元処理中のファイル: {len(pending_files)}件")
                self.logger.info("しばらく待ってから再度実行してください")
            return 0
        
        self.logger.info(f"復元完了ファイル: {len(completed_files)}件をダウンロードします")
        
        # 4. ファイルダウンロード・配置
        restore_requests = self.download_and_place_files(restore_requests)
        
        # 5. ステータスファイル更新
        self._save_restore_status(restore_requests)
        
        # 6. 失敗ファイル用リトライCSV生成
        retry_csv_path = self.generate_failed_files_retry_csv(restore_requests, csv_path)
        if retry_csv_path:
            self.logger.warning(f"失敗ファイルがあります。リトライCSV: {retry_csv_path}")
        
        # 結果サマリー
        downloaded_count = 0
        skipped_count = 0
        for request in restore_requests:
            for file_info in request.get('files_found', []):
                if file_info.get('download_status') == 'completed':
                    downloaded_count += 1
                elif file_info.get('download_status') == 'skipped':
                    skipped_count += 1
        
        self.logger.info(f"ダウンロード処理完了")
        self.logger.info(f"  - ダウンロード成功: {downloaded_count}件")
        self.logger.info(f"  - スキップ: {skipped_count}件")
        
        # 未完了ファイルがある場合の案内
        remaining_count = 0
        for request in restore_requests:
            for file_info in request.get('files_found', []):
                if file_info.get('restore_status') in ['pending', 'in_progress']:
                    remaining_count += 1
        
        if remaining_count > 0:
            self.logger.info(f"復元処理中のファイル: {remaining_count}件")
            self.logger.info("復元完了後に再度ダウンロード処理を実行してください")
        
        return 0


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='ファイル復元処理（ディレクトリ・ファイル混合対応）')
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