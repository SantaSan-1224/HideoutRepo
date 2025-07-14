#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
デバッグ用テストスクリプト
個別のパス検証を行う
"""

import os
import sys
import logging

def setup_logger():
    """ログ設定"""
    logger = logging.getLogger('debug_test')
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def test_path_validation(path: str, logger):
    """パス検証のテスト"""
    logger.info(f"=== パス検証テスト開始 ===")
    logger.info(f"テスト対象パス: '{path}'")
    logger.info(f"パス長: {len(path)} 文字")
    logger.info(f"パス文字コード: {[ord(c) for c in path[:50]]}...")
    
    # 基本的な存在チェック
    try:
        exists = os.path.exists(path)
        logger.info(f"os.path.exists(): {exists}")
        
        if exists:
            is_dir = os.path.isdir(path)
            logger.info(f"os.path.isdir(): {is_dir}")
            
            can_read = os.access(path, os.R_OK)
            logger.info(f"読み取り権限: {can_read}")
            
            # ディレクトリ内容の確認
            try:
                items = os.listdir(path)
                logger.info(f"ディレクトリ内アイテム数: {len(items)}")
                if items:
                    logger.info(f"最初の数個のアイテム: {items[:5]}")
            except Exception as e:
                logger.error(f"ディレクトリ内容取得エラー: {e}")
        
        # UNCパスの場合の追加チェック
        if path.startswith('\\\\'):
            parts = path.split('\\')
            server_name = parts[2] if len(parts) > 2 else ""
            share_name = parts[3] if len(parts) > 3 else ""
            
            logger.info(f"UNCパス情報:")
            logger.info(f"  サーバー名: '{server_name}'")
            logger.info(f"  共有名: '{share_name}'")
            
            # サーバーへのping
            if server_name:
                try:
                    import subprocess
                    ping_result = subprocess.run(['ping', '-n', '1', server_name], 
                                               capture_output=True, text=True, timeout=10)
                    if ping_result.returncode == 0:
                        logger.info(f"サーバー '{server_name}' への ping 成功")
                    else:
                        logger.warning(f"サーバー '{server_name}' への ping 失敗")
                        logger.warning(f"ping stdout: {ping_result.stdout}")
                        logger.warning(f"ping stderr: {ping_result.stderr}")
                except Exception as e:
                    logger.error(f"ping実行エラー: {e}")
    
    except Exception as e:
        logger.error(f"パス検証中にエラー: {e}")
    
    logger.info(f"=== パス検証テスト終了 ===")

def test_csv_reading(csv_path: str, logger):
    """CSV読み込みのテスト"""
    logger.info(f"=== CSV読み込みテスト開始 ===")
    logger.info(f"CSVファイル: '{csv_path}'")
    
    # ファイルの存在確認
    if not os.path.exists(csv_path):
        logger.error(f"CSVファイルが存在しません: {csv_path}")
        return
    
    # 複数のエンコーディングでテスト
    encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932', 'euc-jp']
    
    for encoding in encodings:
        try:
            logger.info(f"エンコーディング '{encoding}' で読み込み試行...")
            
            with open(csv_path, 'r', encoding=encoding) as f:
                content = f.read()
                logger.info(f"読み込み成功 - 内容長: {len(content)} 文字")
                logger.info(f"最初の200文字: {repr(content[:200])}")
                
                # 行に分割
                lines = content.split('\n')
                logger.info(f"行数: {len(lines)}")
                
                for i, line in enumerate(lines[:5]):  # 最初の5行
                    logger.info(f"行 {i+1}: {repr(line)}")
                
                break
                
        except UnicodeDecodeError as e:
            logger.warning(f"エンコーディング '{encoding}' で読み込み失敗: {e}")
            continue
        except Exception as e:
            logger.error(f"エンコーディング '{encoding}' で予期しないエラー: {e}")
            continue
    else:
        logger.error("すべてのエンコーディングで読み込みに失敗")
    
    logger.info(f"=== CSV読み込みテスト終了 ===")

def main():
    """メイン関数"""
    logger = setup_logger()
    
    if len(sys.argv) < 2:
        print("使用法: python debug_test.py <test_type> [parameters]")
        print("  test_type: 'path' または 'csv'")
        print("  path: python debug_test.py path <directory_path>")
        print("  csv: python debug_test.py csv <csv_file_path>")
        sys.exit(1)
    
    test_type = sys.argv[1]
    
    if test_type == 'path':
        if len(sys.argv) < 3:
            print("パスを指定してください")
            sys.exit(1)
        
        test_path = sys.argv[2]
        test_path_validation(test_path, logger)
    
    elif test_type == 'csv':
        if len(sys.argv) < 3:
            print("CSVファイルパスを指定してください")
            sys.exit(1)
        
        csv_path = sys.argv[2]
        test_csv_reading(csv_path, logger)
    
    else:
        print(f"不明なテストタイプ: {test_type}")
        sys.exit(1)

if __name__ == "__main__":
    main()