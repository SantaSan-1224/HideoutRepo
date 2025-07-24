#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV内容の詳細解析スクリプト
パスの問題を特定するため
"""

import os
import sys
import csv
import logging

def setup_logger():
    """ログ設定"""
    logger = logging.getLogger('csv_analyzer')
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def analyze_csv_content(csv_path: str, logger):
    """CSVファイルの詳細解析"""
    logger.info(f"=== CSV詳細解析開始: {csv_path} ===")
    
    # ファイルの存在確認
    if not os.path.exists(csv_path):
        logger.error(f"CSVファイルが存在しません: {csv_path}")
        return
    
    # ファイルサイズ
    file_size = os.path.getsize(csv_path)
    logger.info(f"ファイルサイズ: {file_size} bytes")
    
    # 複数のエンコーディングで内容を確認
    encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932', 'euc-jp', 'latin-1']
    
    for encoding in encodings:
        try:
            logger.info(f"\n--- エンコーディング: {encoding} ---")
            
            with open(csv_path, 'rb') as f:
                raw_bytes = f.read()
                logger.info(f"生バイト (最初の100バイト): {raw_bytes[:100]}")
            
            with open(csv_path, 'r', encoding=encoding) as f:
                content = f.read()
                logger.info(f"読み込み成功 - 文字数: {len(content)}")
                logger.info(f"内容: {repr(content)}")
                
                # 行に分割して詳細確認
                lines = content.split('\n')
                logger.info(f"行数: {len(lines)}")
                
                for i, line in enumerate(lines):
                    if line.strip():  # 空行以外
                        logger.info(f"行 {i+1}: {repr(line)}")
                        logger.info(f"行 {i+1} 文字コード: {[ord(c) for c in line]}")
                        
                        # CSVとして解析
                        try:
                            row = next(csv.reader([line]))
                            logger.info(f"行 {i+1} CSV解析: {row}")
                            
                            if len(row) > 0:
                                path_value = row[0].strip()
                                logger.info(f"行 {i+1} パス値: {repr(path_value)}")
                                
                                # パス正規化テスト
                                normalized = normalize_path_test(path_value, logger)
                                logger.info(f"行 {i+1} 正規化後: {repr(normalized)}")
                                
                                # 存在チェック
                                if normalized:
                                    exists = os.path.exists(normalized)
                                    logger.info(f"行 {i+1} 存在チェック: {exists}")
                                    
                        except Exception as e:
                            logger.warning(f"行 {i+1} CSV解析エラー: {e}")
                
                # このエンコーディングで成功したので終了
                logger.info(f"エンコーディング {encoding} で解析完了")
                break
                
        except UnicodeDecodeError as e:
            logger.warning(f"エンコーディング {encoding} で読み込み失敗: {e}")
            continue
        except Exception as e:
            logger.error(f"エンコーディング {encoding} で予期しないエラー: {e}")
            continue
    else:
        logger.error("すべてのエンコーディングで読み込みに失敗")

def normalize_path_test(path: str, logger) -> str:
    """パス正規化のテスト"""
    logger.info(f"パス正規化テスト開始: {repr(path)}")
    
    # 元のパス情報
    logger.info(f"元パス長: {len(path)}")
    logger.info(f"元パス文字コード: {[ord(c) for c in path]}")
    
    # ステップバイステップで正規化
    step1 = path.strip()
    logger.info(f"ステップ1 (strip): {repr(step1)}")
    
    step2 = step1.replace('/', '\\')
    logger.info(f"ステップ2 (スラッシュ変換): {repr(step2)}")
    
    step3 = step2.rstrip('\\')
    logger.info(f"ステップ3 (末尾\\削除): {repr(step3)}")
    
    # 特殊文字チェック
    for i, char in enumerate(step3):
        if ord(char) < 32 or ord(char) > 126:
            if char not in ['\u3042', '\u3044', '\u3046']:  # 日本語文字は除外
                logger.warning(f"位置 {i}: 特殊文字検出 '{char}' (コード: {ord(char)})")
    
    return step3

def test_path_variations(base_path: str, logger):
    """パスのバリエーションテスト"""
    logger.info(f"=== パスバリエーションテスト ===")
    
    variations = [
        base_path,
        base_path.replace('\\', '/'),
        base_path.replace('/', '\\'),
        '\\\\' + base_path.lstrip('\\'),
        base_path.rstrip('\\'),
        base_path.rstrip('/'),
    ]
    
    for i, variation in enumerate(variations):
        logger.info(f"バリエーション {i+1}: {repr(variation)}")
        try:
            exists = os.path.exists(variation)
            logger.info(f"  存在チェック: {exists}")
            if exists:
                is_dir = os.path.isdir(variation)
                logger.info(f"  ディレクトリチェック: {is_dir}")
        except Exception as e:
            logger.error(f"  エラー: {e}")

def main():
    """メイン関数"""
    logger = setup_logger()
    
    if len(sys.argv) < 2:
        print("使用法: python csv_content_analyzer.py <csv_file_path> [test_path]")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    # CSV内容の解析
    analyze_csv_content(csv_path, logger)
    
    # 特定のパステストが指定された場合
    if len(sys.argv) > 2:
        test_path = sys.argv[2]
        test_path_variations(test_path, logger)

if __name__ == "__main__":
    main()