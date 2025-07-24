#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
シンプルCSVテストスクリプト
パス切れ問題の原因特定用
"""

import sys
import csv

def test_csv_file(csv_path):
    print(f"=== CSVファイルテスト: {csv_path} ===")
    
    # 1. 生バイト読み込み
    print("\n1. 生バイト読み込み:")
    with open(csv_path, 'rb') as f:
        raw_data = f.read()
        print(f"ファイルサイズ: {len(raw_data)} bytes")
        print(f"最初の100バイト: {raw_data[:100]}")
        print(f"最後の50バイト: {raw_data[-50:]}")
    
    # 2. UTF-8で文字列読み込み
    print("\n2. UTF-8文字列読み込み:")
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        content = f.read()
        print(f"文字数: {len(content)}")
        print(f"内容: {repr(content)}")
    
    # 3. 行単位読み込み
    print("\n3. 行単位読み込み:")
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            print(f"行 {i+1}: {repr(line)} (長さ: {len(line)})")
    
    # 4. CSVライブラリ使用（フィールド制限なし）
    print("\n4. CSVライブラリ（制限なし）:")
    csv.field_size_limit(sys.maxsize)
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            print(f"行 {i+1}: {repr(row)}")
            for j, cell in enumerate(row):
                print(f"  セル[{j}]: {repr(cell)} (長さ: {len(cell)})")
    
    # 5. CSVライブラリ使用（デフォルト制限）
    print("\n5. CSVライブラリ（デフォルト制限）:")
    csv.field_size_limit(131072)  # デフォルト値
    try:
        with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                print(f"行 {i+1}: {repr(row)}")
                for j, cell in enumerate(row):
                    print(f"  セル[{j}]: {repr(cell)} (長さ: {len(cell)})")
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用法: python simple_csv_test.py <csv_file>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    test_csv_file(csv_path)
