#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アーカイブスクリプト検証用テストデータ生成スクリプト
サイズ混在のダミーファイルを生成
"""

import os
import random
import argparse
import datetime
from pathlib import Path
from typing import List, Tuple

class TestDataGenerator:
    """テストデータ生成クラス"""
    
    def __init__(self):
        self.generated_files = []
        self.total_size = 0
        
    def create_mixed_size_test_files(self, base_dir: str, file_count: int = 100) -> None:
        """
        サイズが混在したテストファイルを生成
        
        Args:
            base_dir: 出力ディレクトリ
            file_count: 生成するファイル数
        """
        print(f"テストデータ生成開始: {file_count}ファイルを{base_dir}に作成")
        
        # ディレクトリ作成
        Path(base_dir).mkdir(parents=True, exist_ok=True)
        
        # サイズパターンの定義（大ファイル多めのバランス）
        size_patterns = self._generate_size_patterns(file_count)
        
        print("ファイルサイズ構成:")
        for size_name, size_bytes, count in size_patterns:
            print(f"  {size_name}: {count}ファイル")
        
        # ファイル生成
        file_index = 0
        for size_name, size_bytes, count in size_patterns:
            for i in range(count):
                file_path = self._create_single_file(
                    base_dir, file_index, size_name, size_bytes
                )
                self.generated_files.append(file_path)
                self.total_size += size_bytes
                file_index += 1
                
                # 進捗表示
                if file_index % 10 == 0:
                    print(f"  生成中... {file_index}/{file_count}")
        
        # 完了報告
        self._print_summary(base_dir, file_count)
    
    def _generate_size_patterns(self, total_count: int) -> List[Tuple[str, int, int]]:
        """
        ファイルサイズパターンを生成（大ファイル多めのバランス）
        
        Returns:
            List[Tuple[サイズ名, バイト数, ファイル数]]
        """
        patterns = []
        
        # 大ファイル重視の配分
        if total_count >= 100:
            # 100ファイル以上の場合
            patterns = [
                ("100MB", 100 * 1024 * 1024, max(1, total_count // 100)),     # 1%
                ("50MB",  50 * 1024 * 1024,  max(2, total_count // 50)),      # 2%
                ("10MB",  10 * 1024 * 1024,  max(5, total_count // 20)),      # 5%
                ("5MB",   5 * 1024 * 1024,   max(8, total_count // 12)),      # 8%
                ("1MB",   1 * 1024 * 1024,   max(15, total_count // 6)),      # 15%
                ("500KB", 500 * 1024,        max(10, total_count // 10)),     # 10%
                ("100KB", 100 * 1024,        max(15, total_count // 6)),      # 15%
                ("10KB",  10 * 1024,         max(20, total_count // 5)),      # 20%
                ("1KB",   1 * 1024,          0),                              # 残り全て
            ]
        else:
            # 100ファイル未満の場合（比例配分）
            patterns = [
                ("100MB", 100 * 1024 * 1024, max(1, total_count // 100)),
                ("10MB",  10 * 1024 * 1024,  max(1, total_count // 25)),
                ("1MB",   1 * 1024 * 1024,   max(2, total_count // 10)),
                ("100KB", 100 * 1024,        max(3, total_count // 8)),
                ("10KB",  10 * 1024,         max(5, total_count // 5)),
                ("1KB",   1 * 1024,          0),  # 残り全て
            ]
        
        # 残りファイル数を1KBファイルに割り当て
        assigned_count = sum(count for _, _, count in patterns[:-1])
        remaining_count = max(0, total_count - assigned_count)
        patterns[-1] = (patterns[-1][0], patterns[-1][1], remaining_count)
        
        # ファイル数が0のパターンを除去
        patterns = [(name, size, count) for name, size, count in patterns if count > 0]
        
        return patterns
    
    def _create_single_file(self, base_dir: str, file_index: int, 
                           size_name: str, size_bytes: int) -> str:
        """単一ファイルの生成"""
        
        # ファイル名生成
        timestamp = datetime.datetime.now().strftime("%H%M%S")
        filename = f"test_{file_index:04d}_{size_name}_{timestamp}.dat"
        file_path = os.path.join(base_dir, filename)
        
        # ファイル生成（効率的な書き込み）
        with open(file_path, 'wb') as f:
            remaining = size_bytes
            chunk_size = min(64 * 1024, size_bytes)  # 64KB チャンク
            
            while remaining > 0:
                write_size = min(chunk_size, remaining)
                
                # 小さなファイルの場合は固定パターン、大きなファイルの場合はランダムデータ
                if size_bytes < 1024 * 1024:  # 1MB未満
                    data = b'TEST_DATA_' * (write_size // 10) + b'TEST_DATA_'[:write_size % 10]
                else:  # 1MB以上
                    data = os.urandom(write_size)
                
                f.write(data)
                remaining -= write_size
        
        return file_path
    
    def _print_summary(self, base_dir: str, expected_count: int) -> None:
        """生成結果のサマリー表示"""
        actual_count = len(self.generated_files)
        total_size_mb = self.total_size / (1024 * 1024)
        
        print(f"\n=== テストデータ生成完了 ===")
        print(f"出力先: {base_dir}")
        print(f"生成ファイル数: {actual_count} (期待値: {expected_count})")
        print(f"総ファイルサイズ: {total_size_mb:.2f} MB")
        print(f"平均ファイルサイズ: {(self.total_size / actual_count / 1024):.2f} KB")
        
        # ファイルサイズ分布の表示
        self._show_size_distribution()
    
    def _show_size_distribution(self) -> None:
        """ファイルサイズ分布の表示"""
        if not self.generated_files:
            return
        
        print(f"\nファイルサイズ分布:")
        
        size_ranges = [
            ("1KB未満", 0, 1024),
            ("1KB-10KB", 1024, 10*1024),
            ("10KB-100KB", 10*1024, 100*1024),
            ("100KB-1MB", 100*1024, 1024*1024),
            ("1MB-10MB", 1024*1024, 10*1024*1024),
            ("10MB-100MB", 10*1024*1024, 100*1024*1024),
            ("100MB以上", 100*1024*1024, float('inf'))
        ]
        
        for range_name, min_size, max_size in size_ranges:
            count = 0
            total_size = 0
            
            for file_path in self.generated_files:
                file_size = os.path.getsize(file_path)
                if min_size <= file_size < max_size:
                    count += 1
                    total_size += file_size
            
            if count > 0:
                avg_size = total_size / count / 1024  # KB
                total_mb = total_size / 1024 / 1024   # MB
                print(f"  {range_name}: {count}ファイル (平均{avg_size:.1f}KB, 計{total_mb:.2f}MB)")
    
    def create_directory_structure(self, base_dir: str, dir_count: int = 5) -> List[str]:
        """
        複数ディレクトリ構造の作成
        
        Args:
            base_dir: ベースディレクトリ
            dir_count: 作成するサブディレクトリ数
            
        Returns:
            作成されたディレクトリのリスト
        """
        print(f"\nディレクトリ構造作成: {dir_count}個のサブディレクトリ")
        
        directories = []
        for i in range(dir_count):
            dir_name = f"test_dir_{i+1:02d}"
            dir_path = os.path.join(base_dir, dir_name)
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            directories.append(dir_path)
            print(f"  作成: {dir_path}")
        
        return directories
    
    def generate_csv_file(self, directories: List[str], csv_path: str) -> None:
        """
        アーカイブスクリプト用のCSVファイル生成
        
        Args:
            directories: 対象ディレクトリのリスト
            csv_path: 出力CSVファイルパス
        """
        print(f"\nCSVファイル生成: {csv_path}")
        
        with open(csv_path, 'w', encoding='utf-8-sig') as f:
            f.write("Directory Path\n")  # ヘッダー
            for directory in directories:
                f.write(f"{directory}\n")
        
        print(f"CSV生成完了: {len(directories)}ディレクトリを記載")

def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='アーカイブスクリプト検証用テストデータ生成')
    parser.add_argument('base_dir', help='テストデータ出力先ディレクトリ')
    parser.add_argument('--file-count', type=int, default=100, 
                       help='生成するファイル数 (デフォルト: 100)')
    parser.add_argument('--dir-count', type=int, default=5,
                       help='作成するサブディレクトリ数 (デフォルト: 5)')
    parser.add_argument('--csv-output', default='test_archive_request.csv',
                       help='アーカイブ依頼CSV出力パス (デフォルト: test_archive_request.csv)')
    
    args = parser.parse_args()
    
    try:
        generator = TestDataGenerator()
        
        # 1. ディレクトリ構造作成
        directories = generator.create_directory_structure(args.base_dir, args.dir_count)
        
        # 2. 各ディレクトリにテストファイル生成
        files_per_dir = args.file_count // args.dir_count
        remaining_files = args.file_count % args.dir_count
        
        for i, directory in enumerate(directories):
            # 余りファイルを最初のディレクトリに追加
            file_count = files_per_dir + (1 if i < remaining_files else 0)
            if file_count > 0:
                print(f"\n--- {os.path.basename(directory)} ---")
                dir_generator = TestDataGenerator()
                dir_generator.create_mixed_size_test_files(directory, file_count)
        
        # 3. アーカイブ依頼CSV生成
        generator.generate_csv_file(directories, args.csv_output)
        
        print(f"\n🎉 全ての準備が完了しました！")
        print(f"📁 テストデータ: {args.base_dir}")
        print(f"📄 アーカイブ依頼CSV: {args.csv_output}")
        print(f"📊 総ファイル数: {args.file_count}")
        print(f"📂 ディレクトリ数: {args.dir_count}")
        
        print(f"\n次のコマンドでアーカイブスクリプト検証を実行できます:")
        print(f"python archive_script_test_v1.py {args.csv_output} TEST-REQ-001")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())