#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
データベース検索デバッグスクリプト
復元スクリプトの問題を特定するための単独テスト
"""

import json
import psycopg2

def test_database_search():
    """データベース検索のテスト"""
    
    # 設定読み込み
    try:
        with open('config/archive_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
    except Exception as e:
        print(f"設定ファイル読み込みエラー: {e}")
        return
    
    # データベース接続
    try:
        db_config = config.get('database', {})
        conn = psycopg2.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 5432),
            database=db_config.get('database', 'archive_system'),
            user=db_config.get('user', 'postgres'),
            password=db_config.get('password', ''),
            connect_timeout=30
        )
        print("データベース接続成功")
        
    except Exception as e:
        print(f"データベース接続エラー: {e}")
        return
    
    # 検索対象パス
    search_path = "\\\\amznfsxbeak7dyp.priv-req-gl01.fujifilm-intra.com\\test\\test_dir_02\\"
    print(f"検索対象パス: {search_path}")
    print(f"パス長: {len(search_path)}")
    
    try:
        with conn.cursor() as cursor:
            # 1. 総レコード数確認
            cursor.execute("SELECT COUNT(*) FROM archive_history")
            total_count = cursor.fetchone()[0]
            print(f"総レコード数: {total_count}")
            
            # 2. サンプルデータ表示
            cursor.execute("SELECT original_file_path FROM archive_history LIMIT 3")
            samples = cursor.fetchall()
            print("サンプルデータ:")
            for i, row in enumerate(samples, 1):
                print(f"  {i}: {row[0]}")
            
            # 3. test_dir_02 を含むパス検索
            cursor.execute("SELECT original_file_path FROM archive_history WHERE original_file_path LIKE %s", ("%test_dir_02%",))
            test_results = cursor.fetchall()
            print(f"test_dir_02を含むパス: {len(test_results)}件")
            for row in test_results[:5]:  # 最初の5件のみ
                print(f"  - {row[0]}")
            
            # 4. 複数の検索パターンでテスト
            patterns = [
                f"{search_path}%",
                f"{search_path[:-1]}\\%",
                f"{search_path}*",
                f"%test_dir_02%",
                f"%test_dir_02\\%",
                f"%test_dir_02/%"
            ]
            
            print("\n検索パターンテスト:")
            for i, pattern in enumerate(patterns, 1):
                try:
                    cursor.execute("SELECT COUNT(*) FROM archive_history WHERE original_file_path LIKE %s", (pattern,))
                    count = cursor.fetchone()[0]
                    print(f"  パターン {i}: {pattern} -> {count}件")
                    
                    if count > 0:
                        cursor.execute("SELECT original_file_path FROM archive_history WHERE original_file_path LIKE %s LIMIT 2", (pattern,))
                        examples = cursor.fetchall()
                        for example in examples:
                            print(f"    例: {example[0]}")
                except Exception as e:
                    print(f"  パターン {i}: {pattern} -> エラー: {e}")
            
            # 5. サーバー名での検索
            server_name = "amznfsxbeak7dyp"
            cursor.execute("SELECT COUNT(*) FROM archive_history WHERE original_file_path LIKE %s", (f"%{server_name}%",))
            server_count = cursor.fetchone()[0]
            print(f"\nサーバー名'{server_name}'を含むパス: {server_count}件")
            
            if server_count > 0:
                cursor.execute("SELECT original_file_path FROM archive_history WHERE original_file_path LIKE %s LIMIT 3", (f"%{server_name}%",))
                server_examples = cursor.fetchall()
                for example in server_examples:
                    print(f"  例: {example[0]}")
    
    except Exception as e:
        print(f"検索エラー: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    test_database_search()
