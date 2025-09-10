import os
import sys
from pathlib import Path
import time

def format_size(size_bytes):
    """バイト数を人間が読みやすい形式に変換"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024.0 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"

def get_folder_size(folder_path):
    """指定フォルダの総サイズを計算（サブフォルダ含む）"""
    total_size = 0
    file_count = 0
    
    try:
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                try:
                    # ファイルサイズを取得
                    file_size = os.path.getsize(filepath)
                    total_size += file_size
                    file_count += 1
                except (OSError, IOError) as e:
                    # アクセス権限がない場合などのエラーハンドリング
                    print(f"警告: {filepath} にアクセスできません - {e}")
                    
    except (OSError, IOError) as e:
        print(f"エラー: フォルダ {folder_path} にアクセスできません - {e}")
        return 0, 0
    
    return total_size, file_count

def scan_folders(base_path):
    """指定パスの直下フォルダのサイズを調査"""
    print(f"フォルダサイズ調査開始: {base_path}")
    print("=" * 80)
    
    # ベースパスの存在確認
    if not os.path.exists(base_path):
        print(f"エラー: パス '{base_path}' が存在しません")
        return
    
    if not os.path.isdir(base_path):
        print(f"エラー: '{base_path}' はフォルダではありません")
        return
    
    folder_sizes = []
    
    try:
        # 直下のフォルダを取得
        items = os.listdir(base_path)
        folders = [item for item in items if os.path.isdir(os.path.join(base_path, item))]
        
        if not folders:
            print("直下にフォルダが見つかりません")
            return
        
        print(f"発見されたフォルダ数: {len(folders)}")
        print("-" * 80)
        
        # 各フォルダのサイズを計算
        for i, folder in enumerate(folders, 1):
            folder_path = os.path.join(base_path, folder)
            print(f"[{i}/{len(folders)}] 計算中: {folder}")
            
            start_time = time.time()
            size_bytes, file_count = get_folder_size(folder_path)
            end_time = time.time()
            
            folder_sizes.append({
                'name': folder,
                'size_bytes': size_bytes,
                'size_formatted': format_size(size_bytes),
                'file_count': file_count,
                'scan_time': end_time - start_time
            })
            
            print(f"    サイズ: {format_size(size_bytes)} ({file_count:,} ファイル) - {end_time - start_time:.2f}秒")
            print()
        
        # 結果をサイズ順でソート（降順）
        folder_sizes.sort(key=lambda x: x['size_bytes'], reverse=True)
        
        # 結果サマリー表示
        print("=" * 80)
        print("結果サマリー（サイズ順）")
        print("=" * 80)
        
        total_size = sum(folder['size_bytes'] for folder in folder_sizes)
        total_files = sum(folder['file_count'] for folder in folder_sizes)
        
        print(f"{'順位':<4} {'フォルダ名':<30} {'サイズ':<12} {'ファイル数':<10} {'割合':<8}")
        print("-" * 80)
        
        for i, folder_info in enumerate(folder_sizes, 1):
            percentage = (folder_info['size_bytes'] / total_size * 100) if total_size > 0 else 0
            print(f"{i:<4} {folder_info['name']:<30} {folder_info['size_formatted']:<12} "
                  f"{folder_info['file_count']:<10,} {percentage:.1f}%")
        
        print("-" * 80)
        print(f"合計: {format_size(total_size)} ({total_files:,} ファイル)")
        
        # CSV出力用のコード（オプション）
        csv_output = input("\n結果をCSVファイルに出力しますか？ (y/N): ")
        if csv_output.lower() in ['y', 'yes']:
            export_to_csv(folder_sizes, base_path)
        
    except PermissionError:
        print(f"エラー: '{base_path}' にアクセス権限がありません")
    except Exception as e:
        print(f"予期しないエラーが発生しました: {e}")

def export_to_csv(folder_sizes, base_path):
    """結果をCSVファイルに出力"""
    import csv
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"folder_sizes_{timestamp}.csv"
    
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['順位', 'フォルダ名', 'サイズ(バイト)', 'サイズ(読みやすい形式)', 'ファイル数', 'スキャン時間(秒)']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for i, folder_info in enumerate(folder_sizes, 1):
                writer.writerow({
                    '順位': i,
                    'フォルダ名': folder_info['name'],
                    'サイズ(バイト)': folder_info['size_bytes'],
                    'サイズ(読みやすい形式)': folder_info['size_formatted'],
                    'ファイル数': folder_info['file_count'],
                    'スキャン時間(秒)': f"{folder_info['scan_time']:.2f}"
                })
        
        print(f"CSVファイルに出力しました: {csv_filename}")
        
    except Exception as e:
        print(f"CSV出力でエラーが発生しました: {e}")

def main():
    """メイン関数"""
    # デフォルトパス（必要に応じて変更してください）
    default_path = r"\\server\share\A_Doc"
    
    if len(sys.argv) > 1:
        target_path = sys.argv[1]
    else:
        print("フォルダサイズ調査ツール")
        print("=" * 40)
        target_path = input(f"調査するパスを入力してください (Enter でデフォルト: {default_path}): ").strip()
        
        if not target_path:
            target_path = default_path
    
    print(f"\n調査対象: {target_path}")
    
    # 実行確認
    confirm = input("実行しますか？ (Y/n): ")
    if confirm.lower() not in ['', 'y', 'yes']:
        print("キャンセルされました")
        return
    
    print()
    scan_folders(target_path)

if __name__ == "__main__":
    main()