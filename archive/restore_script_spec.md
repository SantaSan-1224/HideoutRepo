# 復元スクリプト仕様書

## 1. 概要

### 1.1 目的
AWS S3 Glacier Deep Archiveからアーカイブされたファイルをファイルサーバへ復元するPythonスクリプト。

### 1.2 スクリプト名
`restore_script_main.py`

### 1.3 実行環境
- **Python**: 3.8以上
- **OS**: Windows Server（FSxアクセス用）
- **AWS**: EC2インスタンス（4vCPU、16GBメモリ）

## 2. 機能仕様

### 2.1 主要機能
1. **復元依頼CSV読み込み・検証**: 元ファイルパスと復元先の妥当性チェック
2. **データベース検索**: アーカイブ履歴からS3パス取得
3. **S3復元リクエスト送信**: Glacier Deep Archiveからの復元要求
4. **復元ステータス確認**: S3 APIでの復元完了判定
5. **ファイルダウンロード・配置**: S3から指定ディレクトリへの配置
6. **ステータス管理**: JSON形式での復元状況追跡

### 2.2 実行モード

#### 2.2.1 復元リクエスト送信モード (`--request-only`)
- 復元依頼CSV読み込み・検証
- データベースからS3パス検索
- S3復元リクエスト送信
- 復元ステータスファイル保存

#### 2.2.2 ダウンロード実行モード (`--download-only`)
- 復元ステータスファイル読み込み
- S3復元ステータス確認（自動実行）
- 復元完了ファイルのダウンロード・配置
- ステータスファイル更新

### 2.3 入力仕様

#### 2.3.1 コマンドライン引数
```bash
python restore_script_main.py <csv_path> <request_id> [--config <config_path>] <mode>
```

| 引数 | 必須 | 説明 | 例 |
|------|------|------|-----|
| csv_path | ✓ | 復元依頼を記載したCSVファイルパス | `restore_request.csv` |
| request_id | ✓ | 復元依頼ID | `REQ-RESTORE-001` |
| --config | - | 設定ファイルパス | `config/archive_config.json` |
| --request-only | ✓※ | 復元リクエスト送信のみ実行 | |
| --download-only | ✓※ | 復元ステータス確認+ダウンロード実行 | |

※ どちらか一方必須

#### 2.3.2 復元依頼CSVファイル形式
```csv
元ファイルパス,復元先ディレクトリ
\\server\share\project1\file.txt,C:\restored\files\
\\server\share\project2\data.xlsx,D:\backup\restore\
\\server\share\archive\document.pdf,\\fileserver\shared\restored\
```

**仕様**:
- **エンコーディング**: UTF-8-SIG
- **ヘッダー**: 自動検出（"復元対象"、"元ファイルパス"等を含む行）
- **データ行**: 1行1復元依頼
- **元ファイルパス**: アーカイブ時のoriginal_file_path（データベース検索キー）
- **復元先ディレクトリ**: ファイルを配置するディレクトリパス

#### 2.3.3 設定ファイル形式
```json
{
    "aws": {
        "region": "ap-northeast-1",
        "s3_bucket": "your-bucket-name",
        "vpc_endpoint_url": "https://bucket.vpce-xxx.s3.region.vpce.amazonaws.com"
    },
    "database": {
        "host": "localhost",
        "port": 5432,
        "database": "archive_system",
        "user": "postgres",
        "password": "password",
        "timeout": 30
    },
    "restore": {
        "restore_tier": "Standard",
        "restore_days": 7,
        "check_interval": 300,
        "max_wait_time": 86400,
        "download_retry_count": 3,
        "skip_existing_files": true,
        "temp_download_directory": "temp_downloads"
    },
    "logging": {
        "log_directory": "logs",
        "log_level": "INFO"
    }
}
```

### 2.4 出力仕様

#### 2.4.1 ログファイル
- **場所**: `logs/restore_YYYYMMDD_HHMMSS.log`
- **レベル**: DEBUG（ファイル）、INFO（コンソール）
- **形式**: `YYYY-MM-DD HH:MM:SS - logger_name - LEVEL - message`

#### 2.4.2 復元ステータスファイル
- **場所**: `logs/restore_status_{request_id}.json`
- **形式**: JSON
```json
{
  "request_id": "REQ-RESTORE-001",
  "request_date": "2025-07-16T10:30:00",
  "total_requests": 10,
  "restore_requests": [
    {
      "line_number": 2,
      "original_file_path": "\\\\server\\share\\file.txt",
      "restore_directory": "C:\\restored\\",
      "s3_path": "s3://bucket/server/share/file.txt",
      "bucket": "bucket",
      "key": "server/share/file.txt",
      "restore_status": "completed",
      "restore_request_time": "2025-07-16T10:30:15",
      "restore_completed_time": "2025-07-18T14:20:00",
      "restore_expiry": "Fri, 25 Jul 2025 14:20:00 GMT",
      "download_status": "completed",
      "destination_path": "C:\\restored\\file.txt",
      "downloaded_size": 1024
    }
  ]
}
```

#### 2.4.3 エラーCSVファイル
**復元依頼エラー**: `logs/{元ファイル名}_restore_errors_YYYYMMDD_HHMMSS.csv`
```csv
行番号,内容,エラー理由,元の行
2,"\\invalid\path -> C:\restore","元ファイルパスが存在しません","\\invalid\path,C:\restore"
```

#### 2.4.4 復元ファイル
- **場所**: 復元依頼CSVで指定されたディレクトリ
- **ファイル名**: 元ファイル名のまま
- **同名ファイル**: スキップ（設定により制御可能）

## 3. 処理フロー仕様

### 3.1 復元リクエスト送信モード処理フロー
```
1. 復元依頼CSV読み込み・検証
2. データベースからS3パス検索
3. S3復元リクエスト送信
   - restore_object API実行
   - 復元ティア指定（Standard/Expedited/Bulk）
   - 保持日数指定（デフォルト7日）
4. 復元ステータスファイル保存
5. 48時間後のダウンロード実行案内
```

### 3.2 ダウンロード実行モード処理フロー
```
1. 復元ステータスファイル読み込み
2. S3復元ステータス確認（自動実行）
   - head_object API実行
   - Restoreヘッダー解析
   - 復元完了判定
3. 復元完了ファイルのフィルタリング
4. ファイルダウンロード・配置
   - 一時ディレクトリにダウンロード
   - 同名ファイルスキップチェック
   - 最終配置先に移動
5. ステータスファイル更新
```

### 3.3 復元依頼CSV検証処理
```
入力: 復元依頼CSVファイルパス
出力: (有効な復元依頼リスト, エラー項目リスト)

1. UTF-8-SIG エンコーディングで読み込み
2. ヘッダー行の自動検出・スキップ
3. 各行の復元依頼検証
   - カラム数チェック（2カラム必須）
   - 元ファイルパス妥当性チェック
   - 復元先ディレクトリ存在・権限チェック
   - パス長制限チェック（260文字以下）
4. エラー発生時も処理継続
5. エラーCSV生成（エラーがある場合）
```

### 3.4 S3復元ステータス確認処理
```
入力: 復元リクエストリスト
出力: ステータス更新された復元リクエストリスト

1. S3 head_object API実行
2. Restoreヘッダー解析
   - ongoing-request="true" → 復元処理中
   - ongoing-request="false" → 復元完了
   - ヘッダーなし → 復元未開始
3. 復元有効期限抽出（可能な場合）
4. ステータス更新
   - pending/in_progress/completed/failed
```

### 3.5 ファイルダウンロード・配置処理
```
入力: 復元完了ファイルリスト
出力: ダウンロード結果リスト

復元完了ファイルのみ処理:
1. 一時ダウンロードディレクトリ作成
2. 各ファイルの処理
   - 同名ファイル存在チェック（スキップ判定）
   - S3からの一時ダウンロード（リトライ付き）
   - ファイルサイズ確認（0バイトファイル対応）
   - 最終配置先への移動
   - 一時ファイル自動クリーンアップ
3. 進捗・統計情報出力
```

## 4. エラーハンドリング仕様

### 4.1 エラー分類

| エラー種別 | 処理継続 | リトライ | エラーCSV |
|-----------|---------|---------|-----------|
| CSV読み込みエラー | × | - | - |
| CSV検証エラー | ✓ | - | ✓ |
| データベース接続エラー | × | - | - |
| S3接続エラー | × | - | - |
| 復元リクエストエラー | ✓ | × | - |
| ダウンロードエラー | ✓ | ✓ | - |
| ファイル配置エラー | ✓ | × | - |

### 4.2 復元ステータス値

| ステータス | 説明 | 次のアクション |
|-----------|------|---------------|
| requested | 復元リクエスト送信済み | 待機 |
| already_in_progress | 既に復元処理中 | 待機 |
| pending | 復元処理待機中 | 待機 |
| in_progress | 復元処理中 | 待機 |
| completed | 復元完了（ダウンロード可能） | ダウンロード実行 |
| failed | 復元失敗 | 調査・再実行 |

### 4.3 ダウンロードリトライ仕様
- **対象**: S3ダウンロードエラー
- **回数**: 最大3回（設定可能）
- **間隔**: 指数バックオフ（2^n秒）
- **除外**: NoSuchKey、AccessDenied、InvalidObjectState

### 4.4 ログレベル仕様

| レベル | 用途 | 例 |
|--------|------|-----|
| DEBUG | デバッグ情報 | 一時ファイル削除成功 |
| INFO | 処理進捗 | 復元完了確認開始 |
| WARNING | 警告 | 同名ファイルをスキップ |
| ERROR | エラー | 復元先ディレクトリが存在しません |

## 5. 復元ティア仕様

### 5.1 復元ティア選択

| ティア | 復元時間 | コスト | 用途 |
|-------|---------|-------|------|
| Standard | 3-5時間 | 中程度 | **推奨**：通常の復元作業 |
| Expedited | 1-5分 | 高額 | 緊急時のみ |
| Bulk | 5-12時間 | 安価 | 大量ファイル・コスト重視 |

### 5.2 復元保持期間
- **デフォルト**: 7日間
- **設定可能範囲**: 1-30日
- **注意**: 期限切れ後は再度復元リクエストが必要

## 6. パフォーマンス仕様

### 6.1 復元処理能力
- **復元リクエスト送信**: 1,000ファイル/分
- **ダウンロード処理**: ネットワーク帯域に依存
- **並行処理**: なし（シーケンシャル処理）

### 6.2 メモリ使用量
- **復元リクエスト情報**: 約200バイト/ファイル
- **一時ダウンロード**: 1ファイルずつ処理（メモリ効率化）

### 6.3 ディスク使用量
- **一時ダウンロード**: 最大ファイルサイズ分の空き容量必要
- **ステータスファイル**: 約1KB/ファイル

## 7. セキュリティ仕様

### 7.1 認証
- **AWS**: IAMロール認証
- **PostgreSQL**: ユーザー名・パスワード認証

### 7.2 通信暗号化
- **S3**: HTTPS（VPCエンドポイント経由）
- **PostgreSQL**: SSL（設定による）

### 7.3 権限
- **S3**: RestoreObject、GetObject権限
- **PostgreSQL**: SELECT権限
- **ファイルサーバ**: 書き込み権限

## 8. 制約事項

### 8.1 システム制約
- **並行実行**: 不可（同一request_idでの競合回避）
- **復元待機時間**: Glacier Deep Archiveは12-48時間
- **復元有効期限**: 設定した日数後に自動削除

### 8.2 ファイル制約
- **最大パス長**: 260文字（Windows制限）
- **同名ファイル**: スキップ（上書きしない）
- **0バイトファイル**: 正常に処理

### 8.3 運用制約
- **復元履歴**: データベースに記録しない
- **元ファイル検索**: original_file_pathで完全一致検索
- **一時ファイル**: 処理完了後に自動削除

## 9. 戻り値仕様

### 9.1 終了コード

| コード | 意味 | 説明 |
|--------|------|------|
| 0 | 正常終了 | 全処理完了 |
| 1 | 異常終了 | 致命的エラー発生 |

### 9.2 統計情報
```
=== 復元処理統計 ===
処理時間: 0:01:23.123456
CSV検証エラー数: 1
総復元依頼数: 10
復元リクエスト送信数: 9
復元完了数: 8
失敗数: 1
```

## 10. 依存関係

### 10.1 Pythonパッケージ
```
boto3>=1.26.0
psycopg2-binary>=2.9.0
```

### 10.2 システム要件
- **Python**: 3.8以上
- **AWS CLI**: 設定済み（認証情報）
- **PostgreSQL**: 接続可能（SELECT権限）
- **FSx**: マウント済み（書き込み権限）

## 11. 設定可能項目

### 11.1 復元設定
| 項目 | デフォルト | 説明 |
|------|-----------|------|
| restore_tier | "Standard" | 復元速度ティア |
| restore_days | 7 | 復元後保持日数 |
| download_retry_count | 3 | ダウンロードリトライ回数 |
| skip_existing_files | true | 同名ファイルスキップ |
| temp_download_directory | "temp_downloads" | 一時ダウンロード先 |

### 11.2 その他設定
- **check_interval**: ステータス確認間隔（将来の自動確認用）
- **max_wait_time**: 最大待機時間（将来の自動確認用）

## 12. 運用フロー

### 12.1 標準的な復元フロー
```bash
# 1. 復元リクエスト送信（即座に完了）
python restore_script_main.py restore_request.csv REQ-RESTORE-001 --request-only

# 2. 48時間後、ダウンロード実行（ステータス確認も自動実行）
python restore_script_main.py restore_request.csv REQ-RESTORE-001 --download-only
```

### 12.2 部分復元完了時の動作
- 一部ファイルが復元完了、一部が処理中の場合
- `--download-only`実行時に完了分のみダウンロード
- 未完了分は次回実行時に自動的に確認・ダウンロード

### 12.3 エラー対応
```bash
# 復元依頼CSV修正後の再実行
python restore_script_main.py corrected_request.csv REQ-RESTORE-001-RETRY --request-only

# 復元期限切れ時の再リクエスト
python restore_script_main.py original_request.csv REQ-RESTORE-001-RENEW --request-only
```

## 13. 注意事項

### 13.1 データ整合性
- **復元成功**: 復元リクエスト送信 + ダウンロード + 配置が全て成功
- **部分成功**: 個別ファイルの成功・失敗を詳細記録

### 13.2 運用上の注意
- **復元期限**: 指定日数後にS3から自動削除
- **同名ファイル**: 既存ファイルは保護（上書きしない）
- **一時ファイル**: 処理完了後は自動削除

### 13.3 障害対応
- **復元リクエストエラー**: S3権限・接続確認
- **ダウンロードエラー**: 復元完了確認・権限確認
- **配置エラー**: 復元先ディレクトリの権限・容量確認

### 13.4 実機検証済み事項
- **0バイトファイル**: 正常にダウンロード・配置される
- **VPCエンドポイント**: 正常に通信可能
- **一時ファイル管理**: 自動クリーンアップが動作
- **同名ファイルスキップ**: 設定通りに動作