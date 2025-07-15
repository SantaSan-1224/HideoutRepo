# アーカイブシステム詳細設計書

## 1. システム概要

### 1.1 システム目的

企業内ファイルサーバ（FSx for Windows File Server）上のファイルを、ユーザー依頼に基づいて AWS S3（Glacier Deep Archive）にアーカイブし、履歴管理・復元機能を提供する。

### 1.2 システム構成図

```
[FASTワークフロー] → [アーカイブ処理サーバ] → [FSx/S3/PostgreSQL]
     ↓                        ↓
[CSVアップロード]         [Streamlitアプリ]
```

### 1.3 技術スタック

- **処理サーバ**: AWS EC2（4vCPU、16GB メモリ）
- **言語**: Python 3.x
- **データベース**: PostgreSQL
- **Web アプリ**: Streamlit
- **AWS 連携**: boto3、AWS CLI
- **ファイルサーバ**: FSx for Windows File Server

## 2. データベース設計

### 2.1 テーブル設計

#### 2.1.1 archive_history テーブル

| カラム名           | データ型    | 制約                                | 説明                     |
| ------------------ | ----------- | ----------------------------------- | ------------------------ |
| id                 | BIGSERIAL   | PRIMARY KEY                         | 主キー（自動採番）       |
| request_id         | VARCHAR(50) | NOT NULL                            | 依頼 ID                  |
| requester          | VARCHAR(7)  | NOT NULL, CHECK                     | 依頼者（社員番号 7 桁）  |
| request_date       | TIMESTAMP   | NOT NULL, DEFAULT CURRENT_TIMESTAMP | 依頼日時                 |
| approval_date      | TIMESTAMP   |                                     | 承認日時                 |
| process_status     | VARCHAR(20) | NOT NULL, DEFAULT 'pending'         | 処理状況                 |
| original_file_path | TEXT        | NOT NULL                            | 元ファイルパス           |
| s3_path            | TEXT        |                                     | S3 パス                  |
| archive_date       | TIMESTAMP   |                                     | アーカイブ日時           |
| file_size          | BIGINT      | CHECK >= 0                          | ファイルサイズ（バイト） |
| process_result     | TEXT        |                                     | 処理結果・エラー詳細     |
| created_at         | TIMESTAMP   | NOT NULL, DEFAULT CURRENT_TIMESTAMP | 作成日時                 |
| updated_at         | TIMESTAMP   | NOT NULL, DEFAULT CURRENT_TIMESTAMP | 更新日時                 |

#### 2.1.2 処理状況の値

- `pending`: 承認待ち
- `approved`: 承認済み
- `processing`: 処理中
- `completed`: 完了
- `error`: エラー
- `cancelled`: キャンセル

### 2.2 インデックス設計

```sql
-- 検索用インデックス
CREATE INDEX idx_archive_history_requester ON archive_history(requester);
CREATE INDEX idx_archive_history_request_date ON archive_history(request_date);
CREATE INDEX idx_archive_history_process_status ON archive_history(process_status);
CREATE INDEX idx_archive_history_request_id ON archive_history(request_id);

-- ファイルパス検索用（部分一致）
CREATE INDEX idx_archive_history_original_file_path ON archive_history USING gin(original_file_path gin_trgm_ops);

-- 複合インデックス
CREATE INDEX idx_archive_history_requester_date ON archive_history(requester, request_date);
CREATE INDEX idx_archive_history_status_date ON archive_history(process_status, request_date);
```

## 3. アーカイブスクリプト設計

### 3.1 クラス設計

#### 3.1.1 ArchiveProcessor クラス

**責務**: アーカイブ処理の全体制御

**主要メソッド**:

```python
class ArchiveProcessor:
    def __init__(config_path: str)
    def load_config(config_path: str) -> Dict
    def setup_logger() -> logging.Logger
    def validate_csv_input(csv_path: str) -> Tuple[List[str], List[Dict]]
    def collect_files(directories: List[str]) -> List[Dict]
    def archive_to_s3(files: List[Dict]) -> List[Dict]
    def create_archived_files(results: List[Dict]) -> List[Dict]
    def save_to_database(results: List[Dict]) -> None
    def generate_error_csv(error_items: List[Dict], csv_path: str) -> str
    def run(csv_path: str, request_id: str) -> int
```

### 3.2 処理フロー詳細

#### 3.2.1 CSV 読み込み・検証処理

```
1. CSVファイル読み込み（UTF-8-SIG対応）
2. 行単位での処理
   - 空行スキップ
   - ヘッダー行検出・スキップ
   - パス正規化
3. ディレクトリ検証
   - 存在チェック
   - アクセス権限チェック
   - パス長制限チェック
   - 不正文字チェック
4. エラー項目記録
5. エラーCSV生成（必要時）
```

#### 3.2.2 ファイル収集処理

```
1. 各ディレクトリをos.walkで走査
2. ファイルごとの処理
   - 除外拡張子チェック
   - ファイルサイズ制限チェック
   - ファイル情報取得（サイズ、更新日時）
3. ファイル情報リスト生成
```

#### 3.2.3 S3 アップロード処理

```
1. boto3 S3クライアント初期化
2. VPCエンドポイント経由接続
3. ファイル単位でのアップロード
   - Glacier Deep Archive直接指定
   - エラーハンドリング・リトライ
   - 進捗ログ出力
4. アップロード結果記録
```

#### 3.2.4 アーカイブ後処理

```
1. 元ファイル削除
2. 空ファイル作成（_archived.txt）
3. 処理結果記録
```

#### 3.2.5 データベース登録処理

```
1. PostgreSQL接続
2. トランザクション開始
3. archive_historyテーブル挿入
4. コミット・ロールバック処理
```

### 3.3 エラーハンドリング設計

#### 3.3.1 エラーレベル分類

- **CRITICAL**: システム停止が必要なエラー
- **ERROR**: 個別ファイル処理失敗
- **WARNING**: 警告（処理継続可能）
- **INFO**: 一般的な処理情報

#### 3.3.2 エラー CSV 出力仕様

**再試行用 CSV**: `{元ファイル名}_retry_{YYYYMMDD_HHMMSS}.csv`

- 元 CSV と同じフォーマット（再実行可能）
- エラーが発生したパスのみを記録
- 詳細なエラー理由はログファイルに出力

```csv
Directory Path
\\invalid\path
\\another\invalid\path
```

**アーカイブエラー**: `{元ファイル名}_archive_errors_{YYYYMMDD_HHMMSS}.csv`

```csv
ファイルパス,エラー理由,ファイルサイズ,ディレクトリ
C:\temp\file.txt,S3アップロード失敗,1024,C:\temp
```

**エラー理由のログ出力例**:

```
2025-07-15 10:30:25 - archive_processor - ERROR - 行 2: ディレクトリが存在しません ✗
2025-07-15 10:30:25 - archive_processor - ERROR - 行 3: 不正な文字が含まれています: | ✗
2025-07-15 10:30:25 - archive_processor - ERROR - 行 4: 読み取り権限がありません ✗
```

### 3.4 設定ファイル設計

#### 3.4.1 設定項目

```json
{
  "aws": {
    "region": "ap-northeast-1",
    "s3_bucket": "your-archive-bucket",
    "storage_class": "GLACIER_DEEP_ARCHIVE",
    "vpc_endpoint_url": "https://..."
  },
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "archive_system",
    "user": "postgres",
    "password": "password"
  },
  "file_server": {
    "base_path": "\\\\server\\share\\",
    "archived_suffix": "_archived.txt",
    "exclude_extensions": [".tmp", ".lock", ".bak"]
  },
  "processing": {
    "max_file_size": 10737418240,
    "chunk_size": 8388608,
    "retry_count": 3
  },
  "logging": {
    "log_directory": "logs",
    "log_level": "INFO"
  }
}
```

## 4. Streamlit アプリケーション設計

### 4.1 画面構成

#### 4.1.1 メイン画面

- **ヘッダー**: アプリケーション名、現在日時
- **検索フィルター**: 日付範囲、依頼者、処理状況
- **履歴一覧**: ページネーション対応テーブル
- **集計情報**: ファイル数、総サイズ等の統計
- **エクスポート**: Excel/CSV ダウンロードボタン

#### 4.1.2 検索・フィルタリング機能

```python
# フィルター項目
- 依頼日範囲（from_date, to_date）
- 依頼者（社員番号）
- 処理状況（複数選択可能）
- ファイルパス（部分一致検索）
```

#### 4.1.3 データ表示項目

- 依頼 ID
- 依頼者
- 依頼日時
- 処理状況
- 元ファイルパス（省略表示）
- ファイルサイズ
- アーカイブ日時
- 処理結果

### 4.2 機能詳細

#### 4.2.1 履歴検索機能

```sql
-- 基本検索クエリ
SELECT id, request_id, requester, request_date,
       process_status, original_file_path, file_size, archive_date
FROM archive_history
WHERE request_date BETWEEN %s AND %s
  AND requester LIKE %s
  AND process_status IN %s
ORDER BY request_date DESC
LIMIT %s OFFSET %s;
```

#### 4.2.2 集計機能

```sql
-- 集計クエリ例
SELECT
    COUNT(*) as total_files,
    SUM(file_size) as total_size,
    COUNT(CASE WHEN process_status = 'completed' THEN 1 END) as completed_files,
    COUNT(CASE WHEN process_status = 'error' THEN 1 END) as error_files
FROM archive_history
WHERE request_date BETWEEN %s AND %s;
```

#### 4.2.3 エクスポート機能

- **Excel 形式**: `pandas.to_excel()`使用
- **CSV 形式**: `pandas.to_csv()`使用
- **ファイル名**: `archive_history_{YYYYMMDD_HHMMSS}.{xlsx|csv}`

## 5. 復元スクリプト設計

### 5.1 復元処理フロー

```
1. 復元依頼CSV読み込み
2. S3復元リクエスト送信
3. 復元完了待機
4. ファイルダウンロード
5. 指定ディレクトリに配置
6. 処理結果ログ出力
```

### 5.2 復元 CSV 仕様

```csv
復元対象パス,復元先ディレクトリ
archive/path/to/file.txt,C:\restored\files\
archive/path/to/file2.txt,C:\restored\files\
```

### 5.3 復元処理詳細設計

**TODO**: 復元スクリプトの詳細設計（後続で実装）

## 6. 運用・監視設計

### 6.1 ログ設計

#### 6.1.1 ログレベル

- **DEBUG**: デバッグ情報（ファイルのみ）
- **INFO**: 処理進捗情報
- **WARNING**: 警告（処理継続可能）
- **ERROR**: エラー（個別処理失敗）
- **CRITICAL**: 重大エラー（システム停止）

#### 6.1.2 ログ出力先

- **コンソール**: INFO レベル以上
- **ログファイル**: DEBUG レベル以上
- **ファイル名**: `logs/archive_{YYYYMMDD_HHMMSS}.log`

#### 6.1.3 ログローテーション

- **保持期間**: 30 日
- **ファイルサイズ制限**: 100MB
- **圧縮**: gzip 圧縮

### 6.2 パフォーマンス考慮事項

#### 6.2.1 想定処理規模

- **月間依頼件数**: 100-200 件
- **月間処理ファイル数**: 10,000-20,000 ファイル
- **並行処理**: 現時点では実装しない（2 人 1 組運用）

#### 6.2.2 最適化ポイント

- データベースのコミット方式（ディレクトリ単位 vs 一括）
- S3 アップロードのチャンクサイズ
- ファイル収集時のメモリ使用量

## 7. セキュリティ設計

### 7.1 認証・認可

- **アクセス権限**: 運用管理者のみ
- **依頼者権限**: 企業社員番号 7 桁による識別
- **ファイルアクセス**: 部署ごとに独立したファイルサーバ構成

### 7.2 データ保護

- **通信暗号化**: VPC エンドポイント経由の HTTPS 通信
- **データベース接続**: SSL 接続（設定による）
- **ログ保護**: 機密情報のマスキング

## 8. 今後の拡張計画

### 8.1 短期拡張（3 ヶ月以内）

- [ ] 復元スクリプトの実装
- [ ] 進捗確認機能の実装
- [ ] 通知機能の検討

### 8.2 中期拡張（6 ヶ月以内）

- [ ] 並行処理対応
- [ ] パフォーマンス最適化
- [ ] 監視・アラート機能

### 8.3 長期拡張（1 年以内）

- [ ] Web UI での依頼受付機能
- [ ] 自動スケジューリング機能
- [ ] レポート機能の拡充

## 9. 運用手順書

### 9.1 アーカイブ処理手順

```bash
# 1. 設定ファイル確認
python -m json.tool config/archive_config.json

# 2. CSVファイル準備確認
head -5 /path/to/archive_request.csv

# 3. アーカイブ処理実行
python archive_script_main.py /path/to/archive_request.csv REQ-YYYY-XXX

# 4. 処理結果確認
ls -la logs/
tail -f logs/archive_YYYYMMDD_HHMMSS.log
```

### 9.2 トラブルシューティング

- **CSV 読み込みエラー**: 文字エンコーディング確認
- **S3 接続エラー**: VPC エンドポイント・認証情報確認
- **データベース接続エラー**: 接続設定・ネットワーク確認
- **ファイルアクセスエラー**: 権限・パス存在確認

## 10. 更新履歴

| 日付       | バージョン | 更新内容 | 更新者             |
| ---------- | ---------- | -------- | ------------------ |
| 2025-07-14 | 1.0        | 初版作成 | システム開発チーム |

---

**注意**: この設計書は実装の進捗に応じて随時更新されます。
