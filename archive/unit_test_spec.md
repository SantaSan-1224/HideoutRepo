# アーカイブスクリプト単体テスト仕様書

## 1. テスト概要

### 1.1 目的
`archive_script_main.py`の各機能の動作を検証し、品質を保証する。

### 1.2 テストフレームワーク
- **pytest**: メインテストフレームワーク
- **unittest.mock**: モック機能
- **tempfile**: 一時ファイル作成
- **pytest-cov**: カバレッジ測定

### 1.3 テスト実行環境
```bash
# 依存関係インストール
pip install pytest pytest-cov pytest-mock

# テスト実行
pytest tests/ -v --cov=archive_script_main --cov-report=html
```

## 2. テストファイル構成

```
tests/
├── __init__.py
├── test_archive_processor.py          # メインクラステスト
├── test_csv_validation.py             # CSV検証テスト
├── test_file_collection.py            # ファイル収集テスト
├── test_s3_upload.py                  # S3アップロードテスト
├── test_archive_postprocess.py        # アーカイブ後処理テスト
├── test_database.py                   # データベーステスト
├── test_error_handling.py             # エラーハンドリングテスト
├── test_config_management.py          # 設定管理テスト
├── conftest.py                        # テスト設定・フィクスチャ
└── test_data/                         # テストデータ
    ├── valid_directories.csv
    ├── invalid_directories.csv
    ├── mixed_directories.csv
    └── config/
        ├── valid_config.json
        └── invalid_config.json
```

## 3. テストケース仕様

### 3.1 ArchiveProcessor クラステスト

#### 3.1.1 初期化テスト
```python
class TestArchiveProcessorInit:
    def test_init_with_valid_config(self):
        """有効な設定ファイルでの初期化テスト"""
        # Given: 有効な設定ファイル
        # When: ArchiveProcessorを初期化
        # Then: 正常に初期化される
        
    def test_init_with_invalid_config(self):
        """無効な設定ファイルでの初期化テスト"""
        # Given: 無効な設定ファイル
        # When: ArchiveProcessorを初期化
        # Then: デフォルト設定で初期化される
        
    def test_init_with_missing_config(self):
        """設定ファイル不存在での初期化テスト"""
        # Given: 存在しない設定ファイルパス
        # When: ArchiveProcessorを初期化
        # Then: デフォルト設定で初期化される
```

### 3.2 CSV検証テスト

#### 3.2.1 正常系テスト
```python
class TestCSVValidation:
    def test_validate_csv_with_valid_directories(self, temp_csv_file):
        """有効なディレクトリのCSV検証テスト"""
        # Given: 存在する有効なディレクトリパスを含むCSV
        # When: validate_csv_input()を実行
        # Then: 全ディレクトリが有効リストに含まれる
        # And: エラーリストは空
        
    def test_validate_csv_with_header_detection(self, temp_csv_file):
        """ヘッダー行自動検出テスト"""
        # Given: "Directory Path"ヘッダーを含むCSV
        # When: validate_csv_input()を実行
        # Then: ヘッダー行がスキップされる
        
    def test_validate_csv_with_utf8_sig_encoding(self, temp_csv_file):
        """UTF-8-SIGエンコーディングテスト"""
        # Given: UTF-8-SIGエンコーディングのCSV
        # When: validate_csv_input()を実行
        # Then: 正常に読み込まれる
```

#### 3.2.2 異常系テスト
```python
class TestCSVValidationErrors:
    def test_validate_csv_with_nonexistent_directories(self, temp_csv_file):
        """存在しないディレクトリのテスト"""
        # Given: 存在しないディレクトリパスを含むCSV
        # When: validate_csv_input()を実行
        # Then: エラーリストに含まれる
        # And: エラー理由は"ディレクトリが存在しません"
        
    def test_validate_csv_with_invalid_characters(self, temp_csv_file):
        """不正文字を含むパステスト"""
        # Given: 不正文字(<>:"|?*)を含むパス
        # When: validate_csv_input()を実行
        # Then: エラーリストに含まれる
        # And: エラー理由は"不正な文字が含まれています"
        
    def test_validate_csv_with_too_long_path(self, temp_csv_file):
        """パス長制限テスト"""
        # Given: 260文字を超えるパス
        # When: validate_csv_input()を実行
        # Then: エラーリストに含まれる
        # And: エラー理由は"パスが長すぎます"
        
    def test_validate_csv_with_no_read_permission(self, temp_dir_no_permission):
        """読み取り権限なしディレクトリテスト"""
        # Given: 読み取り権限のないディレクトリ
        # When: validate_csv_input()を実行
        # Then: エラーリストに含まれる
        # And: エラー理由は"読み取り権限がありません"
```

### 3.3 ファイル収集テスト

#### 3.3.1 正常系テスト
```python
class TestFileCollection:
    def test_collect_files_from_single_directory(self, temp_directory_with_files):
        """単一ディレクトリからのファイル収集テスト"""
        # Given: ファイルを含む一時ディレクトリ
        # When: collect_files()を実行
        # Then: 全ファイルが収集される
        # And: ファイル情報（パス、サイズ、更新日時）が正しい
        
    def test_collect_files_with_subdirectories(self, temp_directory_with_subdirs):
        """サブディレクトリを含むファイル収集テスト"""
        # Given: サブディレクトリを含む一時ディレクトリ
        # When: collect_files()を実行
        # Then: サブディレクトリ内のファイルも収集される
        
    def test_collect_files_with_exclude_extensions(self, temp_directory_with_mixed_files):
        """除外拡張子テスト"""
        # Given: .tmp, .lock, .bakファイルを含むディレクトリ
        # When: collect_files()を実行
        # Then: 除外拡張子ファイルは収集されない
        
    def test_collect_files_with_size_limit(self, temp_directory_with_large_files):
        """ファイルサイズ制限テスト"""
        # Given: 最大サイズを超えるファイルを含むディレクトリ
        # When: collect_files()を実行
        # Then: サイズ超過ファイルは収集されない
```

### 3.4 S3アップロードテスト

#### 3.4.1 正常系テスト
```python
class TestS3Upload:
    @mock.patch('psycopg2.connect')
    def test_database_connection_failure(self, mock_connect):
        """データベース接続失敗テスト"""
        # Given: 接続エラーを発生させるモック
        # When: save_to_database()を実行
        # Then: 接続エラーが記録される
        # And: 処理は継続される（アーカイブ自体は成功のため）
        
    def test_invalid_requester_length(self):
        """無効な社員番号長さテスト"""
        # Given: 8桁以外の社員番号
        # When: データベース挿入を実行
        # Then: 制約違反エラーが発生
```

### 3.7 エラーハンドリングテスト

#### 3.7.1 エラーCSV生成テスト
```python
class TestErrorHandling:
    def test_csv_error_file_generation(self, temp_csv_path, csv_validation_errors):
        """CSV検証エラーファイル生成テスト"""
        # Given: CSV検証エラーリスト
        # When: generate_csv_error_file()を実行
        # Then: logs/配下に再試行用CSVが生成される
        # And: 元CSVと同じフォーマット
        # And: エラーパスのみが含まれる
        
    def test_archive_error_file_generation(self, temp_csv_path, archive_errors):
        """アーカイブエラーファイル生成テスト"""
        # Given: アーカイブエラーリスト
        # When: generate_error_csv()を実行
        # Then: logs/配下に再試行用CSVが生成される
        # And: 失敗ディレクトリが重複除去されて含まれる
        
    def test_error_statistics_logging(self, mixed_error_results):
        """エラー統計ログテスト"""
        # Given: 複数種類のエラーを含む結果
        # When: エラーCSVを生成
        # Then: エラー理由別の統計がログ出力される
```

#### 3.7.2 ログ機能テスト
```python
class TestLogging:
    def test_log_file_creation(self, temp_log_directory):
        """ログファイル作成テスト"""
        # Given: ログディレクトリ設定
        # When: ArchiveProcessorを初期化
        # Then: logs/archive_YYYYMMDD_HHMMSS.logが作成される
        
    def test_log_level_configuration(self):
        """ログレベル設定テスト"""
        # Given: DEBUG レベル設定
        # When: 各レベルのログを出力
        # Then: ファイルには全レベル、コンソールにはINFO以上が出力
        
    def test_japanese_filename_logging(self, japanese_filename):
        """日本語ファイル名ログテスト"""
        # Given: 日本語を含むファイル名
        # When: ログ出力
        # Then: 文字化けせずに記録される
```

### 3.8 設定管理テスト

#### 3.8.1 設定読み込みテスト
```python
class TestConfigManagement:
    def test_load_valid_config(self, valid_config_file):
        """有効設定ファイル読み込みテスト"""
        # Given: 有効な設定ファイル
        # When: load_config()を実行
        # Then: 全設定項目が正しく読み込まれる
        
    def test_load_config_with_missing_sections(self, incomplete_config_file):
        """不完全設定ファイル読み込みテスト"""
        # Given: 一部セクションが不足する設定ファイル
        # When: load_config()を実行
        # Then: デフォルト値でセクションが補完される
        
    def test_config_json_syntax_error(self, invalid_json_file):
        """JSON構文エラー設定ファイルテスト"""
        # Given: JSON構文エラーを含む設定ファイル
        # When: load_config()を実行
        # Then: デフォルト設定が使用される
        # And: エラーメッセージが出力される
        
    def test_config_encoding_handling(self, utf8_config_file):
        """設定ファイルエンコーディングテスト"""
        # Given: UTF-8エンコーディングの設定ファイル
        # When: load_config()を実行
        # Then: 正しく読み込まれる
```

### 3.9 統合テスト

#### 3.9.1 End-to-Endテスト
```python
class TestIntegration:
    @mock.patch('psycopg2.connect')
    @mock.patch('boto3.client')
    def test_full_archive_process_success(self, mock_boto3, mock_psycopg2, temp_test_environment):
        """完全なアーカイブ処理成功テスト"""
        # Given: 有効なCSV、設定ファイル、テストファイル
        # And: モックS3、データベース（成功レスポンス）
        # When: run()メソッドを実行
        # Then: 全処理が成功
        # And: 戻り値が0
        # And: 統計情報が正しく出力される
        
    @mock.patch('psycopg2.connect')
    @mock.patch('boto3.client')
    def test_full_archive_process_with_errors(self, mock_boto3, mock_psycopg2, mixed_test_environment):
        """エラーを含む完全アーカイブ処理テスト"""
        # Given: 有効・無効パスを含むCSV
        # And: 一部ファイルでS3エラーが発生するモック
        # When: run()メソッドを実行
        # Then: 有効ファイルは正常処理される
        # And: エラーCSVが生成される
        # And: 統計情報にエラー件数が含まれる
```

## 4. テストデータ仕様

### 4.1 CSVテストデータ

#### 4.1.1 有効ディレクトリCSV (`valid_directories.csv`)
```csv
Directory Path
{temp_dir}/valid1
{temp_dir}/valid2
{temp_dir}/valid3
```

#### 4.1.2 無効ディレクトリCSV (`invalid_directories.csv`)
```csv
Directory Path
/nonexistent/path
{temp_dir}/invalid<file>
{very_long_path_over_260_characters}
```

#### 4.1.3 混合ディレクトリCSV (`mixed_directories.csv`)
```csv
Directory Path
{temp_dir}/valid1
/nonexistent/path
{temp_dir}/valid2
{temp_dir}/invalid|file
```

### 4.2 設定ファイルテストデータ

#### 4.2.1 有効設定ファイル (`valid_config.json`)
```json
{
    "aws": {
        "region": "ap-northeast-1",
        "s3_bucket": "test-bucket",
        "storage_class": "DEEP_ARCHIVE"
    },
    "database": {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_password"
    },
    "request": {
        "requester": "12345678"
    },
    "file_server": {
        "archived_suffix": "_archived",
        "exclude_extensions": [".tmp", ".lock"]
    },
    "processing": {
        "max_file_size": 1048576,
        "retry_count": 2
    },
    "logging": {
        "log_directory": "test_logs",
        "log_level": "DEBUG"
    }
}
```

#### 4.2.2 無効設定ファイル (`invalid_config.json`)
```json
{
    "aws": {
        "region": "ap-northeast-1"
        // 意図的なJSON構文エラー
    }
}
```

### 4.3 テストファイル構造
```
test_data/
├── temp_directories/
│   ├── valid1/
│   │   ├── file1.txt
│   │   └── file2.pdf
│   ├── valid2/
│   │   ├── subdir/
│   │   │   └── file3.xlsx
│   │   └── file4.doc
│   └── valid3/
│       ├── file5.jpg
│       ├── file6.tmp      # 除外対象
│       └── large_file.bin # サイズ制限テスト用
```

## 5. フィクスチャ仕様

### 5.1 共通フィクスチャ (`conftest.py`)

```python
import pytest
import tempfile
import os
from pathlib import Path

@pytest.fixture
def temp_directory():
    """一時ディレクトリ作成フィクスチャ"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

@pytest.fixture
def temp_csv_file(temp_directory):
    """一時CSVファイル作成フィクスチャ"""
    csv_path = Path(temp_directory) / "test.csv"
    yield csv_path

@pytest.fixture
def valid_config():
    """有効設定辞書フィクスチャ"""
    return {
        "aws": {"region": "ap-northeast-1", "s3_bucket": "test-bucket"},
        "database": {"host": "localhost", "port": 5432},
        "request": {"requester": "12345678"},
        "file_server": {"archived_suffix": "_archived"},
        "processing": {"max_file_size": 1048576, "retry_count": 3},
        "logging": {"log_directory": "test_logs", "log_level": "INFO"}
    }

@pytest.fixture
def sample_files(temp_directory):
    """サンプルファイル作成フィクスチャ"""
    files = []
    for i in range(3):
        file_path = Path(temp_directory) / f"file{i}.txt"
        file_path.write_text(f"Content of file {i}")
        files.append({
            'path': str(file_path),
            'size': file_path.stat().st_size,
            'modified_time': datetime.datetime.now(),
            'directory': temp_directory
        })
    return files

@pytest.fixture
def mock_s3_success_results(sample_files):
    """S3アップロード成功結果フィクスチャ"""
    results = []
    for file_info in sample_files:
        results.append({
            'file_path': file_info['path'],
            'file_size': file_info['size'],
            'directory': file_info['directory'],
            'success': True,
            'error': None,
            's3_key': f"test/{Path(file_info['path']).name}",
            'modified_time': file_info['modified_time']
        })
    return results
```

## 6. モック仕様

### 6.1 S3クライアントモック
```python
@pytest.fixture
def mock_s3_client_success():
    """S3アップロード成功モック"""
    with mock.patch('boto3.client') as mock_client:
        mock_s3 = mock_client.return_value
        mock_s3.head_bucket.return_value = {}
        mock_s3.upload_file.return_value = None
        yield mock_s3

@pytest.fixture  
def mock_s3_client_with_retry():
    """S3アップロードリトライモック"""
    with mock.patch('boto3.client') as mock_client:
        mock_s3 = mock_client.return_value
        mock_s3.head_bucket.return_value = {}
        # 2回失敗後成功
        mock_s3.upload_file.side_effect = [
            Exception("Temporary error"),
            Exception("Temporary error"), 
            None
        ]
        yield mock_s3
```

### 6.2 データベースモック
```python
@pytest.fixture
def mock_db_connection_success():
    """データベース接続成功モック"""
    with mock.patch('psycopg2.connect') as mock_connect:
        mock_conn = mock_connect.return_value
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.executemany.return_value = None
        mock_cursor.rowcount = 2
        yield mock_conn
```

## 7. カバレッジ目標

### 7.1 カバレッジ目標値
- **ライン カバレッジ**: 90%以上
- **ブランチ カバレッジ**: 85%以上
- **関数 カバレッジ**: 95%以上

### 7.2 カバレッジ除外対象
```python
# pragma: no cover で除外
- ログ初期化の例外処理
- システム終了コード
- デバッグ用print文
```

## 8. テスト実行手順

### 8.1 基本テスト実行
```bash
# 全テスト実行
pytest tests/ -v

# カバレッジ付き実行
pytest tests/ -v --cov=archive_script_main --cov-report=html

# 特定テストクラス実行
pytest tests/test_csv_validation.py::TestCSVValidation -v

# 特定テストメソッド実行
pytest tests/test_csv_validation.py::TestCSVValidation::test_validate_csv_with_valid_directories -v
```

### 8.2 継続的テスト
```bash
# ファイル変更監視テスト
pytest-watch tests/

# 並列実行（pytest-xdist使用）
pytest tests/ -n auto
```

## 9. テスト環境構築

### 9.1 依存関係インストール
```bash
pip install pytest pytest-cov pytest-mock pytest-xdist pytest-watch
```

### 9.2 テストデータベース準備
```sql
-- テスト用データベース作成
CREATE DATABASE test_archive_system;

-- テスト用テーブル作成
\c test_archive_system
\i tests/test_data/test_schema.sql
```

### 9.3 テスト設定ファイル (`pytest.ini`)
```ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
    --disable-warnings
markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow running tests
```

## 10. 品質基準

### 10.1 テスト品質チェック
- **テストケース網羅性**: 正常系・異常系・境界値を網羅
- **アサーション品質**: 具体的で意味のある検証
- **テストデータ品質**: 本番に近いリアルなデータ
- **モック品質**: 実際の動作を正確にシミュレート

### 10.2 レビュー観点
- **テスト名**: 何をテストするかが明確
- **Given-When-Then**: 構造が明確
- **独立性**: テスト間の依存関係なし
- **再実行可能性**: 何度実行しても同じ結果

### 10.3 継続的改善
- **失敗テストの分析**: 原因調査と改善
- **テスト実行時間監視**: 遅いテストの最適化
- **カバレッジ向上**: 未カバー部分の特定と対応
- **テストメンテナンス**: 仕様変更時のテスト更新

---

この単体テスト仕様書に基づいて、品質の高いテストコードを実装し、アーカイブスクリプトの信頼性を確保することができます。patch('boto3.client')
    def test_s3_upload_success(self, mock_boto3_client, sample_files):
        """S3アップロード成功テスト"""
        # Given: モックS3クライアント（成功レスポンス）
        # And: アップロード対象ファイルリスト
        # When: archive_to_s3()を実行
        # Then: 全ファイルのアップロードが成功
        # And: S3キーが正しく生成される
        
    def test_s3_key_generation_unc_path(self):
        """UNCパスのS3キー生成テスト"""
        # Given: UNCパス（\\server\share\file.txt）
        # When: _generate_s3_key()を実行
        # Then: server/share/file.txt 形式のキーが生成される
        
    def test_s3_key_generation_local_path(self):
        """ローカルパスのS3キー生成テスト"""
        # Given: ローカルパス（C:\path\file.txt）
        # When: _generate_s3_key()を実行
        # Then: local_c/path/file.txt 形式のキーが生成される
        
    def test_storage_class_auto_conversion(self):
        """ストレージクラス自動変換テスト"""
        # Given: GLACIER_DEEP_ARCHIVE設定
        # When: _validate_storage_class()を実行
        # Then: DEEP_ARCHIVEに変換される
```

#### 3.4.2 異常系テスト
```python
class TestS3UploadErrors:
    @mock.patch('boto3.client')
    def test_s3_upload_with_access_denied(self, mock_boto3_client, sample_files):
        """S3アクセス拒否エラーテスト"""
        # Given: AccessDeniedエラーを返すモックS3クライアント
        # When: archive_to_s3()を実行
        # Then: リトライが実行される
        # And: 最終的に失敗として記録される
        
    @mock.patch('boto3.client')
    def test_s3_upload_with_retry_success(self, mock_boto3_client, sample_files):
        """S3アップロードリトライ成功テスト"""
        # Given: 2回失敗後3回目で成功するモックS3クライアント
        # When: archive_to_s3()を実行
        # Then: 3回目で成功として記録される
        
    def test_s3_connection_failure(self):
        """S3接続失敗テスト"""
        # Given: 無効なS3設定
        # When: _initialize_s3_client()を実行
        # Then: 接続エラーが発生する
```

### 3.5 アーカイブ後処理テスト

#### 3.5.1 正常系テスト
```python
class TestArchivePostProcess:
    def test_create_archived_files_success(self, temp_files, mock_s3_success_results):
        """アーカイブ後処理成功テスト"""
        # Given: S3アップロード成功ファイルリスト
        # And: 一時テストファイル
        # When: create_archived_files()を実行
        # Then: 空ファイルが作成される
        # And: 元ファイルが削除される
        # And: archive_completed=Trueが設定される
        
    def test_empty_file_creation(self, temp_file):
        """空ファイル作成テスト"""
        # Given: 元ファイル
        # When: 空ファイルを作成
        # Then: {元ファイル名}_archivedファイルが作成される
        # And: ファイルサイズが0バイト
        
    def test_original_file_deletion(self, temp_file):
        """元ファイル削除テスト"""
        # Given: 元ファイルと空ファイル
        # When: 元ファイルを削除
        # Then: 元ファイルが存在しない
        # And: 空ファイルは残存
```

#### 3.5.2 異常系テスト
```python
class TestArchivePostProcessErrors:
    def test_empty_file_creation_failure(self, readonly_directory):
        """空ファイル作成失敗テスト"""
        # Given: 読み取り専用ディレクトリ内のファイル
        # When: create_archived_files()を実行
        # Then: 空ファイル作成が失敗
        # And: 元ファイルは削除されない
        # And: success=Falseが設定される
        
    def test_original_file_deletion_failure(self, locked_file):
        """元ファイル削除失敗テスト"""
        # Given: 他プロセスで使用中のファイル
        # When: create_archived_files()を実行
        # Then: 元ファイル削除が失敗
        # And: 空ファイルがクリーンアップされる
        # And: success=Falseが設定される
```

### 3.6 データベーステスト

#### 3.6.1 正常系テスト
```python
class TestDatabase:
    @mock.patch('psycopg2.connect')
    def test_database_insert_success(self, mock_connect, sample_archive_results):
        """データベース挿入成功テスト"""
        # Given: アーカイブ完了ファイルリスト
        # And: モックデータベース接続
        # When: save_to_database()を実行
        # Then: 正しいSQLが実行される
        # And: 全レコードが挿入される
        
    def test_s3_url_generation(self):
        """S3 URL生成テスト"""
        # Given: S3キーとバケット名
        # When: S3 URLを生成
        # Then: s3://bucket/key 形式のURLが生成される
        
    @mock.patch('psycopg2.connect')
    def test_database_transaction_rollback(self, mock_connect):
        """データベーストランザクションロールバックテスト"""
        # Given: 挿入時にエラーが発生するモック
        # When: save_to_database()を実行
        # Then: トランザクションがロールバックされる
        # And: エラーログが出力される
```

#### 3.6.2 異常系テスト
```python
class TestDatabaseErrors:
    @mock.