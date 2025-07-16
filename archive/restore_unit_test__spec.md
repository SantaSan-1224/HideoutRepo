```
tests/
├── __init__.py
├── test_restore_processor.py           # メインクラステスト
├── test_restore_csv_validation.py      # 復元CSV検証テスト
├── test_database_lookup.py             # データベース検索テスト
├── test_s3_restore_request.py          # S3復元リクエストテスト
├── test_restore_status_check.py        # 復元ステータス確認テスト
├── test_download_and_place.py          # ダウンロード・配置テスト
├── test_status_management.py           # ステータス管理テスト
├── test_restore_error_handling.py      # エラーハンドリングテスト
├── test_restore_config_management.py   # 設定管理テスト
├── conftest.py                         # テスト設定・フィクスチャ
└── test_data/                          # テストデータ
    ├── restore_requests/
    │   ├── valid_restore_request.csv
    │   ├── invalid_restore_request.csv
    │   └── mixed_restore_request.csv
    ├── status_files/
    │   ├── restore_status_sample.json
    │   └── restore_status_partial.json
    └── config/
        ├── valid_restore_config.json
        └── invalid_restore_config.json
```

## 3. テストケース仕様

### 3.1 RestoreProcessor クラステスト

#### 3.1.1 初期化テスト
```python
class TestRestoreProcessorInit:
    def test_init_with_valid_config(self):
        """有効な設定ファイルでの初期化テスト"""
        # Given: 有効な設定ファイル
        # When: RestoreProcessorを初期化
        # Then: 正常に初期化される
        # And: 復元設定が正しく読み込まれる
        
    def test_init_with_missing_restore_config(self):
        """復元設定不足での初期化テスト"""
        # Given: restore セクションが不足する設定ファイル
        # When: RestoreProcessorを初期化
        # Then: デフォルト復元設定で初期化される
        
    def test_stats_initialization(self):
        """統計情報初期化テスト"""
        # Given: RestoreProcessor初期化
        # When: statsプロパティを確認
        # Then: 全統計項目が0で初期化されている
```

### 3.2 復元CSV検証テスト

#### 3.2.1 正常系テスト
```python
class TestRestoreCSVValidation:
    def test_validate_csv_with_valid_requests(self, temp_restore_csv_file):
        """有効な復元依頼のCSV検証テスト"""
        # Given: 有効な元ファイルパスと復元先を含むCSV
        # When: validate_csv_input()を実行
        # Then: 全復元依頼が有効リストに含まれる
        # And: エラーリストは空
        
    def test_validate_csv_with_japanese_paths(self, temp_restore_csv_file):
        """日本語パスの復元依頼テスト"""
        # Given: 日本語を含むファイルパスのCSV
        # When: validate_csv_input()を実行
        # Then: 正常に処理される
        
    def test_validate_csv_with_unc_and_local_paths(self, temp_restore_csv_file):
        """UNCパスとローカルパス混在テスト"""
        # Given: UNCパスとローカルパスを含むCSV
        # When: validate_csv_input()を実行
        # Then: 両方とも正常に処理される
```

#### 3.2.2 異常系テスト
```python
class TestRestoreCSVValidationErrors:
    def test_validate_csv_with_insufficient_columns(self, temp_restore_csv_file):
        """カラム数不足テスト"""
        # Given: 1カラムのみのCSV行
        # When: validate_csv_input()を実行
        # Then: エラーリストに含まれる
        # And: エラー理由は"カラム数が不足しています"
        
    def test_validate_csv_with_nonexistent_restore_directory(self, temp_restore_csv_file):
        """存在しない復元先ディレクトリテスト"""
        # Given: 存在しない復元先ディレクトリを含むCSV
        # When: validate_csv_input()を実行
        # Then: エラーリストに含まれる
        # And: エラー理由は"復元先ディレクトリが存在しません"
        
    def test_validate_csv_with_no_write_permission(self, temp_readonly_directory):
        """書き込み権限なしディレクトリテスト"""
        # Given: 書き込み権限のない復元先ディレクトリ
        # When: validate_csv_input()を実行
        # Then: エラーリストに含まれる
        # And: エラー理由は"復元先ディレクトリへの書き込み権限がありません"
        
    def test_validate_csv_with_too_long_path(self, temp_restore_csv_file):
        """パス長制限テスト"""
        # Given: 260文字を超える元ファイルパス
        # When: validate_csv_input()を実行
        # Then: エラーリストに含まれる
        # And: エラー理由は"ファイルパスが長すぎます"
```

### 3.3 データベース検索テスト

#### 3.3.1 正常系テスト
```python
class TestDatabaseLookup:
    @mock.patch('psycopg2.connect')
    def test_lookup_s3_paths_success(self, mock_connect, sample_restore_requests):
        """S3パス検索成功テスト"""
        # Given: アーカイブ履歴が存在するデータベース
        # And: 有効な復元依頼リスト
        # When: lookup_s3_paths_from_database()を実行
        # Then: 対応するS3パスが取得される
        # And: bucket、keyが正しく分解される
        
    @mock.patch('psycopg2.connect')
    def test_lookup_s3_paths_with_archive_date(self, mock_connect, sample_restore_requests):
        """アーカイブ日時付きS3パス検索テスト"""
        # Given: アーカイブ日時を含むデータベースレスポンス
        # When: lookup_s3_paths_from_database()を実行
        # Then: archive_dateが正しく設定される
        
    def test_extract_bucket_from_s3_path(self):
        """S3パスからバケット名抽出テスト"""
        # Given: s3://bucket-name/key/path 形式のS3パス
        # When: _extract_bucket_from_s3_path()を実行
        # Then: bucket-name が抽出される
        
    def test_extract_key_from_s3_path(self):
        """S3パスからキー抽出テスト"""
        # Given: s3://bucket-name/key/path 形式のS3パス
        # When: _extract_key_from_s3_path()を実行
        # Then: key/path が抽出される
```

#### 3.3.2 異常系テスト
```python
class TestDatabaseLookupErrors:
    @mock.patch('psycopg2.connect')
    def test_lookup_s3_paths_not_found(self, mock_connect, sample_restore_requests):
        """S3パス見つからずテスト"""
        # Given: データベースに存在しない元ファイルパス
        # When: lookup_s3_paths_from_database()を実行
        # Then: s3_path=None が設定される
        # And: error="データベースにアーカイブ履歴が見つかりません"
        
    @mock.patch('psycopg2.connect')
    def test_database_connection_failure(self, mock_connect, sample_restore_requests):
        """データベース接続失敗テスト"""
        # Given: 接続エラーを発生させるモック
        # When: lookup_s3_paths_from_database()を実行
        # Then: 全リクエストにエラーマークが設定される
        # And: エラー内容にDB接続エラーが記録される
```

### 3.4 S3復元リクエストテスト

#### 3.4.1 正常系テスト
```python
class TestS3RestoreRequest:
    @mock.patch('boto3.client')
    def test_request_restore_success(self, mock_boto3_client, sample_restore_requests_with_s3):
        """S3復元リクエスト成功テスト"""
        # Given: 正常なS3クライアントモック
        # And: S3パス情報を含む復元依頼リスト
        # When: request_restore()を実行
        # Then: restore_object APIが正しいパラメータで呼ばれる
        # And: restore_status="requested"が設定される
        
    @mock.patch('boto3.client')
    def test_request_restore_with_different_tiers(self, mock_boto3_client, sample_restore_requests_with_s3):
        """復元ティア指定テスト"""
        # Given: Standard/Expedited/Bulk の復元ティア設定
        # When: request_restore()を実行
        # Then: 指定されたティアでリクエストが送信される
        
    @mock.patch('boto3.client')
    def test_request_restore_already_in_progress(self, mock_boto3_client, sample_restore_requests_with_s3):
        """復元既に進行中テスト"""
        # Given: RestoreAlreadyInProgressエラーを返すS3クライアント
        # When: request_restore()を実行
        # Then: restore_status="already_in_progress"が設定される
        # And: 成功扱いとして処理される
```

#### 3.4.2 異常系テスト
```python
class TestS3RestoreRequestErrors:
    @mock.patch('boto3.client')
    def test_request_restore_with_invalid_object_state(self, mock_boto3_client, sample_restore_requests_with_s3):
        """無効なオブジェクト状態エラーテスト"""
        # Given: InvalidObjectStateエラーを返すS3クライアント
        # When: request_restore()を実行
        # Then: restore_status="failed"が設定される
        # And: エラー内容が記録される
        
    def test_request_restore_with_incomplete_s3_info(self, sample_restore_requests_incomplete):
        """S3情報不完全テスト"""
        # Given: bucket または key が不足する復元依頼
        # When: request_restore()を実行
        # Then: restore_status="failed"が設定される
        # And: エラー理由は"S3パス情報が不完全です"
```

### 3.5 復元ステータス確認テスト

#### 3.5.1 正常系テスト
```python
class TestRestoreStatusCheck:
    @mock.patch('boto3.client')
    def test_check_restore_completion_success(self, mock_boto3_client, sample_restore_requests_pending):
        """復元完了確認成功テスト"""
        # Given: ongoing-request="false"を返すS3クライアント
        # When: check_restore_completion()を実行
        # Then: restore_status="completed"が設定される
        # And: restore_completed_timeが記録される
        
    @mock.patch('boto3.client')
    def test_check_restore_in_progress(self, mock_boto3_client, sample_restore_requests_pending):
        """復元進行中確認テスト"""
        # Given: ongoing-request="true"を返すS3クライアント
        # When: check_restore_completion()を実行
        # Then: restore_status="in_progress"が設定される
        
    @mock.patch('boto3.client')
    def test_check_restore_pending(self, mock_boto3_client, sample_restore_requests_pending):
        """復元待機中確認テスト"""
        # Given: Restoreヘッダーなしを返すS3クライアント
        # When: check_restore_completion()を実行
        # Then: restore_status="pending"が設定される
        
    def test_extract_restore_expiry_date(self):
        """復元有効期限抽出テスト"""
        # Given: expiry-dateを含むRestoreヘッダー
        # When: 有効期限を抽出
        # Then: 正しい日時が抽出される
```

#### 3.5.2 異常系テスト
```python
class TestRestoreStatusCheckErrors:
    @mock.patch('boto3.client')
    def test_check_restore_no_such_key(self, mock_boto3_client, sample_restore_requests_pending):
        """キー見つからずエラーテスト"""
        # Given: NoSuchKeyエラーを返すS3クライアント
        # When: check_restore_completion()を実行
        # Then: restore_status="failed"が設定される
        # And: エラー理由は"S3にファイルが見つかりません"
        
    @mock.patch('boto3.client')
    def test_check_restore_invalid_object_state(self, mock_boto3_client, sample_restore_requests_pending):
        """無効なオブジェクト状態エラーテスト"""
        # Given: InvalidObjectStateエラーを返すS3クライアント
        # When: check_restore_completion()を実行
        # Then: restore_status="failed"が設定される
        # And: エラー理由は"オブジェクトがGlacierストレージクラスではありません"
```

### 3.6 ダウンロード・配置テスト

#### 3.6.1 正常系テスト
```python
class TestDownloadAndPlace:
    @mock.patch('boto3.client')
    def test_download_and_place_success(self, mock_boto3_client, sample_completed_requests, temp_restore_directory):
        """ダウンロード・配置成功テスト"""
        # Given: 復元完了ファイルリスト
        # And: 正常なS3ダウンロードモック
        # When: download_and_place_files()を実行
        # Then: ファイルが正しくダウンロード・配置される
        # And: download_status="completed"が設定される
        
    @mock.patch('boto3.client')
    def test_download_zero_byte_file(self, mock_boto3_client, sample_zero_byte_request, temp_restore_directory):
        """0バイトファイルダウンロードテスト"""
        # Given: 0バイトの復元完了ファイル
        # When: download_and_place_files()を実行
        # Then: 正常にダウンロード・配置される
        # And: ファイルサイズが0バイトとして記録される
        
    def test_skip_existing_files(self, sample_completed_requests, temp_restore_directory_with_existing_file):
        """既存ファイルスキップテスト"""
        # Given: 同名ファイルが既に存在する復元先
        # And: skip_existing_files=true設定
        # When: download_and_place_files()を実行
        # Then: ダウンロードがスキップされる
        # And: download_status="skipped"が設定される
        
    def test_temp_file_cleanup_success(self, sample_completed_requests, temp_restore_directory):
        """一時ファイルクリーンアップ成功テスト"""
        # Given: 正常なダウンロード・配置処理
        # When: download_and_place_files()を実行
        # Then: 一時ファイルが自動削除される
        # And: temp_downloadsディレクトリが空になる
```

#### 3.6.2 異常系テスト
```python
class TestDownloadAndPlaceErrors:
    @mock.patch('boto3.client')
    def test_download_file_with_retry_failure(self, mock_boto3_client, sample_completed_requests):
        """ダウンロードリトライ失敗テスト"""
        # Given: 最大リトライ回数まで失敗するS3クライアント
        # When: download_and_place_files()を実行
        # Then: download_status="failed"が設定される
        # And: エラー内容に"最大リトライ回数到達"が含まれる
        
    def test_download_file_access_denied(self, sample_completed_requests):
        """ダウンロードアクセス拒否テスト"""
        # Given: AccessDeniedエラーを返すS3クライアント
        # When: _download_file_with_retry()を実行
        # Then: リトライせずに失敗する
        # And: エラー理由は"AccessDenied"
        
    def test_place_file_permission_error(self, sample_temp_file, readonly_destination):
        """ファイル配置権限エラーテスト"""
        # Given: 読み取り専用の配置先ディレクトリ
        # When: _place_file_to_destination()を実行
        # Then: success=False が返される
        # And: エラー理由は"権限エラー"
        
    def test_place_file_destination_not_exist(self, sample_temp_file):
        """配置先ディレクトリ不存在テスト"""
        # Given: 存在しない配置先ディレクトリ
        # When: _place_file_to_destination()を実行
        # Then: success=False が返される
        # And: エラー理由は"配置先ディレクトリが存在しません"
```

### 3.7 ステータス管理テスト

#### 3.7.1 正常系テスト
```python
class TestStatusManagement:
    def test_save_restore_status_success(self, sample_restore_requests, temp_log_directory):
        """復元ステータス保存成功テスト"""
        # Given: 復元リクエストリストとログディレクトリ
        # When: _save_restore_status()を実行
        # Then: JSONファイルが正しく保存される
        # And: ファイル名は restore_status_{request_id}.json
        
    def test_load_restore_status_success(self, sample_status_file):
        """復元ステータス読み込み成功テスト"""
        # Given: 有効な復元ステータスJSONファイル
        # When: _load_restore_status()を実行
        # Then: 復元リクエストリストが正しく読み込まれる
        
    def test_load_restore_status_not_found(self):
        """復元ステータスファイル不存在テスト"""
        # Given: 存在しないrequest_id
        # When: _load_restore_status()を実行
        # Then: 空リストが返される
        # And: エラーログが出力される
        
    def test_status_file_json_format(self, sample_restore_requests):
        """ステータスファイルJSON形式テスト"""
        # Given: 複雑な復元リクエストデータ
        # When: JSON保存・読み込みを実行
        # Then: 全データが正確に復元される
        # And: 日時データがISO形式で保存される
```

#### 3.7.2 異常系テスト
```python
class TestStatusManagementErrors:
    def test_save_restore_status_permission_error(self, sample_restore_requests, readonly_log_directory):
        """ステータス保存権限エラーテスト"""
        # Given: 書き込み権限のないログディレクトリ
        # When: _save_restore_status()を実行
        # Then: エラーログが出力される
        # And: 例外は発生しない（処理継続）
        
    def test_load_restore_status_corrupted_json(self, corrupted_status_file):
        """破損したJSONファイル読み込みテスト"""
        # Given: JSON構文エラーのあるステータスファイル
        # When: _load_restore_status()を実行
        # Then: 空リストが返される
        # And: エラーログが出力される
```

### 3.8 実行モードテスト

#### 3.8.1 復元リクエスト送信モードテスト
```python
class TestRestoreRequestMode:
    @mock.patch('boto3.client')
    @mock.patch('psycopg2.connect')
    def test_run_restore_request_mode_success(self, mock_db, mock_s3, valid_restore_csv, temp_test_environment):
        """復元リクエスト送信モード成功テスト"""
        # Given: 有効な復元依頼CSV
        # And: 正常なデータベース・S3モック
        # When: run()をrequest-onlyモードで実行
        # Then: 戻り値が0
        # And: 復元リクエストが送信される
        # And: ステータスファイルが保存される
        
    def test_run_restore_request_mode_no_valid_requests(self, invalid_restore_csv):
        """有効な復元依頼なしテスト"""
        # Given: 全て無効な復元依頼を含むCSV
        # When: run()をrequest-onlyモードで実行
        # Then: 戻り値が1
        # And: エラーCSVが生成される
```

#### 3.8.2 ダウンロード実行モードテスト
```python
class TestDownloadMode:
    @mock.patch('boto3.client')
    def test_run_download_mode_success(self, mock_s3, sample_status_file, temp_test_environment):
        """ダウンロード実行モード成功テスト"""
        # Given: 復元完了ファイルを含むステータスファイル
        # And: 正常なS3ダウンロードモック
        # When: run()をdownload-onlyモードで実行
        # Then: 戻り値が0
        # And: ファイルがダウンロード・配置される
        
    def test_run_download_mode_no_completed_files(self, pending_status_file):
        """復元未完了ファイルのみテスト"""
        # Given: 復元処理中ファイルのみのステータスファイル
        # When: run()をdownload-onlyモードで実行
        # Then: 戻り値が0
        # And: "復元処理中"メッセージが出力される
        
    def test_run_download_mode_no_status_file(self):
        """ステータスファイル不存在テスト"""
        # Given: 存在しないrequest_id
        # When: run()をdownload-onlyモードで実行
        # Then: 戻り値が1
        # And: "復元リクエスト送信を先に実行"メッセージが出力される
```

### 3.9 エラーハンドリングテスト

#### 3.9.1 エラーCSV生成テスト
```python
class TestRestoreErrorHandling:
    def test_generate_restore_error_csv(self, temp_csv_path, restore_validation_errors):
        """復元エラーCSV生成テスト"""
        # Given: 復元CSV検証エラーリスト
        # When: generate_restore_error_csv()を実行
        # Then: logs/配下にエラーCSVが生成される
        # And: 行番号、内容、エラー理由、元の行が含まれる
        
    def test_error_csv_encoding_utf8_sig(self, restore_validation_errors):
        """エラーCSVエンコーディングテスト"""
        # Given: 日本語エラー理由を含むエラーリスト
        # When: エラーCSVを生成
        # Then: UTF-8-SIGエンコーディングで保存される
        # And: Excelで正常に開ける
```

#### 3.9.2 ログ機能テスト
```python
class TestRestoreLogging:
    def test_log_file_creation_restore(self, temp_log_directory):
        """復元ログファイル作成テスト"""
        # Given: ログディレクトリ設定
        # When: RestoreProcessorを初期化
        # Then: logs/restore_YYYYMMDD_HHMMSS.logが作成される
        
    def test_progress_logging(self, sample_completed_requests):
        """進捗ログテスト"""
        # Given: 複数の復元完了ファイル
        # When: ダウンロード処理を実行
        # Then: [n/total] 形式の進捗ログが出力される
        
    def test_statistics_logging(self, mixed_restore_results):
        """統計ログテスト"""
        # Given: 成功・スキップ・失敗を含む処理結果
        # When: print_statistics()を実行
        # Then: 詳細な統計情報がログ出力される
```

### 3.10 統合テスト

#### 3.10.1 End-to-Endテスト
```python
class TestRestoreIntegration:
    @mock.patch('psycopg2.connect')
    @mock.patch('boto3.client')
    def test_full_restore_process_request_to_download(self, mock_s3, mock_db, temp_test_environment):
        """完全復元プロセステスト（リクエスト→ダウンロード）"""
        # Given: 有効な復元依頼CSV
        # And: アーカイブ履歴があるデータベースモック
        # And: 復元完了を返すS3モック
        # When: request-only → download-only を順次実行
        # Then: 両方とも成功（戻り値0）
        # And: ファイルが最終的に配置される
        
    @mock.patch('psycopg2.connect')
    @mock.patch('boto3.client')
    def test_partial_restore_completion_handling(self, mock_s3, mock_db, temp_test_environment):
        """部分復元完了処理テスト"""
        # Given: 複数ファイルの復元依頼
        # And: 一部ファイルのみ復元完了のS3モック
        # When: download-only を実行
        # Then: 完了ファイルのみダウンロードされる
        # And: 未完了ファイルは次回に持ち越し
        # And: 適切な案内メッセージが出力される
        
    def test_zero_byte_file_full_process(self, zero_byte_test_environment):
        """0バイトファイル完全処理テスト"""
        # Given: 0バイトファイルのアーカイブ履歴
        # And: 0バイトファイル復元完了状態
        # When: 完全な復元プロセスを実行
        # Then: 0バイトファイルが正常に配置される
        # And: エラーが発生しない
```

## 4. テストデータ仕様

### 4.1 復元依頼CSVテストデータ

#### 4.1.1 有効復元依頼CSV (`valid_restore_request.csv`)
```csv
元ファイルパス,復元先ディレクトリ
\\server\share\project1\file1.txt,{temp_restore_dir}\restored1
\\server\share\project2\file2.pdf,{temp_restore_dir}\restored2
C:\local\data\file3.xlsx,{temp_restore_dir}\restored3
```

#### 4.1.2 無効復元依頼CSV (`invalid_restore_request.csv`)
```csv
元ファイルパス,復元先ディレクトリ
\\nonexistent\path\file.txt,/nonexistent/restore/dir
{very_long_path_over_260_characters},C:\restore
single_column_only
```

#### 4.1.3 混合復元依頼CSV (`mixed_restore_request.csv`)
```csv
元ファイルパス,復元先ディレクトリ
\\server\share\valid\file1.txt,{temp_restore_dir}\restored
\\nonexistent\path\file.txt,/nonexistent/dir
\\server\share\valid\file2.txt,{temp_restore_dir}\restored
```

### 4.2 復元ステータスファイルテストデータ

#### 4.2.1 復元完了ステータス (`restore_status_sample.json`)
```json
{
  "request_id": "TEST-RESTORE-001",
  "request_date": "2025-07-16T10:00:00",
  "total_requests": 2,
  "restore_requests": [
    {
      "line_number": 2,
      "original_file_path": "\\\\server\\share\\file1.txt",
      "restore_directory": "C:\\restored\\",
      "s3_path": "s3://test-bucket/server/share/file1.txt",
      "bucket": "test-bucket",
      "key": "server/share/file1.txt",
      "restore_status": "completed",
      "restore_request_time": "2025-07-16T10:00:15",
      "restore_completed_time": "2025-07-18T14:20:00",
      "restore_expiry": "Fri, 25 Jul 2025 14:20:00 GMT"
    },
    {
      "line_number": 3,
      "original_file_path": "\\\\server\\share\\file2.txt",
      "restore_directory": "C:\\restored\\",
      "s3_path": "s3://test-bucket/server/share/file2.txt",
      "bucket": "test-bucket",
      "key": "server/share/file2.txt",
      "restore_status": "completed",
      "restore_request_time": "2025-07-16T10:00:16",
      "restore_completed_time": "2025-07-18T14:21:00",
      "restore_expiry": "Fri, 25 Jul 2025 14:21:00 GMT"
    }
  ]
}
```

#### 4.2.2 部分復元完了ステータス (`restore_status_partial.json`)
```json
{
  "request_id": "TEST-RESTORE-002",
  "request_date": "2025-07-16T11:00:00",
  "total_requests": 3,
  "restore_requests": [
    {
      "line_number": 2,
      "original_file_path": "\\\\server\\share\\file1.txt",
      "restore_directory": "C:\\restored\\",
      "s3_path": "s3://test-bucket/server/share/file1.txt",
      "bucket": "test-bucket",
      "key": "server/share/file1.txt",
      "restore_status": "completed",
      "restore_completed_time": "2025-07-18T14:20:00"
    },
    {
      "line_number": 3,
      "original_file_path": "\\\\server\\share\\file2.txt",
      "restore_directory": "C:\\restored\\",
      "s3_path": "s3://test-bucket/server/share/file2.txt",
      "bucket": "test-bucket",
      "key": "server/share/file2.txt",
      "restore_status": "in_progress",
      "restore_request_time": "2025-07-16T11:00:16"
    },
    {
      "line_number": 4,
      "original_file_path": "\\\\server\\share\\file3.txt",
      "restore_directory": "C:\\restored\\",
      "s3_path": "s3://test-bucket/server/share/file3.txt",
      "bucket": "test-bucket",
      "key": "server/share/file3.txt",
      "restore_status": "pending",
      "restore_request_time": "2025-07-16T11:00:17"
    }
  ]
}
```

### 4.3 設定ファイルテストデータ

#### 4.3.1 有効復元設定ファイル (`valid_restore_config.json`)
```json
{
    "aws": {
        "region": "ap-northeast-1",
        "s3_bucket": "test-restore-bucket",
        "vpc_endpoint_url": "https://bucket.vpce-test.s3.ap-northeast-1.vpce.amazonaws.com"
    },
    "database": {
        "host": "localhost",
        "port": 5432,
        "database": "test_archive_system",
        "user": "test_user",
        "password": "test_password",
        "timeout": 30
    },
    "restore": {
        "restore_tier": "Standard",
        "restore_days": 7,
        "check_interval": 300,
        "max_wait_time": 86400,
        "download_retry_count": 3,
        "skip_existing_files": true,
        "temp_download_directory": "test_temp_downloads"
    },
    "logging": {
        "log_directory": "test_logs",
        "log_level": "DEBUG"
    }
}
```

#### 4.3.2 復元設定不足ファイル (`incomplete_restore_config.json`)
```json
{
    "aws": {
        "region": "ap-northeast-1",
        "s3_bucket": "test-bucket"
    },
    "database": {
        "host": "localhost",
        "database": "test_db"
    }
    // restore セクションが不足
}
```

### 4.4 テストファイル構造
```
test_data/
├── restore_requests/
│   ├── files/
│   │   ├── test_file_1.txt (5KB)
│   │   ├── test_file_2.pdf (1MB)
│   │   ├── zero_byte_file.txt (0B)
│   │   └── large_test_file.bin (100MB)
│   └── temp_restore_dirs/
│       ├── restore_dir_1/
│       ├── restore_dir_2/
│       └── readonly_dir/ (読み取り専用)
├── s3_mock_data/
│   ├── restore_headers/
│   │   ├── completed_header.txt
│   │   ├── in_progress_header.txt
│   │   └── pending_header.txt
│   └── download_content/
│       ├── sample_content.txt
│       └── binary_content.bin
```

## 5. フィクスチャ仕様

### 5.1 共通フィクスチャ (`conftest.py`)

```python
import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock

@pytest.fixture
def temp_restore_directory():
    """一時復元ディレクトリ作成フィクスチャ"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

@pytest.fixture
def temp_restore_csv_file(temp_restore_directory):
    """一時復元依頼CSVファイル作成フィクスチャ"""
    csv_path = Path(temp_restore_directory) / "restore_request.csv"
    yield csv_path

@pytest.fixture
def valid_restore_config():
    """有効復元設定辞書フィクスチャ"""
    return {
        "aws": {"region": "ap-northeast-1", "s3_bucket": "test-bucket"},
        "database": {"host": "localhost", "port": 5432, "database": "test_db"},
        "restore": {
            "restore_tier": "Standard",
            "restore_days": 7,
            "download_retry_count": 3,
            "skip_existing_files": True,
            "temp_download_directory": "test_temp"
        },
        "logging": {"log_directory": "test_logs", "log_level": "INFO"}
    }

@pytest.fixture
def sample_restore_requests():
    """サンプル復元依頼リストフィクスチャ"""
    return [
        {
            'line_number': 2,
            'original_file_path': '\\\\server\\share\\file1.txt',
            'restore_directory': '/tmp/restore1',
            's3_path': None,
            'bucket': None,
            'key': None
        },
        {
            'line_number': 3,
            'original_file_path': '\\\\server\\share\\file2.txt',
            'restore_directory': '/tmp/restore2',
            's3_path': None,
            'bucket': None,
            'key': None
        }
    ]

@pytest.fixture
def sample_restore_requests_with_s3(sample_restore_requests):
    """S3パス情報付き復元依頼リストフィクスチャ"""
    for request in sample_restore_requests:
        request['s3_path'] = f"s3://test-bucket/server/share/{Path(request['original_file_path']).name}"
        request['bucket'] = 'test-bucket'
        request['key'] = f"server/share/{Path(request['original_file_path']).name}"
    return sample_restore_requests

@pytest.fixture
def sample_completed_requests(sample_restore_requests_with_s3):
    """復元完了リクエストフィクスチャ"""
    for request in sample_restore_requests_with_s3:
        request['restore_status'] = 'completed'
        request['restore_completed_time'] = '2025-07-18T14:20:00'
    return sample_restore_requests_with_s3

@pytest.fixture
def sample_zero_byte_request():
    """0バイトファイル復元依頼フィクスチャ"""
    return {
        'line_number': 2,
        'original_file_path': '\\\\server\\share\\zero_file.txt',
        'restore_directory': '/tmp/restore',
        's3_path': 's3://test-bucket/server/share/zero_file.txt',
        'bucket': 'test-bucket',
        'key': 'server/share/zero_file.txt',
        'restore_status': 'completed'
    }

@pytest.fixture
def mock_restore_status_file(temp_restore_directory, sample_completed_requests):
    """復元ステータスファイルモックフィクスチャ"""
    status_data = {
        "request_id": "TEST-001",
        "request_date": "2025-07-16T10:00:00",
        "total_requests": len(sample_completed_requests),
        "restore_requests": sample_completed_requests
    }
    
    status_file = Path(temp_restore_directory) / "restore_status_TEST-001.json"
    with open(status_file, 'w', encoding='utf-8') as f:
        json.dump(status_data, f, ensure_ascii=False, indent=2, default=str)
    
    yield status_file
```

### 5.2 復元処理専用フィクスチャ

```python
@pytest.fixture
def mock_db_with_archive_history():
    """アーカイブ履歴ありデータベースモック"""
    with mock.patch('psycopg2.connect') as mock_connect:
        mock_conn = mock_connect.return_value
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        
        # アーカイブ履歴検索結果
        mock_cursor.fetchone.return_value = (
            's3://test-bucket/server/share/file1.txt',
            '2025-07-15T10:00:00'
        )
        yield mock_conn

@pytest.fixture
def mock_s3_restore_success():
    """S3復元リクエスト成功モック"""
    with mock.patch('boto3.client') as mock_client:
        mock_s3 = mock_client.return_value
        mock_s3.restore_object.return_value = None
        yield mock_s3

@pytest.fixture
def mock_s3_restore_completed():
    """S3復元完了確認モック"""
    with mock.patch('boto3.client') as mock_client:
        mock_s3 = mock_client.return_value
        mock_s3.head_object.return_value = {
            'Restore': 'ongoing-request="false", expiry-date="Fri, 25 Jul 2025 14:20:00 GMT"'
        }
        yield mock_s3

@pytest.fixture
def mock_s3_download_success():
    """S3ダウンロード成功モック"""
    with mock.patch('boto3.client') as mock_client:
        mock_s3 = mock_client.return_value
        
        def mock_download_file(bucket, key, local_path):
            # 実際にファイルを作成してダウンロードをシミュレート
            Path(local_path).write_text("test content")
        
        mock_s3.download_file.side_effect = mock_download_file
        yield mock_s3
```

## 6. モック仕様

### 6.1 S3クライアントモック

```python
@pytest.fixture
def mock_s3_restore_already_in_progress():
    """復元既に進行中モック"""
    with mock.patch('boto3.client') as mock_client:
        mock_s3 = mock_client.return_value
        
        from botocore.exceptions import ClientError
        error_response = {
            'Error': {
                'Code': 'RestoreAlreadyInProgress',
                'Message': 'Object restore is already in progress'
            }
        }
        mock_s3.restore_object.side_effect = ClientError(error_response, 'RestoreObject')
        yield mock_s3

@pytest.fixture
def mock_s3_download_with_retry():
    """S3ダウンロードリトライモック"""
    with mock.patch('boto3.client') as mock_client:
        mock_s3 = mock_client.return_value
        
        call_count = 0
        def mock_download_with_failure(bucket, key, local_path):
            nonlocal call_count
            call_count += 1
            if call_count < 3:  # 最初の2回は失敗
                raise Exception("Temporary network error")
            else:  # 3回目で成功
                Path(local_path).write_text("test content")
        
        mock_s3.download_file.side_effect = mock_download_with_failure
        yield mock_s3

@pytest.fixture
def mock_s3_no_such_key():
    """キー見つからずモック"""
    with mock.patch('boto3.client') as mock_client:
        mock_s3 = mock_client.return_value
        
        from botocore.exceptions import ClientError
        error_response = {
            'Error': {
                'Code': 'NoSuchKey',
                'Message': 'The specified key does not exist'
            }
        }
        mock_s3.head_object.side_effect = ClientError(error_response, 'HeadObject')
        yield mock_s3
```

### 6.2 データベースモック

```python
@pytest.fixture
def mock_db_no_archive_history():
    """アーカイブ履歴なしデータベースモック"""
    with mock.patch('psycopg2.connect') as mock_connect:
        mock_conn = mock_connect.return_value
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchone.return_value = None  # 履歴なし
        yield mock_conn

@pytest.fixture
def mock_db_connection_error():
    """データベース接続エラーモック"""
    with mock.patch('psycopg2.connect') as mock_connect:
        mock_connect.side_effect = Exception("Database connection failed")
        yield mock_connect
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
- wait_for_restore_completion メソッド（TODO実装）
```

## 8. テスト実行手順

### 8.1 基本テスト実行
```bash
# 全テスト実行
pytest tests/ -v

# カバレッジ付き実行
pytest tests/ -v --cov=restore_script_main --cov-report=html

# 特定テストクラス実行
pytest tests/test_restore_csv_validation.py::TestRestoreCSVValidation -v

# 特定テストメソッド実行
pytest tests/test_download_and_place.py::TestDownloadAndPlace::test_download_zero_byte_file -v
```

### 8.2 復元処理専用テスト
```bash
# 復元リクエスト関連テストのみ
pytest tests/test_s3_restore_request.py tests/test_restore_status_check.py -v

# ダウンロード・配置関連テストのみ
pytest tests/test_download_and_place.py -v

# 統合テストのみ
pytest tests/test_restore_processor.py::TestRestoreIntegration -v
```

### 8.3 実機検証テスト
```bash
# 0バイトファイル処理テスト
pytest tests/ -k "zero_byte" -v

# エラーハンドリングテスト
pytest tests/test_restore_error_handling.py -v

# ステータス管理テスト
pytest tests/test_status_management.py -v
```

## 9. テスト環境構築

### 9.1 依存関係インストール
```bash
pip install pytest pytest-cov pytest-mock pytest-xdist pytest-watch
pip install boto3 psycopg2-binary  # 復元スクリプト依存関係
```

### 9.2 テストデータベース準備
```sql
-- テスト用データベース作成
CREATE DATABASE test_archive_system;

-- テスト用アーカイブ履歴データ挿入
\c test_archive_system
INSERT INTO archive_history (request_id, requester, request_date, original_file_path, s3_path, archive_date, file_size)
VALUES 
('TEST-ARCHIVE-001', '12345678', '2025-07-15 10:00:00', '\\server\share\file1.txt', 's3://test-bucket/server/share/file1.txt', '2025-07-15 10:00:00', 1024),
('TEST-ARCHIVE-001', '12345678', '2025-07-15 10:00:00', '\\server\share\file2.txt', 's3://test-bucket/server/share/file2.txt', '2025-07-15 10:00:00', 0);
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
    restore: Restore process tests
    s3: S3 related tests
    database: Database related tests
```

## 10. 品質基準

### 10.1 復元処理特有の品質チェック
- **復元ステータス管理**: 各ステータス遷移の正確性
- **部分復元対応**: 一部ファイル完了時の適切な処理
- **0バイトファイル対応**: 空ファイルの正常処理
- **同名ファイルスキップ**: 既存ファイル保護の確認
- **一時ファイル管理**: 自動クリーンアップの動作確認

### 10.2 復元処理テスト観点
- **データ整合性**: ダウンロード→配置→ステータス更新の順序
- **エラー回復性**: 部分失敗時の適切なハンドリング
- **リソース管理**: 一時ファイル・メモリの適切な解放
- **ユーザビリティ**: 適切な進捗表示・エラーメッセージ

### 10.3 実機検証項目
- **VPCエンドポイント通信**: AWS接続の正常性
- **Glacier復元**: 実際の復元プロセスの動作確認
- **ファイルシステム操作**: Windows環境での権限・パス処理
- **文字エンコーディング**: 日本語ファイル名の正常処理

## 11. 継続的テスト改善

### 11.1 テストメトリクス監視
- **実行時間**: 遅いテストの特定・最適化
- **失敗率**: 不安定なテストの改善
- **カバレッジ**: 未テスト部分の特定

### 11.2 実機テスト連携
- **定期実機検証**: 週次での実機環境テスト
- **0バイトファイルテスト**: 実際の0バイトファイルでの検証
- **大容量ファイルテスト**: メモリ・ディスク使用量確認

### 11.3 テストデータ管理
- **多様なファイル形式**: 各種拡張子・サイズのテストファイル
- **エラーパターン**: 実際に発生したエラーの再現テスト
- **エッジケース**: 境界値・特殊条件のテストケース追加

---

この復元スクリプト単体テスト仕様書に基づいて、堅牢で信頼性の高い復元機能のテストコードを実装し、企業環境での安定稼働を保証することができます。# 復元スクリプト単体テスト仕様書

## 1. テスト概要

### 1.1 目的
`restore_script_main.py`の各機能の動作を検証し、品質を保証する。

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
pytest tests/ -v --cov=restore_script_main --cov-report=html
```

## 2. テストファイル構成

```
tests/
├── __init__.py
├── test_restore_processor.py           # メインクラステスト
├── test_restore_csv_validation.py      # 復元CSV検証テスト
├── test_database_lookup.py             # データベース検索テスト
├── test_s3_restore_request.py          # S3復元リクエストテスト
├── test_restore_status_check.py        # 復元ステータス確認テスト
├── test_download_and_place.py          # ダウンロード・配置テスト
├── test_status_management.py           # ステータス管理テスト
├── test_restore_error_handling.py      # エラーハンドリングテスト
├── test_restore_config_management.py   # 設定管理テスト
├── conftest.py                         # テ