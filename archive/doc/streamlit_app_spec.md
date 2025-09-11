# Streamlitアプリケーション仕様書（streamlit_app.py）

## 1. 概要

### 1.1 目的
アーカイブされたファイルの履歴を閲覧・検索し、統計情報の表示とデータエクスポート機能を提供するWebアプリケーション。

### 1.2 実装状況
✅ **実装完了・実機検証済み**
- 履歴閲覧・検索機能
- 統計情報表示機能
- Excel・CSVエクスポート機能
- セッション状態管理
- SQLAlchemy 2.0対応
- ブラウザ互換性対応

### 1.3 技術仕様
- **フレームワーク**: Streamlit 1.46以上
- **言語**: Python 3.13
- **データベース**: PostgreSQL 13以上（読み取り専用）
- **依存ライブラリ**: pandas, sqlalchemy, psycopg2-binary, openpyxl
- **ブラウザ要件**: Microsoft Edge 93以降

## 2. アプリケーション構成

### 2.1 画面レイアウト
```
┌─────────────────────────────────────────────────────┐
│  📁 アーカイブ履歴管理システム                      │
│  最終更新: 2025年07月17日 13:00:32                  │
│  [🔄 初期画面に戻る] ←検索実行後のみ表示           │
├─────────────────────────────────────────────────────┤
│ サイドバー                    │ メインエリア        │
│ ┌─────────────────────────────┐ │ ┌─────────────────│
│ │ 🔍 検索条件                │ │ │ 📊 統計情報     │
│ │ ┌─────────────────────────┐ │ │ │ ┌─────────────│
│ │ │ 期間指定                │ │ │ │ │ 総ファイル数│
│ │ │ 開始日 [2025-06-17]    │ │ │ │ │ 総サイズ    │
│ │ │ 終了日 [2025-07-17]    │ │ │ │ │ 依頼件数    │
│ │ └─────────────────────────┘ │ │ │ └─────────────│
│ │ 依頼ID                     │ │ │ 📋 履歴一覧     │
│ │ 依頼者                     │ │ │ [データテーブル]│
│ │ ファイル検索               │ │ │ 📥 エクスポート │
│ │ 表示件数                   │ │ │ [Excel] [CSV]  │
│ │ [🔍 検索実行]             │ │ │                 │
│ └─────────────────────────────┘ │ └─────────────────│
└─────────────────────────────────────────────────────┘
```

### 2.2 ArchiveHistoryApp クラス構成
```python
class ArchiveHistoryApp:
    """アーカイブ履歴管理アプリケーション"""
    
    def __init__(self)
    def load_config(self) -> Dict
    def get_database_engine(self)
    def search_archive_history(...) -> pd.DataFrame
    def get_statistics(...) -> Dict
    def get_requester_list(self) -> List[str]
    def format_file_size(self, size_bytes: int) -> str
    def create_download_link(self, df: pd.DataFrame, filename: str, file_format: str) -> str
    def render_header(self)
    def render_sidebar_filters(self)
    def render_statistics(self, stats: Dict)
    def render_data_table(self, df: pd.DataFrame)
    def render_export_section(self, df: pd.DataFrame)
    def render_initial_screen(self)
    def run(self)
```

## 3. 機能仕様

### 3.1 検索・フィルタリング機能

#### 3.1.1 検索条件
| 項目 | 必須 | 説明 | 例 |
|------|------|------|-----|
| 期間指定 | ✓ | アーカイブ日時の範囲指定 | 2025-06-17 ～ 2025-07-17 |
| 依頼ID | - | 部分一致検索 | REQ-2025-001 |
| 依頼者 | - | 社員番号での絞り込み | 12345678 |
| ファイル検索 | - | ファイルパスの部分一致 | project1, .txt, \\server\share |
| 表示件数 | ✓ | 1回の検索結果表示件数 | 100/500/1000/2000 |

#### 3.1.2 検索クエリ（SQLAlchemy 2.0対応）
```python
def search_archive_history(self, start_date, end_date, request_id="", 
                          requester="", file_path="", limit=1000, offset=0):
    """アーカイブ履歴検索"""
    query = """
        SELECT 
            id, request_id, requester, request_date,
            original_file_path, s3_path, archive_date, file_size, created_at
        FROM archive_history 
        WHERE request_date::date BETWEEN %(start_date)s AND %(end_date)s
    """
    
    params = {'start_date': start_date, 'end_date': end_date}
    
    # 動的フィルター追加
    if request_id.strip():
        query += " AND request_id ILIKE %(request_id)s"
        params['request_id'] = f"%{request_id.strip()}%"
    
    if requester.strip():
        query += " AND requester LIKE %(requester)s"
        params['requester'] = f"%{requester.strip()}%"
    
    if file_path.strip():
        query += " AND original_file_path ILIKE %(file_path)s"
        params['file_path'] = f"%{file_path.strip()}%"
    
    query += " ORDER BY request_date DESC LIMIT %(limit)s OFFSET %(offset)s"
    params.update({'limit': limit, 'offset': offset})
    
    # SQLAlchemy 2.0 対応実行
    df = pd.read_sql_query(query, engine, params=params)
    return df
```

### 3.2 統計情報表示機能

#### 3.2.1 基本統計
```python
def get_statistics(self, start_date, end_date, ...):
    """統計情報取得"""
    query = """
        SELECT 
            COUNT(*) as total_files,
            SUM(file_size) as total_size,
            COUNT(DISTINCT request_id) as total_requests,
            AVG(file_size) as avg_file_size,
            MAX(file_size) as max_file_size,
            MIN(request_date) as first_archive,
            MAX(request_date) as last_archive
        FROM archive_history 
        WHERE request_date::date BETWEEN %(start_date)s AND %(end_date)s
    """
```

#### 3.2.2 表示項目
- **総ファイル数**: 検索条件に一致するアーカイブ済みファイル数
- **総サイズ**: アーカイブ済みファイルの合計サイズ（自動単位変換）
- **依頼件数**: アーカイブ依頼の総数
- **平均サイズ**: ファイルの平均サイズ
- **最大サイズ**: 最大ファイルサイズ
- **期間情報**: 最初・最新のアーカイブ日時

### 3.3 データ表示機能

#### 3.3.1 メインテーブル表示列
| 表示列 | 説明 | フォーマット |
|--------|------|-------------|
| 依頼ID | アーカイブ依頼ID | 文字列 |
| 依頼者 | 社員番号 | 8桁数字 |
| 依頼日時 | アーカイブ依頼日時 | YYYY-MM-DD HH:MM |
| ファイルパス | 元ファイルパス（短縮表示） | 50文字+... |
| サイズ(MB) | ファイルサイズ | 小数点2桁 |
| アーカイブ日時 | S3アップロード完了日時 | YYYY-MM-DD HH:MM |

#### 3.3.2 詳細表示機能
```python
def render_data_table(self, df: pd.DataFrame):
    """データテーブル描画"""
    # 表示用DataFrame準備
    display_df = df.copy()
    
    # 表示列の選択・リネーム
    display_columns = {
        'request_id': '依頼ID',
        'requester': '依頼者', 
        'request_date': '依頼日時',
        'file_path_short': 'ファイルパス',
        'file_size_mb': 'サイズ(MB)',
        'archive_date': 'アーカイブ日時'
    }
    
    # 日時列のフォーマット
    display_df['依頼日時'] = display_df['依頼日時'].dt.strftime('%Y-%m-%d %H:%M')
    display_df['アーカイブ日時'] = display_df['アーカイブ日時'].dt.strftime('%Y-%m-%d %H:%M')
    
    # テーブル表示
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # 詳細表示セクション
    with st.expander("🔍 詳細情報表示"):
        selected_indices = st.multiselect(
            "詳細を表示するレコードを選択（依頼IDで選択）",
            options=df['request_id'].unique(),
            max_selections=5
        )
        
        if selected_indices:
            detail_df = df[df['request_id'].isin(selected_indices)]
            # 詳細情報の表示処理
```

### 3.4 エクスポート機能

#### 3.4.1 対応フォーマット
- **Excel形式**: .xlsx（openpyxl使用）
- **CSV形式**: .csv（UTF-8-SIG エンコーディング）

#### 3.4.2 エクスポート処理
```python
def create_download_link(self, df: pd.DataFrame, filename: str, file_format: str = "excel"):
    """ダウンロードリンク作成"""
    try:
        if file_format == "excel":
            # Excel出力
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                export_df = df.copy()
                
                # 列名を日本語に変更
                column_mapping = {
                    'id': 'ID',
                    'request_id': '依頼ID',
                    'requester': '依頼者',
                    'request_date': '依頼日時',
                    'original_file_path': '元ファイルパス',
                    's3_path': 'S3パス',
                    'archive_date': 'アーカイブ日時',
                    'file_size': 'ファイルサイズ(Bytes)',
                    'file_size_mb': 'ファイルサイズ(MB)',
                    'created_at': '作成日時'
                }
                
                export_df = export_df.drop(['file_path_short'], axis=1, errors='ignore')
                export_df = export_df.rename(columns=column_mapping)
                export_df.to_excel(writer, sheet_name='アーカイブ履歴', index=False)
            
            output.seek(0)
            b64 = base64.b64encode(output.read()).decode()
            
        elif file_format == "csv":
            # CSV出力
            export_df = df.copy()
            export_df = export_df.drop(['file_path_short'], axis=1, errors='ignore')
            
            csv_buffer = io.StringIO()
            export_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_string = csv_buffer.getvalue()
            b64 = base64.b64encode(csv_string.encode('utf-8-sig')).decode()
        
        # ダウンロードリンク生成
        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if file_format == "excel" else "text/csv"
        href = f'<a href="data:{mime_type};base64,{b64}" download="{filename}">📥 {filename} をダウンロード</a>'
        
        return href
        
    except Exception as e:
        st.error(f"ダウンロードリンク作成エラー: {str(e)}")
        return ""
```

#### 3.4.3 ファイル名規則
```python
# ファイル名生成
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
excel_filename = f"archive_history_{timestamp}.xlsx"
csv_filename = f"archive_history_{timestamp}.csv"
```

## 4. セッション状態管理

### 4.1 セッション変数
```python
# セッション状態の初期化
if 'search_executed' not in st.session_state:
    st.session_state.search_executed = False
if 'last_search_params' not in st.session_state:
    st.session_state.last_search_params = {}
if 'search_results' not in st.session_state:
    st.session_state.search_results = pd.DataFrame()
if 'search_stats' not in st.session_state:
    st.session_state.search_stats = {}
```

### 4.2 状態遷移
```
初期画面 → 検索実行 → 結果表示 → 初期画面リセット
     ↑                            ↓
     └──── リセットボタン ←────────┘
```

### 4.3 セッション管理機能
```python
def reset_session_state():
    """セッション状態のリセット"""
    st.session_state.search_executed = False
    st.session_state.search_results = pd.DataFrame()
    st.session_state.search_stats = {}
    st.session_state.last_search_params = {}
    st.rerun()

# リセットボタン（検索実行後のみ表示）
if st.session_state.search_executed:
    if st.button("🔄 初期画面に戻る", key="reset_button"):
        reset_session_state()
```

## 5. データベース連携仕様

### 5.1 SQLAlchemy 2.0 対応
```python
def get_database_engine(self):
    """SQLAlchemy エンジンを取得（pandas警告対策）"""
    if self.engine is None:
        try:
            db_config = self.config.get('database', {})
            
            # PostgreSQL接続文字列の構築
            connection_string = (
                f"postgresql://{db_config.get('user', 'postgres')}:"
                f"{db_config.get('password', '')}@"
                f"{db_config.get('host', 'localhost')}:"
                f"{db_config.get('port', 5432)}/"
                f"{db_config.get('database', 'archive_system')}"
            )
            
            self.engine = create_engine(connection_string)
            
            # 接続テスト
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            return self.engine
            
        except Exception as e:
            st.error(f"データベース接続エラー: {str(e)}")
            st.stop()
    return self.engine
```

### 5.2 クエリ最適化
```sql
-- 基本検索（インデックス活用）
SELECT id, request_id, requester, request_date,
       original_file_path, s3_path, archive_date, file_size
FROM archive_history
WHERE request_date::date BETWEEN %s AND %s
ORDER BY request_date DESC
LIMIT %s OFFSET %s;

-- 統計情報取得
SELECT COUNT(*) as total_files,
       SUM(file_size) as total_size,
       COUNT(DISTINCT request_id) as total_requests
FROM archive_history
WHERE request_date::date BETWEEN %s AND %s;
```

### 5.3 エラーハンドリング
```python
try:
    # SQLAlchemy 2.0 対応
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        return result.fetchall()
        
except Exception as e:
    st.error(f"データベースエラー: {str(e)}")
    return []
```

## 6. UI/UXデザイン仕様

### 6.1 カスタムCSS
```css
.main-header {
    font-size: 2.5rem;
    color: #1f77b4;
    text-align: center;
    margin-bottom: 2rem;
}
.metric-container {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 0.5rem;
    margin: 0.5rem 0;
}
.success-metric {
    background-color: #d4edda;
    border-left: 4px solid #28a745;
}
.info-metric {
    background-color: #d1ecf1;
    border-left: 4px solid #17a2b8;
}
```

### 6.2 レスポンシブデザイン
```python
# 3列レイアウト（統計情報）
col1, col2, col3 = st.columns(3)

# 2列レイアウト（エクスポート）
col1, col2 = st.columns(2)

# コンテナ幅調整
st.dataframe(df, use_container_width=True)
```

### 6.3 アクセシビリティ対応
- **alt属性**: 適切なヘルプテキスト
- **キーボード操作**: Streamlit標準対応
- **色覚障害対応**: 十分なコントラスト比

## 7. パフォーマンス仕様

### 7.1 処理性能
- **検索応答時間**: 1,000件以下で1秒以内
- **統計情報取得**: 0.5秒以内
- **エクスポート処理**: 10,000件で10秒以内

### 7.2 メモリ使用量
- **DataFrame**: 検索結果分のメモリ使用
- **セッション状態**: 最小限のデータ保持
- **エクスポート**: 一時的なメモリ使用

### 7.3 最適化実装
```python
# データフォーマット最適化
def format_file_size(self, size_bytes: int) -> str:
    """効率的なファイルサイズフォーマット"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size = float(size_bytes)
    
    while size >= 1024.0 and i < len(size_names) - 1:
        size /= 1024.0
        i += 1
    
    return f"{size:.1f} {size_names[i]}"

# 依頼者リスト取得の最適化
@st.cache_data(ttl=3600)  # 1時間キャッシュ
def get_requester_list(self) -> List[str]:
    """依頼者リスト取得（キャッシュ付き）"""
    # 実装省略
```

## 8. セキュリティ仕様

### 8.1 アクセス制御
- **認証**: 実装なし（内部システム前提）
- **権限**: データベース読み取り専用権限
- **ネットワーク**: 内部ネットワークからのアクセスのみ

### 8.2 データ保護
```python
def mask_sensitive_data(log_message: str) -> str:
    """機密情報のマスキング"""
    # パスワード、認証情報のマスキング実装
    masked = re.sub(r'password["\s]*[:=]["\s]*[^"]*', 'password=***', log_message)
    return masked
```

### 8.3 入力検証
- **SQL インジェクション**: SQLAlchemy パラメータ化クエリで防止
- **XSS**: Streamlit標準の自動エスケープ
- **ファイルパス**: 表示時の適切なエスケープ

## 9. 運用仕様

### 9.1 起動方法

#### 9.1.1 手動起動
```bash
# 基本起動
streamlit run streamlit_app.py --server.port 8501

# 設定指定起動
streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0
```

#### 9.1.2 Windows サービス起動
```powershell
# サービス開始
Start-ScheduledTask -TaskName "ArchiveHistoryStreamlitService"

# 状態確認
Get-ScheduledTask -TaskName "ArchiveHistoryStreamlitService"

# ログ確認
Get-Content "logs\service\streamlit_service_$(Get-Date -Format 'yyyyMMdd').log" -Tail 20
```

### 9.2 ヘルスチェック機能
```python
def check_database_connection():
    """データベース接続確認"""
    try:
        engine = self.get_database_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM archive_history"))
            count = result.fetchone()[0]
            st.success(f"✅ データベース接続: 正常 (総レコード数: {count:,}件)")
            return True
    except Exception as e:
        st.error(f"❌ データベース接続: エラー - {str(e)}")
        return False
```

### 9.3 ログ管理
```python
# Streamlit ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/streamlit_app.log'),
        logging.StreamHandler()
    ]
)
```

## 10. ブラウザ互換性

### 10.1 対応ブラウザ
- **Microsoft Edge**: 93以降 ✅
- **Google Chrome**: 91以降 ✅
- **Mozilla Firefox**: 89以降 ✅
- **Safari**: 14以降 ✅

### 10.2 互換性対応
```javascript
// Object.hasOwn() の代替実装（Edge 86対応）
if (!Object.hasOwn) {
    Object.hasOwn = function(obj, prop) {
        return Object.prototype.hasOwnProperty.call(obj, prop);
    };
}
```

### 10.3 機能制限
- **IE**: 未対応
- **古いブラウザ**: 機能制限あり

## 11. エラーハンドリング

### 11.1 エラー分類
| エラー種別 | 対応方法 | 表示方法 |
|-----------|---------|---------|
| データベース接続エラー | 設定確認案内 | st.error() |
| 検索タイムアウト | 条件変更案内 | st.warning() |
| エクスポートエラー | 詳細ログ出力 | st.error() |
| セッションエラー | 自動リセット | st.info() |

### 11.2 エラー画面
```python
def render_error_screen(self, error_message: str):
    """エラー画面表示"""
    st.error(f"アプリケーションエラー: {error_message}")
    st.exception(e)
    
    # エラー時のリセットオプション
    if st.button("🔄 アプリケーションをリセット", key="error_reset"):
        # セッション状態をリセット
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
```

## 12. 制約・注意事項

### 12.1 技術的制約
- **同時接続**: Streamlit標準制限
- **メモリ使用量**: 大量データ時の制限
- **レスポンス時間**: データベース性能に依存

### 12.2 運用制約
- **認証機能**: 実装なし
- **権限管理**: データベースレベルのみ
- **監査ログ**: 基本ログのみ

### 12.3 セキュリティ注意事項
- **内部ネットワーク**: 外部公開不可
- **データ保護**: 読み取り専用アクセス
- **ログ管理**: 定期的なローテーション推奨

## 13. 今後の拡張予定

### 13.1 短期拡張（3ヶ月以内）
- [ ] ダッシュボード機能
- [ ] 詳細フィルタリング
- [ ] レポート機能

### 13.2 中期拡張（6ヶ月以内）
- [ ] ユーザー認証機能
- [ ] 権限管理機能
- [ ] API連携機能

### 13.3 長期拡張（1年以内）
- [ ] リアルタイム更新
- [ ] 高度な分析機能
- [ ] モバイル対応

---

**最終更新**: 2025年7月
**バージョン**: v1.0（実装完了・実機検証済み）
**実装状況**: ✅ 本番運用可能