#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
アーカイブ履歴閲覧 Streamlit アプリケーション
"""

import streamlit as st
import pandas as pd
import psycopg2
import json
import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import io
import base64

# ページ設定
st.set_page_config(
    page_title="アーカイブ履歴管理システム",
    page_icon="📁",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS
st.markdown("""
<style>
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
    .error-metric {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
    }
    .info-metric {
        background-color: #d1ecf1;
        border-left: 4px solid #17a2b8;
    }
    .stDataFrame {
        border: 1px solid #e0e0e0;
        border-radius: 0.25rem;
    }
</style>
""", unsafe_allow_html=True)

class ArchiveHistoryApp:
    """アーカイブ履歴管理アプリケーション"""
    
    def __init__(self):
        self.config = self.load_config()
        self.db_connection = None
        
    def load_config(self) -> Dict:
        """設定ファイル読み込み"""
        config_path = "config/archive_config.json"
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            st.error(f"設定ファイルが見つかりません: {config_path}")
            st.stop()
        except json.JSONDecodeError as e:
            st.error(f"設定ファイルの形式が正しくありません: {e}")
            st.stop()
    
    def get_database_connection(self):
        """データベース接続を取得"""
        if self.db_connection is None:
            try:
                db_config = self.config.get('database', {})
                self.db_connection = psycopg2.connect(
                    host=db_config.get('host', 'localhost'),
                    port=db_config.get('port', 5432),
                    database=db_config.get('database', 'archive_system'),
                    user=db_config.get('user', 'postgres'),
                    password=db_config.get('password', ''),
                    connect_timeout=db_config.get('timeout', 30)
                )
                return self.db_connection
            except Exception as e:
                st.error(f"データベース接続エラー: {str(e)}")
                st.stop()
        return self.db_connection
    
    def search_archive_history(self, 
                             start_date: datetime.date,
                             end_date: datetime.date,
                             requester: str = "",
                             file_path: str = "",
                             limit: int = 1000,
                             offset: int = 0) -> pd.DataFrame:
        """アーカイブ履歴検索"""
        try:
            conn = self.get_database_connection()
            
            # 基本クエリ
            query = """
                SELECT 
                    id,
                    request_id,
                    requester,
                    request_date,
                    original_file_path,
                    s3_path,
                    archive_date,
                    file_size,
                    created_at
                FROM archive_history 
                WHERE request_date::date BETWEEN %s AND %s
            """
            params = [start_date, end_date]
            
            # 依頼者フィルター
            if requester.strip():
                query += " AND requester LIKE %s"
                params.append(f"%{requester.strip()}%")
            
            # ファイルパスフィルター
            if file_path.strip():
                query += " AND original_file_path ILIKE %s"
                params.append(f"%{file_path.strip()}%")
            
            # ソート・制限
            query += " ORDER BY request_date DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            # 実行
            df = pd.read_sql_query(query, conn, params=params)
            
            if not df.empty:
                # 日時列の変換
                df['request_date'] = pd.to_datetime(df['request_date'])
                df['archive_date'] = pd.to_datetime(df['archive_date'])
                df['created_at'] = pd.to_datetime(df['created_at'])
                
                # ファイルサイズの表示形式変換
                df['file_size_mb'] = (df['file_size'] / 1024 / 1024).round(2)
                
                # ファイルパスの短縮表示用
                df['file_path_short'] = df['original_file_path'].apply(
                    lambda x: x if len(x) <= 50 else f"{x[:47]}..."
                )
            
            return df
            
        except Exception as e:
            st.error(f"検索エラー: {str(e)}")
            return pd.DataFrame()
    
    def get_statistics(self, 
                      start_date: datetime.date,
                      end_date: datetime.date,
                      requester: str = "",
                      file_path: str = "") -> Dict:
        """統計情報取得"""
        try:
            conn = self.get_database_connection()
            
            # 基本クエリ
            query = """
                SELECT 
                    COUNT(*) as total_files,
                    SUM(file_size) as total_size,
                    COUNT(DISTINCT request_id) as total_requests,
                    COUNT(DISTINCT requester) as total_requesters,
                    AVG(file_size) as avg_file_size,
                    MAX(file_size) as max_file_size,
                    MIN(request_date) as first_archive,
                    MAX(request_date) as last_archive
                FROM archive_history 
                WHERE request_date::date BETWEEN %s AND %s
            """
            params = [start_date, end_date]
            
            # フィルター条件追加
            if requester.strip():
                query += " AND requester LIKE %s"
                params.append(f"%{requester.strip()}%")
            
            if file_path.strip():
                query += " AND original_file_path ILIKE %s"
                params.append(f"%{file_path.strip()}%")
            
            # 実行
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                
                if result:
                    return {
                        'total_files': result[0] or 0,
                        'total_size': result[1] or 0,
                        'total_requests': result[2] or 0,
                        'total_requesters': result[3] or 0,
                        'avg_file_size': result[4] or 0,
                        'max_file_size': result[5] or 0,
                        'first_archive': result[6],
                        'last_archive': result[7]
                    }
                else:
                    return {
                        'total_files': 0,
                        'total_size': 0,
                        'total_requests': 0,
                        'total_requesters': 0,
                        'avg_file_size': 0,
                        'max_file_size': 0,
                        'first_archive': None,
                        'last_archive': None
                    }
                    
        except Exception as e:
            st.error(f"統計情報取得エラー: {str(e)}")
            return {}
    
    def get_requester_list(self) -> List[str]:
        """依頼者リスト取得"""
        try:
            conn = self.get_database_connection()
            query = "SELECT DISTINCT requester FROM archive_history ORDER BY requester"
            
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                return [row[0] for row in results]
                
        except Exception as e:
            st.error(f"依頼者リスト取得エラー: {str(e)}")
            return []
    
    def format_file_size(self, size_bytes: int) -> str:
        """ファイルサイズフォーマット"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        size = float(size_bytes)
        
        while size >= 1024.0 and i < len(size_names) - 1:
            size /= 1024.0
            i += 1
        
        return f"{size:.1f} {size_names[i]}"
    
    def create_download_link(self, df: pd.DataFrame, filename: str, file_format: str = "excel") -> str:
        """ダウンロードリンク作成"""
        try:
            if file_format == "excel":
                # Excel形式
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    # 表示用のDataFrameを作成
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
                    
                    # 不要な列を削除
                    export_df = export_df.drop(['file_path_short'], axis=1, errors='ignore')
                    
                    # 列名変更
                    export_df = export_df.rename(columns=column_mapping)
                    
                    # Excelに書き込み
                    export_df.to_excel(writer, sheet_name='アーカイブ履歴', index=False)
                
                output.seek(0)
                b64 = base64.b64encode(output.read()).decode()
                
            elif file_format == "csv":
                # CSV形式
                export_df = df.copy()
                export_df = export_df.drop(['file_path_short'], axis=1, errors='ignore')
                
                csv_buffer = io.StringIO()
                export_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                csv_string = csv_buffer.getvalue()
                b64 = base64.b64encode(csv_string.encode('utf-8-sig')).decode()
            
            # ダウンロードリンク作成
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if file_format == "excel" else "text/csv"
            
            href = f'<a href="data:{mime_type};base64,{b64}" download="{filename}">📥 {filename} をダウンロード</a>'
            return href
            
        except Exception as e:
            st.error(f"ダウンロードリンク作成エラー: {str(e)}")
            return ""
    
    def render_header(self):
        """ヘッダー描画"""
        st.markdown('<h1 class="main-header">📁 アーカイブ履歴管理システム</h1>', unsafe_allow_html=True)
        
        # 現在時刻表示
        current_time = datetime.datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")
        st.markdown(f"<div style='text-align: center; color: #666; margin-bottom: 2rem;'>最終更新: {current_time}</div>", 
                   unsafe_allow_html=True)
    
    def render_sidebar_filters(self):
        """サイドバーフィルター描画"""
        st.sidebar.header("🔍 検索条件")
        
        # 日付範囲
        st.sidebar.subheader("期間指定")
        
        # デフォルト日付（過去30日）
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=30)
        
        start_date = st.sidebar.date_input(
            "開始日",
            value=start_date,
            max_value=end_date
        )
        
        end_date = st.sidebar.date_input(
            "終了日", 
            value=end_date,
            min_value=start_date
        )
        
        # 依頼者フィルター
        st.sidebar.subheader("依頼者")
        requester_list = self.get_requester_list()
        
        if requester_list:
            selected_requester = st.sidebar.selectbox(
                "依頼者選択",
                options=[""] + requester_list,
                index=0
            )
        else:
            selected_requester = st.sidebar.text_input("依頼者（社員番号）")
        
        # ファイルパス検索
        st.sidebar.subheader("ファイル検索")
        file_path = st.sidebar.text_input(
            "ファイルパス（部分一致）",
            placeholder="例: project1, .txt, \\\\server\\share"
        )
        
        # 表示件数
        st.sidebar.subheader("表示設定")
        limit = st.sidebar.selectbox(
            "表示件数",
            options=[100, 500, 1000, 2000],
            index=2
        )
        
        return start_date, end_date, selected_requester, file_path, limit
    
    def render_statistics(self, stats: Dict):
        """統計情報描画"""
        if not stats:
            return
            
        st.subheader("📊 統計情報")
        
        # メトリクス表示
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown('<div class="metric-container success-metric">', unsafe_allow_html=True)
            st.metric(
                label="総ファイル数",
                value=f"{stats['total_files']:,}",
                help="検索条件に一致するアーカイブ済みファイル数"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-container info-metric">', unsafe_allow_html=True)
            st.metric(
                label="総サイズ",
                value=self.format_file_size(stats['total_size']),
                help="アーカイブ済みファイルの合計サイズ"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown('<div class="metric-container success-metric">', unsafe_allow_html=True)
            st.metric(
                label="依頼件数",
                value=f"{stats['total_requests']:,}",
                help="アーカイブ依頼の総数"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            st.markdown('<div class="metric-container info-metric">', unsafe_allow_html=True)
            st.metric(
                label="依頼者数",
                value=f"{stats['total_requesters']:,}",
                help="アーカイブを依頼した人数"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        # 詳細統計情報
        if stats['avg_file_size'] > 0:
            st.markdown("### 📈 詳細統計")
            
            detail_col1, detail_col2 = st.columns(2)
            
            with detail_col1:
                st.write("**ファイルサイズ統計**")
                st.write(f"- 平均サイズ: {self.format_file_size(stats['avg_file_size'])}")
                st.write(f"- 最大サイズ: {self.format_file_size(stats['max_file_size'])}")
            
            with detail_col2:
                if stats['first_archive'] and stats['last_archive']:
                    st.write("**期間情報**")
                    st.write(f"- 最初のアーカイブ: {stats['first_archive'].strftime('%Y-%m-%d %H:%M')}")
                    st.write(f"- 最新のアーカイブ: {stats['last_archive'].strftime('%Y-%m-%d %H:%M')}")
    
    def render_data_table(self, df: pd.DataFrame):
        """データテーブル描画"""
        if df.empty:
            st.warning("📭 検索条件に一致するデータが見つかりません。")
            return
        
        st.subheader(f"📋 アーカイブ履歴 ({len(df):,}件)")
        
        # 表示用のDataFrameを準備
        display_df = df.copy()
        
        # 表示列の選択
        display_columns = {
            'request_id': '依頼ID',
            'requester': '依頼者',
            'request_date': '依頼日時',
            'file_path_short': 'ファイルパス',
            'file_size_mb': 'サイズ(MB)',
            'archive_date': 'アーカイブ日時'
        }
        
        # 列を選択・リネーム
        display_df = display_df[list(display_columns.keys())]
        display_df = display_df.rename(columns=display_columns)
        
        # 日時列のフォーマット
        display_df['依頼日時'] = display_df['依頼日時'].dt.strftime('%Y-%m-%d %H:%M')
        display_df['アーカイブ日時'] = display_df['アーカイブ日時'].dt.strftime('%Y-%m-%d %H:%M')
        
        # テーブル表示
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
        
        # 詳細表示の展開可能セクション
        with st.expander("🔍 詳細情報表示"):
            selected_indices = st.multiselect(
                "詳細を表示するレコードを選択（依頼IDで選択）",
                options=df['request_id'].unique(),
                max_selections=5
            )
            
            if selected_indices:
                detail_df = df[df['request_id'].isin(selected_indices)]
                
                for _, row in detail_df.iterrows():
                    st.markdown(f"**依頼ID: {row['request_id']}**")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"- **依頼者**: {row['requester']}")
                        st.write(f"- **依頼日時**: {row['request_date'].strftime('%Y-%m-%d %H:%M:%S')}")
                        st.write(f"- **ファイルサイズ**: {self.format_file_size(row['file_size'])}")
                    
                    with col2:
                        st.write(f"- **アーカイブ日時**: {row['archive_date'].strftime('%Y-%m-%d %H:%M:%S')}")
                        st.write(f"- **元ファイルパス**: `{row['original_file_path']}`")
                        st.write(f"- **S3パス**: `{row['s3_path']}`")
                    
                    st.markdown("---")
    
    def render_export_section(self, df: pd.DataFrame):
        """エクスポートセクション描画"""
        if df.empty:
            return
            
        st.subheader("📥 データエクスポート")
        
        col1, col2 = st.columns(2)
        
        # ファイル名生成
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        with col1:
            st.markdown("**Excel形式でダウンロード**")
            excel_filename = f"archive_history_{timestamp}.xlsx"
            excel_link = self.create_download_link(df, excel_filename, "excel")
            if excel_link:
                st.markdown(excel_link, unsafe_allow_html=True)
                st.caption("Excelファイル（.xlsx）形式でダウンロード")
        
        with col2:
            st.markdown("**CSV形式でダウンロード**")
            csv_filename = f"archive_history_{timestamp}.csv"
            csv_link = self.create_download_link(df, csv_filename, "csv")
            if csv_link:
                st.markdown(csv_link, unsafe_allow_html=True)
                st.caption("CSVファイル（UTF-8-SIG）形式でダウンロード")
    
    def run(self):
        """メインアプリケーション実行"""
        try:
            # ヘッダー
            self.render_header()
            
            # サイドバーフィルター
            start_date, end_date, requester, file_path, limit = self.render_sidebar_filters()
            
            # 検索実行ボタン
            if st.sidebar.button("🔍 検索実行", type="primary"):
                st.rerun()
            
            # データ検索
            with st.spinner("データを検索中..."):
                df = self.search_archive_history(
                    start_date=start_date,
                    end_date=end_date,
                    requester=requester,
                    file_path=file_path,
                    limit=limit
                )
                
                stats = self.get_statistics(
                    start_date=start_date,
                    end_date=end_date,
                    requester=requester,
                    file_path=file_path
                )
            
            # 統計情報表示
            self.render_statistics(stats)
            
            # データテーブル表示
            self.render_data_table(df)
            
            # エクスポート機能
            self.render_export_section(df)
            
            # フッター
            st.markdown("---")
            st.markdown(
                "<div style='text-align: center; color: #666; font-size: 0.8rem;'>"
                "アーカイブ履歴管理システム v1.0 | "
                f"検索期間: {start_date.strftime('%Y-%m-%d')} ～ {end_date.strftime('%Y-%m-%d')}"
                "</div>",
                unsafe_allow_html=True
            )
            
        except Exception as e:
            st.error(f"アプリケーションエラー: {str(e)}")
            st.exception(e)
        
        finally:
            # データベース接続クローズ
            if self.db_connection:
                try:
                    self.db_connection.close()
                    self.db_connection = None
                except Exception:
                    pass

def main():
    """メイン関数"""
    app = ArchiveHistoryApp()
    app.run()

if __name__ == "__main__":
    main()