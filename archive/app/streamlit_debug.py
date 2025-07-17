#!/usr/bin/env python3

# -*- coding: utf-8 -*-

“””
診断用 Streamlit アプリ（最小構成）
エラー原因の特定用
“””

import streamlit as st
import pandas as pd
import datetime
import traceback
import sys

# ページ設定

st.set_page_config(
page_title=“診断用アプリ”,
page_icon=“🔧”,
layout=“wide”
)

def main():
“”“メイン関数”””
st.title("🔧 診断用アプリ")
st.write("このアプリはエラー原因特定のための最小構成です")

# システム情報表示
st.subheader("🖥️ システム情報")
col1, col2 = st.columns(2)

with col1:
    st.write(f"**Python バージョン**: {sys.version}")
    st.write(f"**Streamlit バージョン**: {st.__version__}")
    st.write(f"**Pandas バージョン**: {pd.__version__}")

with col2:
    st.write(f"**現在時刻**: {datetime.datetime.now()}")
    st.write(f"**プラットフォーム**: {sys.platform}")

# セッション状態テスト
st.subheader("🧪 セッション状態テスト")

# 基本的なセッション状態
if 'counter' not in st.session_state:
    st.session_state.counter = 0

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("カウンターを増加"):
        st.session_state.counter += 1
        st.rerun()

with col2:
    if st.button("カウンターをリセット"):
        st.session_state.counter = 0
        st.rerun()

with col3:
    st.write(f"**カウンター値**: {st.session_state.counter}")

# データフレームテスト
st.subheader("📊 データフレームテスト")

try:
    # サンプルデータの作成
    sample_data = {
        'ID': [1, 2, 3, 4, 5],
        '名前': ['テスト1', 'テスト2', 'テスト3', 'テスト4', 'テスト5'],
        '日付': [datetime.date.today() - datetime.timedelta(days=i) for i in range(5)]
    }
    
    df = pd.DataFrame(sample_data)
    st.dataframe(df, use_container_width=True)
    st.success("✅ データフレーム表示: 正常")
    
except Exception as e:
    st.error(f"❌ データフレーム表示エラー: {str(e)}")
    st.code(traceback.format_exc())

# 設定ファイル読み込みテスト
st.subheader("📁 設定ファイルテスト")

try:
    import json
    config_path = "config/archive_config.json"
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    st.success(f"✅ 設定ファイル読み込み: 正常")
    st.json(config)
    
except FileNotFoundError:
    st.warning(f"⚠️ 設定ファイルが見つかりません: {config_path}")
except Exception as e:
    st.error(f"❌ 設定ファイル読み込みエラー: {str(e)}")
    st.code(traceback.format_exc())

# データベース接続テスト
st.subheader("🗄️ データベース接続テスト")

try:
    import psycopg2
    st.success("✅ psycopg2 インポート: 正常")
    
    # SQLAlchemy テスト
    try:
        from sqlalchemy import create_engine
        st.success("✅ SQLAlchemy インポート: 正常")
    except ImportError:
        st.warning("⚠️ SQLAlchemy がインストールされていません")
    
except ImportError:
    st.warning("⚠️ psycopg2 がインストールされていません")
except Exception as e:
    st.error(f"❌ データベース関連エラー: {str(e)}")
    st.code(traceback.format_exc())

# JavaScript エラー確認
st.subheader("🌐 JavaScript エラー確認")
st.write("ブラウザの開発者ツール（F12）でConsoleタブを確認してください")
st.write("赤いエラーメッセージがある場合は、その内容をお知らせください")

# セッション状態詳細表示
st.subheader("🔍 セッション状態詳細")

with st.expander("セッション状態をすべて表示"):
    for key, value in st.session_state.items():
        st.write(f"**{key}**: {type(value).__name__} = {value}")

# 環境変数表示
st.subheader("🌍 環境変数")

with st.expander("環境変数を表示"):
    import os
    for key, value in os.environ.items():
        if any(keyword in key.upper() for keyword in ['PATH', 'PYTHON', 'STREAMLIT']):
            st.write(f"**{key}**: {value}")
if **name** == “**main**”:
try:
main()
except Exception as e:
st.error(f”❌ アプリケーションエラー: {str(e)}”)
st.code(traceback.format_exc())
    # エラー時のデバッグ情報
    st.subheader("🐛 デバッグ情報")
    st.write("**エラー発生時刻**:", datetime.datetime.now())
    st.write("**Python パス**:", sys.executable)
    st.write("**作業ディレクトリ**:", os.getcwd())