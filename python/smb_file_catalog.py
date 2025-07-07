#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMB共有ファイルのカタログ化プログラム
Python 3.11.9 対応
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateDisposition, CreateOptions, FileAccessMask
from smbprotocol.query_info import QueryInfoRequest, InfoType, FileInformationClass
from smbprotocol.security_descriptor import SecurityDescriptor
import struct
import json

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('smb_catalog.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SMBFileCatalog:
    """SMBファイルカタログ化クラス"""
    
    def __init__(self, db_config: Dict[str, str]):
        """
        初期化
        
        Args:
            db_config: データベース接続設定
        """
        self.db_config = db_config
        self.connection = None
        self.session = None
        self.tree_connect = None
        
    def connect_database(self) -> bool:
        """データベース接続"""
        try:
            self.db_connection = psycopg2.connect(**self.db_config)
            logger.info("データベース接続成功")
            return True
        except Exception as e:
            logger.error(f"データベース接続失敗: {e}")
            return False
    
    def create_catalog_table(self):
        """カタログテーブル作成"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS file_catalog (
            id SERIAL PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL,
            file_path TEXT NOT NULL,
            file_size BIGINT,
            created_at TIMESTAMP,
            modified_at TIMESTAMP,
            accessed_at TIMESTAMP,
            owner_sid VARCHAR(100),
            owner_domain VARCHAR(100),
            owner_username VARCHAR(100),
            catalog_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(file_path)
        );
        
        CREATE TABLE IF NOT EXISTS file_permissions (
            id SERIAL PRIMARY KEY,
            file_catalog_id INTEGER REFERENCES file_catalog(id) ON DELETE CASCADE,
            permission_type VARCHAR(20) NOT NULL, -- 'allow' or 'deny'
            account_sid VARCHAR(100),
            domain_name VARCHAR(100),
            username VARCHAR(100),
            permission_mask INTEGER,
            permission_description TEXT,
            catalog_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_file_path ON file_catalog(file_path);
        CREATE INDEX IF NOT EXISTS idx_file_name ON file_catalog(file_name);
        CREATE INDEX IF NOT EXISTS idx_catalog_date ON file_catalog(catalog_date);
        CREATE INDEX IF NOT EXISTS idx_owner_username ON file_catalog(owner_username);
        CREATE INDEX IF NOT EXISTS idx_perm_username ON file_permissions(username);
        CREATE INDEX IF NOT EXISTS idx_perm_file_id ON file_permissions(file_catalog_id);
        """
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.db_connection.commit()
                logger.info("カタログテーブル作成完了")
        except Exception as e:
            logger.error(f"テーブル作成失敗: {e}")
            raise
    
    def connect_smb(self, server: str, username: str, password: str, share: str) -> bool:
        """SMB接続"""
        try:
            # SMB接続の確立
            self.connection = Connection(uuid.uuid4(), server, 445)
            self.connection.connect()
            
            # セッション確立
            self.session = Session(self.connection, username, password)
            self.session.connect()
            
            # 共有フォルダ接続
            self.tree_connect = TreeConnect(self.session, f"\\\\{server}\\{share}")
            self.tree_connect.connect()
            
            logger.info(f"SMB接続成功: {server}\\{share}")
            return True
            
        except Exception as e:
            logger.error(f"SMB接続失敗: {e}")
            return False
    
    def get_file_info(self, file_path: str) -> Optional[Dict]:
        """ファイル情報取得（アクセス権情報含む）"""
        try:
            # ファイルオープン
            file_open = Open(self.tree_connect, file_path)
            file_open.create(
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_NON_DIRECTORY_FILE,
                FileAccessMask.FILE_READ_ATTRIBUTES | FileAccessMask.READ_CONTROL
            )
            
            # ファイル基本情報取得
            query = QueryInfoRequest()
            query.info_type = InfoType.FILE
            query.file_information_class = FileInformationClass.FILE_ALL_INFORMATION
            query.file_id = file_open.file_id
            
            response = self.session.send(query)
            data = response['buffer']
            
            # ファイル情報パース
            file_size = struct.unpack('<Q', data[40:48])[0]
            created_time = self._filetime_to_datetime(struct.unpack('<Q', data[8:16])[0])
            modified_time = self._filetime_to_datetime(struct.unpack('<Q', data[24:32])[0])
            accessed_time = self._filetime_to_datetime(struct.unpack('<Q', data[16:24])[0])
            
            # セキュリティ情報取得
            security_info = self.get_security_info(file_open.file_id)
            
            file_open.close()
            
            file_info = {
                'file_name': os.path.basename(file_path),
                'file_path': file_path,
                'file_size': file_size,
                'created_at': created_time,
                'modified_at': modified_time,
                'accessed_at': accessed_time,
                'owner_sid': security_info.get('owner_sid'),
                'owner_domain': security_info.get('owner_domain'),
                'owner_username': security_info.get('owner_username'),
                'permissions': security_info.get('permissions', [])
            }
            
            return file_info
            
        except Exception as e:
            logger.error(f"ファイル情報取得失敗 {file_path}: {e}")
            return None
    
    def get_security_info(self, file_id: bytes) -> Dict:
        """セキュリティ情報取得"""
        try:
            # セキュリティディスクリプタ取得
            query = QueryInfoRequest()
            query.info_type = InfoType.SECURITY
            query.additional_information = 0x00000007  # OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION
            query.file_id = file_id
            
            response = self.session.send(query)
            security_descriptor = SecurityDescriptor()
            security_descriptor.unpack(response['buffer'])
            
            # オーナー情報取得
            owner_info = self.resolve_sid_to_username(security_descriptor.owner)
            
            # アクセス権情報取得
            permissions = []
            if security_descriptor.dacl:
                for ace in security_descriptor.dacl.aces:
                    user_info = self.resolve_sid_to_username(ace.sid)
                    permission = {
                        'permission_type': 'allow' if ace.ace_type == 0 else 'deny',
                        'account_sid': str(ace.sid),
                        'domain_name': user_info.get('domain'),
                        'username': user_info.get('username'),
                        'permission_mask': ace.access_mask,
                        'permission_description': self.decode_permission_mask(ace.access_mask)
                    }
                    permissions.append(permission)
            
            return {
                'owner_sid': str(security_descriptor.owner) if security_descriptor.owner else None,
                'owner_domain': owner_info.get('domain'),
                'owner_username': owner_info.get('username'),
                'permissions': permissions
            }
            
        except Exception as e:
            logger.error(f"セキュリティ情報取得失敗: {e}")
            return {
                'owner_sid': None,
                'owner_domain': None,
                'owner_username': None,
                'permissions': []
            }
    
    def resolve_sid_to_username(self, sid) -> Dict[str, str]:
        """SIDをユーザー名/ドメイン名に変換"""
        try:
            if not sid:
                return {'domain': None, 'username': None}
            
            # SID文字列化
            sid_str = str(sid)
            
            # 既知のSIDパターンをチェック
            well_known_sids = {
                'S-1-1-0': {'domain': 'Everyone', 'username': 'Everyone'},
                'S-1-5-32-544': {'domain': 'BUILTIN', 'username': 'Administrators'},
                'S-1-5-32-545': {'domain': 'BUILTIN', 'username': 'Users'},
                'S-1-5-32-546': {'domain': 'BUILTIN', 'username': 'Guests'},
                'S-1-5-18': {'domain': 'NT AUTHORITY', 'username': 'SYSTEM'},
                'S-1-5-19': {'domain': 'NT AUTHORITY', 'username': 'LOCAL SERVICE'},
                'S-1-5-20': {'domain': 'NT AUTHORITY', 'username': 'NETWORK SERVICE'},
            }
            
            if sid_str in well_known_sids:
                return well_known_sids[sid_str]
            
            # ドメインSIDからユーザー名を推定（実際の実装では、Active Directory等への問い合わせが必要）
            # ここでは簡略化のため、SIDの最後の部分をRIDとして扱う
            sid_parts = sid_str.split('-')
            if len(sid_parts) >= 3:
                domain_sid = '-'.join(sid_parts[:-1])
                rid = sid_parts[-1]
                
                # ドメイン名の推定（実際の環境では適切な方法で取得）
                domain_name = self.get_domain_name_from_sid(domain_sid)
                username = f"User_{rid}"  # 実際の実装では、SIDからユーザー名を解決
                
                return {
                    'domain': domain_name,
                    'username': username
                }
            
            return {'domain': 'Unknown', 'username': f'SID_{sid_str}'}
            
        except Exception as e:
            logger.error(f"SID解決失敗 {sid}: {e}")
            return {'domain': 'Unknown', 'username': str(sid) if sid else 'Unknown'}
    
    def get_domain_name_from_sid(self, domain_sid: str) -> str:
        """ドメインSIDからドメイン名を取得（簡略化版）"""
        # 実際の実装では、Active DirectoryやLDAP等への問い合わせが必要
        # ここでは例として固定値を返す
        domain_mapping = {
            'S-1-5-21-123456789-123456789-123456789': 'EXAMPLE.COM',
            'S-1-5-21-987654321-987654321-987654321': 'TESTDOMAIN.LOCAL',
        }
        
        return domain_mapping.get(domain_sid, 'UNKNOWN_DOMAIN')
    
    def decode_permission_mask(self, mask: int) -> str:
        """アクセス権マスクを文字列に変換"""
        permissions = []
        
        # 標準的なファイルアクセス権
        if mask & 0x00000001:  # FILE_READ_DATA
            permissions.append('読み取り')
        if mask & 0x00000002:  # FILE_WRITE_DATA
            permissions.append('書き込み')
        if mask & 0x00000004:  # FILE_APPEND_DATA
            permissions.append('追加')
        if mask & 0x00000020:  # FILE_EXECUTE
            permissions.append('実行')
        if mask & 0x00000040:  # FILE_DELETE_CHILD
            permissions.append('削除')
        if mask & 0x00010000:  # DELETE
            permissions.append('削除権限')
        if mask & 0x00020000:  # READ_CONTROL
            permissions.append('読み取り制御')
        if mask & 0x00040000:  # WRITE_DAC
            permissions.append('アクセス制御変更')
        if mask & 0x00080000:  # WRITE_OWNER
            permissions.append('所有者変更')
        if mask & 0x10000000:  # GENERIC_ALL
            permissions.append('フルコントロール')
        
        return ', '.join(permissions) if permissions else f'未知の権限(0x{mask:08x})'
    
    def _filetime_to_datetime(self, filetime: int) -> datetime:
        """Windows FILETIMEをdatetimeに変換"""
        if filetime == 0:
            return None
        
        # FILETIMEは1601年1月1日からの100ナノ秒単位
        # Unixエポック（1970年1月1日）との差は11644473600秒
        timestamp = (filetime - 116444736000000000) / 10000000
        return datetime.fromtimestamp(timestamp)
    
    def scan_directory(self, directory_path: str = "") -> List[Dict]:
        """ディレクトリスキャン"""
        files_info = []
        
        try:
            # ディレクトリ一覧取得のためのパス
            search_path = directory_path + "\\*" if directory_path else "*"
            
            # ディレクトリオープン
            dir_open = Open(self.tree_connect, directory_path or "")
            dir_open.create(
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_DIRECTORY_FILE,
                FileAccessMask.FILE_LIST_DIRECTORY
            )
            
            # ディレクトリ内容取得
            query = QueryInfoRequest()
            query.info_type = InfoType.FILE
            query.file_information_class = FileInformationClass.FILE_DIRECTORY_INFORMATION
            query.file_id = dir_open.file_id
            
            # 実際の実装では、ディレクトリ内容を取得する処理が必要
            # ここでは簡略化のため、手動でファイルリストを処理
            
            dir_open.close()
            
        except Exception as e:
            logger.error(f"ディレクトリスキャン失敗 {directory_path}: {e}")
        
        return files_info
    
    def insert_file_info(self, file_info: Dict):
        """ファイル情報をデータベースに挿入（アクセス権情報含む）"""
        try:
            with self.db_connection.cursor() as cursor:
                # ファイル基本情報を挿入/更新
                file_insert_sql = """
                INSERT INTO file_catalog (file_name, file_path, file_size, created_at, modified_at, accessed_at, 
                                         owner_sid, owner_domain, owner_username)
                VALUES (%(file_name)s, %(file_path)s, %(file_size)s, %(created_at)s, %(modified_at)s, %(accessed_at)s,
                        %(owner_sid)s, %(owner_domain)s, %(owner_username)s)
                ON CONFLICT (file_path) DO UPDATE SET
                    file_name = EXCLUDED.file_name,
                    file_size = EXCLUDED.file_size,
                    created_at = EXCLUDED.created_at,
                    modified_at = EXCLUDED.modified_at,
                    accessed_at = EXCLUDED.accessed_at,
                    owner_sid = EXCLUDED.owner_sid,
                    owner_domain = EXCLUDED.owner_domain,
                    owner_username = EXCLUDED.owner_username,
                    catalog_date = CURRENT_TIMESTAMP
                RETURNING id
                """
                
                cursor.execute(file_insert_sql, file_info)
                file_id = cursor.fetchone()[0]
                
                # 既存のアクセス権情報を削除
                cursor.execute("DELETE FROM file_permissions WHERE file_catalog_id = %s", (file_id,))
                
                # 新しいアクセス権情報を挿入
                if file_info.get('permissions'):
                    permission_insert_sql = """
                    INSERT INTO file_permissions (file_catalog_id, permission_type, account_sid, domain_name, 
                                                username, permission_mask, permission_description)
                    VALUES (%(file_catalog_id)s, %(permission_type)s, %(account_sid)s, %(domain_name)s, 
                            %(username)s, %(permission_mask)s, %(permission_description)s)
                    """
                    
                    for permission in file_info['permissions']:
                        permission['file_catalog_id'] = file_id
                        cursor.execute(permission_insert_sql, permission)
                
                self.db_connection.commit()
                logger.debug(f"ファイル情報挿入完了: {file_info['file_path']}")
                
        except Exception as e:
            logger.error(f"ファイル情報挿入失敗: {e}")
            self.db_connection.rollback()
    
    def get_file_permissions_report(self, file_path: str = None) -> List[Dict]:
        """ファイルアクセス権レポート取得"""
        try:
            with self.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                if file_path:
                    # 特定ファイルのアクセス権情報
                    query = """
                    SELECT 
                        fc.file_name,
                        fc.file_path,
                        fc.owner_domain,
                        fc.owner_username,
                        fp.permission_type,
                        fp.domain_name,
                        fp.username,
                        fp.permission_description
                    FROM file_catalog fc
                    LEFT JOIN file_permissions fp ON fc.id = fp.file_catalog_id
                    WHERE fc.file_path = %s
                    ORDER BY fc.file_path, fp.permission_type, fp.domain_name, fp.username
                    """
                    cursor.execute(query, (file_path,))
                else:
                    # 全ファイルのアクセス権情報
                    query = """
                    SELECT 
                        fc.file_name,
                        fc.file_path,
                        fc.owner_domain,
                        fc.owner_username,
                        fp.permission_type,
                        fp.domain_name,
                        fp.username,
                        fp.permission_description
                    FROM file_catalog fc
                    LEFT JOIN file_permissions fp ON fc.id = fp.file_catalog_id
                    ORDER BY fc.file_path, fp.permission_type, fp.domain_name, fp.username
                    """
                    cursor.execute(query)
                
                return cursor.fetchall()
                
        except Exception as e:
            logger.error(f"アクセス権レポート取得失敗: {e}")
            return []
    
    def get_user_access_report(self, domain_name: str = None, username: str = None) -> List[Dict]:
        """ユーザー別アクセス権レポート取得"""
        try:
            with self.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                where_conditions = []
                params = []
                
                if domain_name:
                    where_conditions.append("(fp.domain_name = %s OR fc.owner_domain = %s)")
                    params.extend([domain_name, domain_name])
                
                if username:
                    where_conditions.append("(fp.username = %s OR fc.owner_username = %s)")
                    params.extend([username, username])
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                
                query = f"""
                SELECT DISTINCT
                    COALESCE(fp.domain_name, fc.owner_domain) as domain_name,
                    COALESCE(fp.username, fc.owner_username) as username,
                    fc.file_path,
                    fc.file_name,
                    CASE 
                        WHEN fp.username IS NOT NULL THEN fp.permission_description
                        ELSE 'オーナー'
                    END as access_type
                FROM file_catalog fc
                LEFT JOIN file_permissions fp ON fc.id = fp.file_catalog_id
                WHERE {where_clause}
                ORDER BY domain_name, username, file_path
                """
                
                cursor.execute(query, params)
                return cursor.fetchall()
                
        except Exception as e:
            logger.error(f"ユーザー別アクセス権レポート取得失敗: {e}")
            return []
    
    def catalog_files(self, target_files: List[str]):
        """ファイルカタログ化実行"""
        success_count = 0
        error_count = 0
        
        for file_path in target_files:
            try:
                file_info = self.get_file_info(file_path)
                if file_info:
                    self.insert_file_info(file_info)
                    success_count += 1
                    logger.info(f"処理完了: {file_path}")
                else:
                    error_count += 1
                    logger.warning(f"ファイル情報取得失敗: {file_path}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"ファイル処理エラー {file_path}: {e}")
        
        logger.info(f"カタログ化完了 - 成功: {success_count}, 失敗: {error_count}")
    
    def close_connections(self):
        """接続終了"""
        try:
            if self.tree_connect:
                self.tree_connect.disconnect()
            if self.session:
                self.session.disconnect()
            if self.connection:
                self.connection.disconnect()
            if hasattr(self, 'db_connection'):
                self.db_connection.close()
            logger.info("接続終了")
        except Exception as e:
            logger.error(f"接続終了エラー: {e}")


def main():
    """メイン処理"""
    # データベース設定
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'file_catalog',
        'user': 'postgres',
        'password': 'password'
    }
    
    # SMB接続設定
    smb_server = "192.168.1.100"
    smb_username = "username"
    smb_password = "password"
    smb_share = "shared_folder"
    
    # カタログ化対象ファイルリスト（例）
    target_files = [
        "document1.txt",
        "folder1\\document2.pdf",
        "folder2\\image1.jpg"
    ]
    
    catalog = SMBFileCatalog(db_config)
    
    try:
        # データベース接続
        if not catalog.connect_database():
            sys.exit(1)
        
        # テーブル作成
        catalog.create_catalog_table()
        
        # SMB接続
        if not catalog.connect_smb(smb_server, smb_username, smb_password, smb_share):
            sys.exit(1)
        
        # ファイルカタログ化実行
        catalog.catalog_files(target_files)
        
        # アクセス権レポート出力例
        print("\n=== ファイルアクセス権レポート ===")
        permissions_report = catalog.get_file_permissions_report()
        for row in permissions_report:
            print(f"ファイル: {row['file_path']}")
            print(f"  オーナー: {row['owner_domain']}\\{row['owner_username']}")
            if row['username']:
                print(f"  アクセス権: {row['domain_name']}\\{row['username']} - {row['permission_description']}")
            print("")
        
        # ユーザー別アクセス権レポート出力例
        print("\n=== ユーザー別アクセス権レポート ===")
        user_report = catalog.get_user_access_report()
        current_user = None
        for row in user_report:
            user_key = f"{row['domain_name']}\\{row['username']}"
            if user_key != current_user:
                current_user = user_key
                print(f"\nユーザー: {current_user}")
            print(f"  {row['file_path']} - {row['access_type']}")
        
    except Exception as e:
        logger.error(f"プログラム実行エラー: {e}")
        sys.exit(1)
    
    finally:
        catalog.close_connections()


if __name__ == "__main__":
    main()
