-- アーカイブシステム データベース作成スクリプト (8桁対応版)
-- PostgreSQL用

-- データベース作成（必要に応じて）
-- CREATE DATABASE archive_system;

-- テーブル作成前の準備
DROP TABLE IF EXISTS archive_history CASCADE;

-- アーカイブ履歴テーブル (成功のみ記録、8桁社員番号対応)
CREATE TABLE archive_history (
    id BIGSERIAL PRIMARY KEY,
    request_id VARCHAR(50) NOT NULL,
    requester VARCHAR(8) NOT NULL CHECK (requester ~ '^\d{8}$'),
    request_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    original_file_path TEXT NOT NULL,
    s3_path TEXT NOT NULL,
    archive_date TIMESTAMP NOT NULL,
    file_size BIGINT CHECK (file_size >= 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- インデックス作成（最低限）
-- 依頼者での検索用（必須）
CREATE INDEX idx_archive_history_requester ON archive_history(requester);

-- 依頼日時での検索用（必須）
CREATE INDEX idx_archive_history_request_date ON archive_history(request_date);

-- 複合インデックス（依頼者+日付での絞り込み用）
CREATE INDEX idx_archive_history_requester_date ON archive_history(requester, request_date);

-- updated_atの自動更新用トリガー関数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- updated_atの自動更新トリガー
CREATE TRIGGER update_archive_history_updated_at 
    BEFORE UPDATE ON archive_history 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 必要に応じてtrigramエクステンションを有効化（ファイルパス検索用）
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- テーブル作成確認用クエリ
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'archive_history' 
ORDER BY ordinal_position;

-- インデックス確認用クエリ
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'archive_history';

-- サンプルデータ挿入（テスト用）
INSERT INTO archive_history (
    request_id,
    requester,
    request_date,
    original_file_path,
    s3_path,
    archive_date,
    file_size
) VALUES 
    ('REQ-2025-001', '12345678', '2025-01-15 10:00:00', '\\\\fileserver\\dept1\\project\\file1.txt', 's3://bucket/fileserver/dept1/project/file1.txt', '2025-01-15 10:30:00', 1024),
    ('REQ-2025-002', '12345679', '2025-01-16 14:30:00', '\\\\fileserver\\dept2\\archive\\file2.pdf', 's3://bucket/fileserver/dept2/archive/file2.pdf', '2025-01-16 15:00:00', 2048),
    ('REQ-2025-003', '12345680', '2025-01-17 09:15:00', '\\\\fileserver\\dept1\\temp\\file3.xlsx', 's3://bucket/fileserver/dept1/temp/file3.xlsx', '2025-01-17 09:45:00', 4096);

-- 動作確認用クエリ
SELECT * FROM archive_history ORDER BY created_at DESC;