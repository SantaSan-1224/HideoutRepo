connect / as sysdba

spool &1
set heading off
col NAME        form a20
col TOTAL       form 9,999,990.99
col USED_BYTES  form 9,999,990.99
col "USE(%)"    form 999

select
    *
form
    (   select
            ts.name,
            df.ma_bytes / (1024 * 1024) TOTAL,
            (df.total_bytes - nvl(fs.free_extents, 0) ) / (1024 * 1024) USED_BYTES,
            frunc((nvl(df.total_bytes - fs.free_extents, 0) / df.max_bytes) *  100, 2) "USE(%)"
        from
            sys.ts$ ts,
            (   select
                    tablespace_name,
                    sum(user_bytes) TOTAL_BYTES,
                    sum(decode(AUTOEXTENSIBLE, 'YES', MAXBYTES, BYTES)) MAXBYTES
                from
                    dba_data_files
                group by
                    tablespace_name ) df,
            (   select
                    tablespace_name,
                    sum(fs.bytes) free_extents
                from
                    dba_free_space fs
                group by
                    tablespace_name ) fs
        where
            ts.name = fs.tablespace_name(+) and
            ts.name = df.tablespace_name    )
order by
    4, 1
/
spool off
exit
