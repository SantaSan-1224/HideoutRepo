select
    TABLESPACE_NAME,
    FILE_NAME,
    BYTES / 1024 / 1024 MBYTES,
    STATUS,
    AUTOEXTENSIBLE,
    MAXBYTES / 1024 / 1024 MAXBYTES,
    INCREMENT_BY
from
    SYS.DBA_TEMP_FILES

union

select
    TABLESPACE_NAME,
    FILE_NAME,
    BYTES / 1024 / 1024 MBYTES,
    STATUS,
    AUTOEXTENSIBLE,
    MAXBYTES / 1024 / 1024 MAXBYTES,
    INCREMENT_BY
from
    SYS.DBA_TEMP_FILES

order by
    TABLESPACE_NAME,
    FILE_NAME
    