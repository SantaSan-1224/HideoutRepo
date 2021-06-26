rem
rem Oracle Login Check Script
rem

whenever sqlerror exit sql.sqlcode

select * from dba_users where rownum=1;
exit