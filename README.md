# maintenance_schema

A DBA framework for postgres using views. 

* Default is version 0.0.3 intended for PG10, PG11 and higher. 
* Version 0.0.2 for versions 9.4 to 9.6.
* Version 0.0.1 for version 9.3.

NOTE: To change version of installation, modify the default version in `maintenance_schema.control` before install.

# INSTALL

Put `.sql` and `.control` files in the extension folder.

Use `CREATE EXTENSION` in the wanted database(s).

## **WARNING**: log out and back in for the schema to be loaded in the current user path.

## **NOTE**: intended for postgres user or user with SUPER.

* `rpt_` views are meant as information 
* `audit_` views serve as warnings 
* `dba_` views contain DDL/DML statement (just \gexec it !)

## USAGE of full_report 
NOTE : use in shell to have a "nice" html report
`psql -At -d pgbench -c "select concat(sql_statement , ';') from maintenance_schema.full_report  " | psql -d pgbench -H  -o fullrpt.html`

or

~~~
\H
\C 'FULL REPORT'
\o fullrpt.html
 select sql_statement from maintenance_schema.full_report \gexec
~~~

**WARNING**:
Bloat and fine bloat views might give significant different results (1 order of magnitude for instance). 


# **TODOs** :
- add `ALTER TABLE` statements for autovacuum and autoanalyze based on size (currently abandonned)
- add `ALTER TABLE... SET STATISTICS` if the need is confirmed, watch table/column special statistics in audit
https://blog.pgaddict.com/posts/common-issues-with-planner-statistics (currently abandonned)
- audit check : pg_hba.conf, unix socket,...  security in general, (others?) -> On the way for version 0.0.4
- ~~info from \l+ regarding encoding and collation~~
- users not connected since (...), passwords too short (?), too old (?), last login, number of login, last_reset, IP, hostnossl, host, 0.0.0.0/0, peer, trust, md5 => different extension (with new tables),
- dump of GRANTS/REVOKE, see github of slardiere for the dump
- ~~encoding : to perfect (add server side...)~~
- perfect row filtering on expect_av, foreign_fk (others?),
- ~~fix ioguix query (fine bloat)~~
- ~~add `pg_blocking_pids` for v0.0.3~~
- pg_statio_user_table : ratio/size in `pg_size_pretty()` of read/hit
- ~~list of UNLOGGED tables `SELECT relname FROM pg_class WHERE relpersistence = 'u';`~~
- frozenxid
`SELECT c.oid::regclass as table_name, greatest(age(c.relfrozenxid), age(t.relfrozenxid)) as age 
FROM pg_class c 
LEFT JOIN pg_class t ON c.reltoastrelid = t.oid 
WHERE c.relkind IN ('r', 'm') 
ORDER BY age DESC;` (9.3 and higher!)
- check `archive_command` does not return error
- add extensions pgstattuple, others ?
- probe for replication delay (kb and time) `select pid, client_addr, pg_wal_lsn_diff( sent_lsn, write_lsn ), pg_wal_lsn_diff( sent_lsn, flush_lsn ), pg_wal_lsn_diff( sent_lsn, replay_lsn ), write_lag, flush_lag, replay_lag  from pg_stat_replication ;`
- Version compatibility handling for ~~9.3~~ and 9.4 ~~(and 10 for replication)~~
- Handling of query type/lo ck type/state/wait event on long running queries dba view (restrictions needed)
- Need name of view for each table from full report
- add table_summary (ready): 

`SELECT
 schemaname::text, relname::text,
  seq_tup_read as readfromtscan, 
  idx_tup_fetch as readfromiscan, 
  n_tup_ins as inserted, 
  n_tup_upd as updated, 
  n_tup_del as deleted, 
  n_tup_hot_upd as hotupdated,
 seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd as total_transactions,
 
 case when
   (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd) > 0
 then round(1000000.0 * seq_tup_read / (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd)) / 10000
 else 0
 end::numeric(1000, 4) as select_t_pct,
 
 case when
   (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd) > 0
 then round(1000000.0 * idx_tup_fetch / (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd)) / 10000
 else 0
 end::numeric(1000, 4) as select_i_pct,

 case when (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd) > 0
 then
 round(1000000.0 * n_tup_ins / (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd)) / 10000
 else 0
 end::numeric(1000, 4) as insert_pct ,

 case when (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd) > 0
 then
 round(1000000.0 * n_tup_upd / (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd)) / 10000
 else 0
 end::numeric(1000, 4) as update_pct,

 case when (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd) > 0
 then round(1000000.0 * n_tup_del / (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd)) / 10000
 else 0
 end::numeric(1000, 4) as delete_pct,
 
  case when (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd) > 0
 then round(1000000.0 * n_tup_hot_upd / (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del  + n_tup_hot_upd)) / 10000
 else 0
 end::numeric(1000, 4) as hotupd_pct
from
  pg_stat_user_tables
Where 
   seq_tup_read     != 0
 OR  idx_tup_fetch  != 0
 OR  n_tup_ins      != 0
 OR  n_tup_upd      != 0
 OR  n_tup_del      != 0
 OR  n_tup_hot_upd  != 0
  ORDER BY (seq_tup_read + idx_tup_fetch) DESC, n_tup_ins DESC, n_tup_upd DESC, n_tup_del DESC ;`
  
  ~~COPY/PASTE to 0.0.3   
  `-- encoding for every database
-- Non UTF8 only  
 SELECT datname, pg_encoding_to_char(encoding) as encoding, datcollate, datctype 
 FROM pg_database 
 WHERE  datallowconn 
 AND NOT datistemplate
 AND pg_encoding_to_char(encoding) != 'UTF8';`~~
 
 
 
 - locks  with pg_blocking_pids v9.6+ locked and locking sessions
 `SELECT 
     pgsa_blocked.usename as blocked_user, 
     pgsa_blocked.query as blocked_query,
     pgsa_blocked.datname, 
     pgsa_blocked.wait_event_type, 
     pgsa_blocked.wait_event,  
     pgsa_blocked.pid as blocked_pid, 
     pg_blocking_pids(pgsa_blocked.pid) AS blocking_pids,
     pgsa_blocking.usename as blocking_user, 
     pgsa_blocking.query as blocking_query
 FROM pg_stat_activity AS pgsa_blocked
 JOIN pg_stat_activity AS pgsa_blocking 
   ON  pgsa_blocking.pid = ANY ( SELECT unnest(pg_blocking_pids(pgsa_blocked.pid))) ;
 -- WHERE TRUE = ANY (SELECT unnest(pg_blocking_pids(pgsa_blocked.pid)) IS NOT NULL);`
 
 - ~~UNLOGGED Tables  `SELECT relname FROM pg_class WHERE relpersistence = 'u';`~~
 
 - progress of vacuum 9.6+ (??) `select * from pg_stat_progress_vacuum ;`
 
 - finer AV threshold and expect
 `SELECT psut.relname,
     to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') as last_vacuum,
     to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') as last_autovacuum,
     to_char(pg_class.reltuples, '9G999G999G999') AS n_tup,
     to_char(psut.n_dead_tup, '9G999G999G999') AS dead_tup,
         to_char(psut.n_mod_since_analyze, '9G999G999G999') AS mod_slastnlz,
     to_char(CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
         + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
            * pg_class.reltuples), '9G999G999G999') AS av_threshold,
     CASE
	     WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_dead_tup*1.5
         THEN 'Consider Tuning AV!'
         WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_mod_since_analyze*1.5
         THEN 'Consider Tuning AV!'
         WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_dead_tup
         THEN 'Autovacuum is coming...'
         WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_mod_since_analyze
         THEN 'Autovacuum is coming...'

         ELSE ''
     END AS expect_av
 FROM pg_stat_user_tables psut
     JOIN pg_class on psut.relid = pg_class.oid
 WHERE pg_class.reltuples > 10000
   AND (psut.n_dead_tup >1000 OR psut.n_mod_since_analyze>1000)
 ORDER BY n_tup DESC, dead_tup DESC ;`
 
 - change fk duplicate to list only duplicates 
 `SELECT
    array_agg(pc.conname) as duplicated_constraints, 
    pclsc.relname as child_table,
    pac.attname as child_column,
    pclsp.relname as parent_table,
    pap.attname as parent_column,   
    nspname as schema_name
FROM 
    (
    SELECT
     connamespace,conname, unnest(conkey) as "conkey", unnest(confkey)
      as "confkey" , conrelid, confrelid, contype
     FROM
        pg_constraint
    ) pc
    JOIN pg_namespace pn ON pc.connamespace = pn.oid
    JOIN pg_class pclsc ON pc.conrelid = pclsc.oid
    JOIN pg_class pclsp ON pc.confrelid = pclsp.oid
    JOIN pg_attribute pac ON pc.conkey = pac.attnum and pac.attrelid = pclsc.oid
    JOIN pg_attribute pap ON pc.confkey = pap.attnum and pap.attrelid = pclsp.oid
GROUP BY child_table, child_column, parent_table, parent_column, schema_name HAVING COUNT(*)>1
ORDER BY child_table, child_column`

VERSION COMPATIBILITY
=====================

All views should be compatible from 9.5 and up.
Some views might be compatible starting with 9.4.
I will try to maintain all currently supported versions.



