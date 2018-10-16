# maintenance_schema

A DBA framework for postgres using views. 

* Default is version 0.0.3 intended for PG10 and higher. 
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
`psql -d pgbench -c "select concat(sql_statement , ';') from maintenance_schema.full_report  " | psql -d pgbench -H  -o fullrpt.html`

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
- add `ALTER TABLE` statements for autovacuum and autoanalyze based on size
- add `ALTER TABLE... SET STATISTICS` if the need is confirmed, watch table/column special statistics in audit
- audit check : pg_hba.conf, unix socket,...  security in general, (others?) -> On the way for version 0.0.4
- info from \l+ regarding encodingn and collation
- users not connected since (...), passwords too short (?), too old (?), last login, number of login, last_reset, IP, hostnossl, host, 0.0.0.0/0, peer, trust, md5 => different extension (with new tables),
- dump of GRANTS/REVOKE, see github of slardiere for the dump
- ~~encoding : to perfect (add server side...)~~
- perfect row filtering on expect_av, foreign_fk (others?),
- ~~fix ioguix query (fine bloat)~~
- add `pg_blocking_pids` for v0.0.3
- pg_statio_user_table : ratio/size in `pg_size_pretty()` of read/hit
- frozenxid
`SELECT c.oid::regclass as table_name, greatest(age(c.relfrozenxid), age(t.relfrozenxid)) as age 
FROM pg_class c 
LEFT JOIN pg_class t ON c.reltoastrelid = t.oid 
WHERE c.relkind IN ('r', 'm') 
ORDER BY age DESC;` (9.3 and higher!)
- check `archive_command` does not return error
- add extensions pgstattuple, others ?
- Version compatibility handling for ~~9.3~~ and 9.4 ~~(and 10 for replication)~~
- Handling of query type/lock type/state/wait event on long running queries dba view (restrictions needed)
- Need name of view for each table from full report
- add table_summary : 
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
  ORDER BY (seq_tup_read + idx_tup_fetch) DESC, n_tup_ins DESC, n_tup_upd DESC, n_tup_del DESC ;
  `

VERSION COMPATIBILITY
=====================

All views should be compatible from 9.5 and up.
Some views might be compatible starting with 9.4.
I will try to maintain all currently supported versions.



