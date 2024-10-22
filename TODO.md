 
# **TODOs** :
==============

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
- duplicate tables (1/ compare structure 2/ compare rows)
- duplicate columns (denormalisation) incremental name columns
- order by DESC for nb of indexes ` select schemaname, tablename, count(indexname) as nb_idx from pg_indexes group by schemaname, tablename order by 3 desc ;`
- Table with the most colums : 
~~~~sql
select tab.table_schema,
       tab.table_name,
       count(*) as columns
from information_schema.tables tab
inner join information_schema.columns col
           on tab.table_schema = col.table_schema
           and tab.table_name = col.table_name
where tab.table_schema not in ('information_schema', 'pg_catalog')
      and tab.table_type = 'BASE TABLE'
group by tab.table_schema, tab.table_name
order by count(*) desc;
~~~~
- size for all elements of a relation (https://www.2ndquadrant.com/en/blog/optimizing-storage-small-tables-postgresql-12/): 
~~~~sql
select
pg_relation_size(c.oid, 'main') as heap_size,
pg_relation_size(c.oid, 'fsm') as fsm_size,
pg_relation_size(c.oid, 'vm') as vm_size,
pg_relation_size(c.reltoastrelid) as toast_table_size,
pg_relation_size(i.indexrelid) as toast_index_size
from pg_class c
left outer join pg_index i on c.reltoastrelid=i.indrelid ;
~~~~
- ~~tables with a single column~~
- ~~unused tables~~
~~~~sql
Select relname from
pg_stat_user_tables
WHERE (idx_tup_fetch + seq_tup_read)= 0; -- tables where no tuple is read either from seqscan or idx
~~~~
- ~~schema with tables of the same name (check for same structure)~~
~~~~sql
SELECT
n.nspname as "Schema",
c.relname as "Name" FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname IN (SELECT relname FROM pg_catalog.pg_class
WHERE relkind IN ('r')
GROUP BY relname
Having count(relname) > 1)
ORDER BY 2,1;
~~~~
- ~~empty tables~~
~~~~sql
Select relname from
pg_stat_user_tables
WHERE n_live_tup = 0;
~~~~
- add extensions pgstattuple, pg_stat_statements, others... ?
- pg_stat_statement 
   - unused columns
   - missing indexes
   - most calls, total_time, avg, and stddev (biggest gap between short and long)
   - most time/IO/CPU consuming queries :
 ~~~~sql
SELECT  substring(query, 1, 50) AS short_query,
round(total_time::numeric, 2) AS total_time,
calls,
round(mean_time::numeric, 2) AS mean,
round((100 * total_time /
sum(total_time::numeric) OVER ())::numeric, 2) AS percentage_cpu
FROM    pg_stat_statements
ORDER BY total_time DESC
LIMIT 20;
 ~~~~
   - start with these queries (finer):
~~~~sql
SELECT sum(total_time) AS total_time,
sum(blk_read_time + blk_write_time) AS io_time,
sum(total_time - blk_read_time - blk_write_time) AS cpu_time,
sum(calls) AS ncalls,
sum(rows) AS total_rows
FROM pg_stat_statements
WHERE dbid IN (SELECT oid FROM pg_database WHERE datname=current_database());
-- AND 
WITH ttl AS (
SELECT sum(total_time) AS total_time, sum(blk_read_time + blk_write_time) AS io_time,
sum(total_time - blk_read_time - blk_write_time) AS cpu_time,
sum(calls) AS ncalls, sum(rows) AS total_rows
FROM pg_stat_statements WHERE dbid IN (
SELECT oid FROM pg_database WHERE datname=current_database())
)
SELECT *,(pss.total_time-pss.blk_read_time-pss.blk_write_time)/ttl.cpu_time*100 cpu_pct
FROM pg_stat_statements pss, ttl
WHERE (pss.total_time-pss.blk_read_time-pss.blk_write_time)/ttl.cpu_time >= 0.05
ORDER BY pss.total_time-pss.blk_read_time-pss.blk_write_time DESC LIMIT 1;
~~~~
- ~~probe for replication delay (kb and time)~~ 
~~~~sql
select pid, client_addr, pg_wal_lsn_diff( sent_lsn, write_lsn ), pg_wal_lsn_diff( sent_lsn, flush_lsn ), pg_wal_lsn_diff( sent_lsn, replay_lsn ), write_lag, flush_lag, replay_lag  
from pg_stat_replication ;
~~~~
~~missing indexes by perf ~~ 
~~~~sql
-- By Laurenz Albe (Github) 
-- Filter bigger tables (at least 500 tuples) an index would not be used if there the table is too small anyway
SELECT relname,
       seq_scan,
       seq_tup_read / seq_scan AS tup_per_scan
FROM pg_stat_user_tables
WHERE seq_scan > 0;
~~~~

- ~~replication conflicts : ~~
~~~~sql
select datname as dbname_w_replica_conflicts, confl_tablespace,confl_lock,confl_snapshot,confl_bufferpin,confl_deadlock 
from pg_stat_database_conflicts 
where datname not like 'template%' 
and (confl_tablespace + confl_lock + confl_snapshot + confl_bufferpin + confl_deadlock)>0;
~~~~

- ~~count for commits, rollbacks and deadlocks when deadlocks are found :~~
~~~~sql
select datname as dbname_w_deadlocks, numbackends, xact_commit, xact_rollback, deadlocks 
from pg_stat_database 
where datname not like 'template%' 
and deadlocks>0 ;
~~~~
- ~~alert on temp files with current work_mem setting :~~
~~~~sql
select datname as dbname_w_tmpfiles, 
temp_files as nb_tmp_files, 
pg_size_pretty(temp_bytes) as tmp_fsize, 
pg_size_pretty(temp_bytes/temp_files) as avg_tmpfsize, 
( select setting::numeric || unit  from pg_settings where name = 'work_mem') as current_work_mem 
from pg_stat_database where temp_files>0;
~~~~
- Version compatibility handling for ~~9.3~~ and 9.4 ~~(and 10 for replication)~~
- Handling of query type/lock type/state/wait event on long running queries dba view (restrictions needed)
- Need name of view for each table from full report
- ~~add table_summary (ready):~~

~~`SELECT
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
  ORDER BY (seq_tup_read + idx_tup_fetch) DESC, n_tup_ins DESC, n_tup_upd DESC, n_tup_del DESC ;`~~
  
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
 
 - ~~progress of vacuum 9.6+ (??) `select * from pg_stat_progress_vacuum ;`~~ 
 USELESS
 
 - finer AV threshold and expect
 ~~~~sql
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
 ORDER BY n_tup DESC, dead_tup DESC ;
 ~~~~

 - ~~add schema to seqrep view to be used in multi schema environment!~~

 - ~~change fk duplicate to list only duplicates~~
 ~~~~sql
 SELECT
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
~~~~

- Deduplication (moved to find_duplicate_keys): 
~~~~sql
CREATE OR REPLACE VIEW maintenance_schema.rpt_duplicate_rows AS
-- WITH list_tables as (
--  SELECT FORMAT('%I.%I', nspname, relname) FROM pg_class pgc JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid )
-- )
--SELECT
--FORMAT('SELECT distinct * FROM %I.%I HAVING COUNT(*) >1 ', nspname, relname ) AS SQL_statement
--FROM (
 SELECT FORMAT('SELECT distinct * FROM %I.%I HAVING COUNT(*) >1', nspname, relname) as sql_statement FROM pg_class pgc JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
--ORDER BY sql_statement;
SELECT pgc1.connamespace, pgc1.conrelid::regclass, pgc2.relname, pgc1.conkey FROM pg_constraint pgc1 JOIN pg_class pgc2 ON pgc1.conrelid = pgc2.oid JOIN pg_class pgc3 ON pgc3.oid::text = pgc1.conkey::text WHERE pgc1.contype ='p' ;

 SELECT FORMAT('SELECT array_agg(%I), count(*) FROM %I.%I GROUP BY %I HAVING COUNT(*) >1',column_name, column_name, table_schema , table_name) as sql_statement FROM (
-- clefs primaires
        select tc.table_schema, tc.table_name, kc.column_name
from information_schema.table_constraints tc
  join information_schema.key_column_usage kc
    on kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name
where tc.constraint_type = 'PRIMARY KEY'
  and kc.ordinal_position is not null
order by tc.table_schema,
         tc.table_name,
         kc.position_in_unique_constraint
) list_pk

-- colonnes
SELECT *
FROM information_schema.columns
WHERE table_schema = 'your_schema'
  AND table_name   = 'your_table'

-- see duplicate lines/rows/tuples
SELECT  content, count(content), array_agg(id)  FROM public.dummy GROUP BY content HAVING count(content) >1;
~~~~

Find tables which have autovacuum disabled : 
~~~~sql
select 
	relnamespace::regnamespace as schema_name, 
       relname as table_name
from 
	pg_class 
where 'autovacuum_enabled=false' = any(reloptions);
~~~~
Tables with the most indexes

~~~~sql
select tab.table_schema,
       tab.table_name,
       count(*) as columns
from information_schema.tables tab
inner join information_schema.columns col
           on tab.table_schema = col.table_schema
           and tab.table_name = col.table_name
where tab.table_schema not in ('information_schema', 'pg_catalog')
      and tab.table_type = 'BASE TABLE'
group by tab.table_schema, tab.table_name
order by count(*) desc;
moozik_pg=> SELECT
    nsp.nspname AS schema_name,
    cls.relname AS table_name,
    COUNT(idx.indexrelid) AS index_count
FROM
    pg_class cls
    JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
    JOIN pg_index idx ON cls.oid = idx.indrelid
WHERE
    cls.relkind = 'r' -- Only ordinary tables
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Exclude system schemas
GROUP BY
    nsp.nspname,
    cls.relname
ORDER BY
    index_count DESC
LIMIT 10; -- Adjust or remove LIMIT as needed

~~~~
Columns with the most indexes
~~~~sql
SELECT
    nsp.nspname AS schema_name,
    cls.relname AS table_name,
    att.attname AS column_name,
    COUNT(idx.indexrelid) AS index_count
FROM
    pg_class cls
    JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
    JOIN pg_index idx ON cls.oid = idx.indrelid
    JOIN pg_attribute att ON att.attrelid = cls.oid
        AND att.attnum = ANY(idx.indkey)
WHERE
    cls.relkind = 'r' -- Only ordinary tables
    AND nsp.nspname NOT IN ('pg_catalog', 'information_schema') -- Exclude system schemas
    AND NOT att.attisdropped -- Exclude dropped columns
GROUP BY
    nsp.nspname,
    cls.relname,
    att.attname
ORDER BY
    index_count DESC
LIMIT 10; -- Adjust or remove LIMIT as needed

~~~~
List all non-system functions, triggers and stored procedures
~~~~sql
SELECT
    n.nspname AS schema_name,
    p.proname AS function_name,
    pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
    pg_catalog.pg_get_function_result(p.oid) AS return_type,
    CASE
        WHEN p.prokind = 'a' THEN 'Aggregate Function'
        WHEN p.prokind = 'w' THEN 'Window Function'
        WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'Trigger Function'
        ELSE 'Normal Function'
    END AS function_type,
    pg_catalog.pg_get_functiondef(p.oid) AS definition
FROM
    pg_catalog.pg_proc p
    INNER JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND p.prokind IN ('f', 'a', 'w') -- 'f' = normal function, 'a' = aggregate, 'w' = window
    AND pg_catalog.pg_function_is_visible(p.oid)
ORDER BY
    schema_name,
    function_name;

~~~~
List all FKs, constraints (check and UNIQUE): DROP to speed up INSERTS 
~~~~sql
SELECT 
    tc.table_schema, 
    tc.table_name, 
    tc.constraint_name, 
    tc.constraint_type,
    CASE 
        WHEN tc.constraint_type = 'FOREIGN KEY' THEN
            ccu.table_name || '(' || ccu.column_name || ')'
        ELSE
            array_to_string(array_agg(kcu.column_name), ', ')
    END AS constrained_columns,
    CASE
        WHEN tc.constraint_type = 'CHECK' THEN
            pg_get_constraintdef(c.oid)
        ELSE
            NULL
    END AS check_clause
FROM 
    information_schema.table_constraints tc
JOIN 
    information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name 
    AND tc.table_schema = kcu.table_schema
LEFT JOIN 
    information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
LEFT JOIN 
    pg_constraint c ON c.conname = tc.constraint_name
WHERE 
    tc.constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE')
    AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
GROUP BY 
    tc.table_schema, 
    tc.table_name, 
    tc.constraint_name, 
    tc.constraint_type,
    ccu.table_name,
    ccu.column_name,
    c.oid
ORDER BY 
    tc.table_schema, 
    tc.table_name, 
    tc.constraint_name;
~~~~
Wraparound with percentage 
~~~~~sql
select datname, max(age(datfrozenxid)) as dboldest_fxid, pg_settings.setting::int as max_age, 
round(((pg_settings.setting::numeric - max(age(datfrozenxid)::numeric) )/pg_settings.setting::numeric)*100, 2) as pct_fxid_remaining
from pg_database  , pg_settings
WHERE pg_settings.name  = 'autovacuum_freeze_max_age' 
GROUP BY datname, datminmxid, pg_settings.setting
ORDER BY max(age(datfrozenxid)) DESC;

~~~~

~~Alignment padding for columns : ~~
~~~~sql
--[EXPERIMENTAL] Alignment Padding. How many bytes can be saved if columns are ordered better?

-- TODO: not-yet-analyzed tables – show a warning (cannot get n_live_tup -> cannot get total bytes)
-- TODO: NULLs
-- TODO: simplify, cleanup
-- TODO: chunk_size 4 or 8
with recursive constants as (
  select 8 as chunk_size
), columns as (
  select
    true as is_orig,
    table_schema,
    table_name,
    ordinal_position,
    column_name,
    udt_name,
    typalign,
    typlen,
    case typalign -- see https://www.postgresql.org/docs/current/static/catalog-pg-type.html
      when 'c' then
        case when typlen > 0 then typlen % chunk_size else 0 end
      when 's' then 2
      when 'i' then 4
      when 'd' then 8
      else null
    end as _shift,
    case typalign
      when 's' then 1
      when 'i' then 2
      when 'd' then 3
      when 'c' then
        case when typlen > 0 then typlen % chunk_size else 9 end
      else 9
    end as alt_order_group,
    character_maximum_length
  from information_schema.columns
  join constants on true
  join pg_type on udt_name = typname
  where table_schema not in ('information_schema', 'pg_catalog')
), alt_columns as (
  select
    false as is_orig,
    table_schema,
    table_name,
    row_number() over (partition by table_schema, table_name order by alt_order_group, column_name) as ordinal_position,
    column_name,
    udt_name,
    typalign,
    typlen,
    _shift,
    alt_order_group,
    character_maximum_length
  from columns
), combined_columns as (
  select *, coalesce(character_maximum_length, _shift) as shift
  from columns
  union all
  select *, coalesce(character_maximum_length, _shift) as shift
  from alt_columns
), analyze_alignment as (
  select
    is_orig,
    table_schema,
    table_name,
    0 as analyzed,
    (select chunk_size from constants) as left_in_chunk,
    '{}'::text[] as padded_columns,
    '{}'::int[] as pads,
    (select max(ordinal_position) from columns c where c.table_name = _.table_name and c.table_schema = _.table_schema) as col_cnt,
    array_agg(_.column_name::text order by ordinal_position) as cols,
    array_agg(_.udt_name::text order by ordinal_position) as types,
    array_agg(shift order by ordinal_position) as shifts,
    null::int as curleft,
    null::text as prev_column_name,
    false as has_varlena
  from
    combined_columns _
  group by is_orig, table_schema, table_name
  union all
  select
    is_orig,
    table_schema,
    table_name,
    analyzed + 1,
    cur_left_in_chunk,
    case when padding_occured > 0 then padded_columns || array[prev_column_name] else padded_columns end,
    case when padding_occured > 0 then pads || array[padding_occured] else pads end,
    col_cnt,
    cols,
    types,
    shifts,
    cur_left_in_chunk,
    ext.column_name as prev_column_name,
    a.has_varlena or (ext.typlen = -1) -- see https://www.postgresql.org/docs/current/static/catalog-pg-type.html
  from analyze_alignment a, constants, lateral (
    select
      shift,
      case when left_in_chunk < shift then left_in_chunk else 0 end as padding_occured,
      case when left_in_chunk < shift then chunk_size - shift % chunk_size else left_in_chunk - shift end as cur_left_in_chunk,
      column_name,
      typlen
    from combined_columns c, constants
    where
      ordinal_position = a.analyzed + 1
      and c.is_orig = a.is_orig
      and c.table_name = a.table_name
      and c.table_schema = a.table_schema
  ) as ext
  where
    analyzed < col_cnt and analyzed < 1000/*sanity*/
), result_pre as (
  select distinct on (is_orig, table_schema, table_name)
    is_orig ,
    table_schema as schema_name,
    table_name,
    padded_columns,
    case when curleft % chunk_size > 0 then pads || array[curleft] else pads end as pads,
    curleft,
    coalesce((select sum(p) from unnest(pads) _(p)), 0) + (chunk_size + a1.curleft) % chunk_size as padding_sum,
    n_live_tup,
    n_dead_tup,
    c.oid as oid,
    pg_total_relation_size(c.oid) - pg_indexes_size(c.oid) - coalesce(pg_total_relation_size(reltoastrelid), 0) as table_bytes,
    cols,
    types,
    shifts,
    analyzed,
    a1.has_varlena
  from analyze_alignment a1
  join pg_namespace n on n.nspname = table_schema
  join pg_class c on n.oid = c.relnamespace and c.relname = table_name
  join pg_stat_user_tables s on s.schemaname = table_schema and s.relname = table_name
  join constants on true
  order by is_orig, table_schema, table_name, analyzed desc
), result_both as (
  select
    *,
    padding_sum * (n_live_tup + n_dead_tup) as padding_total_est
  from result_pre
), result as (
  select
    r1.schema_name,
    r1.table_name,
    r1.table_bytes,
    r1.n_live_tup,
    r1.n_dead_tup,
    r1.padding_total_est - coalesce(r2.padding_total_est, 0) as padding_total_est,
    r1.padding_sum - coalesce(r2.padding_sum, 0) as padding_sum,
    r1.padding_sum as r1_padding_sum,
    r1.padding_total_est as r1_padding_total_est,
    r2.padding_sum as r2_padding_sum,
    r2.padding_total_est as r2_padding_total_est,
    r1.cols,
    r1.types,
    r1.shifts,
    r2.cols as alt_cols,
    r2.types as alt_types,
    r2.shifts as alt_shits,
    r1.pads,
    r1.curleft,
    r2.pads as alt_pads,
    r2.curleft as alt_curleft,
    r1.padded_columns,
    r1.analyzed,
    r1.has_varlena,
    case
      when r1.table_bytes > 0 then
        round(100 * (r1.padding_sum - coalesce(r2.padding_sum, 0))::numeric * (r1.n_live_tup + r1.n_dead_tup)::numeric / r1.table_bytes, 2)
      else 0
    end as wasted_percent
  from result_both r1
  join result_both r2 on r1.is_orig and not r2.is_orig and r1.schema_name = r2.schema_name and r1.table_name = r2.table_name
)
select
  coalesce(nullif(schema_name, 'public') || '.', '') || table_name as "Table",
  pg_size_pretty(table_bytes) "Table Size",
  case when has_varlena then 'Includes VARLENA' else null end as "Comment",
  case
    when padding_total_est > 0 then '~' || pg_size_pretty(padding_total_est) || ' (' || wasted_percent::text || '%)'
    else ''
  end as "Wasted *",
  case
    when padding_total_est > 0 then (
      with cols1(c) as (
        select array_to_string(array_agg(elem::text), ', ')
        from (select * from unnest(alt_cols) with ordinality as __(elem, i)) _
        group by (i - 1) / 3
        order by (i - 1) / 3
      )
      select array_to_string(array_agg(c), e'\n') from cols1
    )
    else null
  end as "Suggested Columns Reorder"
  --case when padding_total_est > 0 then array_to_string(alt_cols, ', ') else null end as "Suggested Columns Reorder"
from result r1
order by table_bytes desc
;


~~~~
