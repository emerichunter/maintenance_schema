\echo Use "CREATE EXTENSION maintenance_schema" to load this file. \quit

/* TODOs :
- audit check : pg_hba.conf, listen_addresses, unix socket,...
- encoding : to perfect
- filter rows on expect_av, foreign_fk (others?)
- add pgstattuple, others ?
*/


CREATE SCHEMA IF NOT EXISTS maintenance_schema AUTHORIZATION current_user;


-- VIEW last analyze et vacuum avec filtres sur toutes les tables/bases du cluster
CREATE OR REPLACE VIEW maintenance_schema.report_last_analyze_vacuum AS
select 
	   c.relname,
	   c.relowner,
	   c.relpages,            
       c.reltuples,           
       c.relallvisible,
       c.relkind,
       c.relfrozenxid,
       sat.schemaname, 
       sat.last_vacuum, 
       sat.last_autovacuum, 
       sat.last_analyze, 
       sat.last_autoanalyze 
from pg_stat_user_tables sat
JOIN pg_class c ON c.relname= sat.relname 
WHERE c.reltuples>=100  -- minimum 100 tuples 
AND ((sat.last_vacuum < CURRENT_TIMESTAMP - INTERVAL '7 days' OR sat.last_analyze < CURRENT_TIMESTAMP - INTERVAL '7 days') OR (sat.last_vacuum IS NULL OR sat.last_analyze IS NULL ) )
order by last_vacuum desc NULLS FIRST,last_analyze desc;




-- report bloat approx 
-- from check_postgres
CREATE OR REPLACE VIEW maintenance_schema.report_bloat_approx AS
SELECT current_database, schemaname, tablename, tbloat, wastedbytes/1024/1024 as tablewaste, iname, ibloat, wastedibytes/1024/1024 as indexwaste
FROM (

SELECT
  current_database(), schemaname, tablename, 
  ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,
  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,
  iname, 
  ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat,
  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes
FROM (
  SELECT
    schemaname, tablename, cc.reltuples, cc.relpages, bs,
    CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,
    COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta -- very rough approximation, assumes all cols
  FROM (
    SELECT
      ma,bs,schemaname,tablename,
      (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,
      (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        schemaname, tablename, hdr, ma, bs,
        SUM((1-null_frac)*avg_width) AS datawidth,
        MAX(null_frac) AS maxfracsum,
        hdr+(
          SELECT 1+COUNT(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
        ) AS nullhdr
      FROM pg_stats s, (
        SELECT
          (SELECT current_setting('block_size')::NUMERIC) AS bs,
          CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  JOIN pg_class cc ON cc.relname = rs.tablename
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml
ORDER BY wastedbytes DESC
) AS t1 
WHERE (tbloat >1.0 OR ibloat >1.0 )
AND (wastedbytes/1024/1024> 100 OR wastedibytes/1024/1024> 100)
;


-- report or unused indexes
CREATE OR REPLACE VIEW maintenance_schema.report_unused_idx
AS
SELECT 
	pg_sui.relname, 
	indexrelname, 
	pg_c.reltuples ,
	pg_sui.idx_scan as idx_scan_count,  
	pg_sut.idx_scan as tot_idx_scan_table, 
	pg_sut.seq_scan
FROM pg_stat_user_indexes pg_sui
JOIN pg_class pg_c ON pg_sui.relname = pg_c.relname
JOIN pg_stat_user_tables pg_sut ON pg_c.relname = pg_sut.relname
JOIN pg_indexes pg_is ON pg_sui.indexrelname=pg_is.indexname
WHERE (pg_sui.idx_scan = 0 OR (pg_sut.idx_scan < pg_sut.seq_scan AND  pg_sui.idx_scan < pg_sut.seq_scan) )
  AND pg_sut.seq_scan <> 0     
  AND pg_c.reltuples > 500
  AND pg_is.indexdef  !~* 'unique' 
  AND seq_scan <> (date_part('day', now()) - date_part('day', GREATEST(last_analyze,last_autoanalyze )))+1
ORDER BY relname
;
		
	
-- report of most full scanned tables	
CREATE OR REPLACE VIEW maintenance_schema.report_mostfullscaned
AS
SELECT pg_sut.relname as nom_table, seq_scan, COALESCE(seq_tup_read,0) as fullscan_tuples,
                COALESCE(idx_scan,0) as idx_scan, COALESCE(idx_tup_fetch,0) as idxscan_tuples,
                COALESCE(ROUND((seq_scan*1.0)/(NULLIF(idx_scan,0)*1.0),2),0) as ratio_scan,
                COALESCE(ROUND((seq_tup_read*1.0)/(NULLIF(idx_tup_fetch,0)*1.0),2),0) as ratio_tup,
                (COALESCE(float4(seq_tup_read),0)*1.0/(NULLIF(pg_c.reltuples,0)*1.0)) as ratio_tot_seq,
                (COALESCE(float4(idx_tup_fetch),0)*1.0/(NULLIF(pg_c.reltuples,0)*1.0)) as ratio_tot_idx,
                COALESCE(pg_c.reltuples,0) as est_tuple,
                greatest(last_autoanalyze, last_analyze) as last_analyze

	FROM pg_stat_user_tables pg_sut 
	JOIN pg_class pg_c ON pg_sut.relname = pg_c.relname
	WHERE ( seq_tup_read > 3*pg_c.reltuples OR idx_scan < 100*seq_scan)
	  AND pg_c.reltuples >500
	  AND seq_tup_read <> 0
	  AND seq_tup_read IS NOT NULL
	  AND ((COALESCE(float4(seq_tup_read),0)*1.0/(NULLIF(pg_c.reltuples,0)*1.0)) >0.01
	   OR  COALESCE(ROUND((seq_tup_read*1.0)/(NULLIF(idx_tup_fetch,0)*1.0),2),0)> 0.01)
	ORDER BY seq_scan DESC
	;

		
-- report index usage
-- from wiki
CREATE OR REPLACE VIEW maintenance_schema.report_index_usage
AS
SELECT
    t.tablename,
    indexname,
    c.reltuples AS num_rows,
    pg_size_pretty(pg_relation_size(quote_ident(t.tablename)::text)) AS table_size,
    pg_size_pretty(pg_relation_size(quote_ident(indexrelname)::text)) AS index_size,
    CASE WHEN indisunique THEN 'Y'
       ELSE 'N'
    END AS UNIQUE,
    idx_scan AS number_of_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_tables t
LEFT OUTER JOIN pg_class c ON t.tablename=c.relname
LEFT OUTER JOIN
    ( SELECT c.relname AS ctablename, ipg.relname AS indexname, x.indnatts AS number_of_columns, idx_scan, idx_tup_read, idx_tup_fetch, indexrelname, indisunique FROM pg_index x
           JOIN pg_class c ON c.oid = x.indrelid
           JOIN pg_class ipg ON ipg.oid = x.indexrelid
           JOIN pg_stat_user_indexes psai ON x.indexrelid = psai.indexrelid )
    AS foo
    ON t.tablename = foo.ctablename
WHERE t.schemaname NOT IN ('information_schema', 'pg_catalog')
ORDER BY t.tablename, indexname;
		
-- report index usage by count
-- from wiki
-- TO DROP?
CREATE OR REPLACE VIEW maintenance_schema.report_indexusagecounts 
AS 
SELECT
    schemaname::text,
    relname::text,
    indexrelname::text,
    idx_scan,
    pg_size_pretty(pg_relation_size(i.indexrelid)) as index_size
FROM
  pg_stat_user_indexes i
JOIN
  pg_index using (indexrelid)
WHERE
  indisunique is false
ORDER BY
  idx_scan desc, pg_relation_size(i.indexrelid) desc;

  
-- report of unindex foreign keys
-- from Josh Berkus
CREATE OR REPLACE VIEW maintenance_schema.report_unindexed_fk AS
WITH y AS (
    SELECT
        pg_catalog.format('%I.%I', n1.nspname, c1.relname)  AS referencing_tbl,
        pg_catalog.quote_ident(a1.attname) AS referencing_column,
        t.conname AS existing_fk_on_referencing_tbl,
        pg_catalog.format('%I.%I', n2.nspname, c2.relname) AS referenced_tbl,
        pg_catalog.quote_ident(a2.attname) AS referenced_column,
        pg_relation_size( pg_catalog.format('%I.%I', n1.nspname, c1.relname) ) AS referencing_tbl_bytes,
        pg_relation_size( pg_catalog.format('%I.%I', n2.nspname, c2.relname) ) AS referenced_tbl_bytes,
        pg_catalog.format($$CREATE INDEX ON %I.%I(%I);$$, n1.nspname, c1.relname, a1.attname) AS suggestion
    FROM pg_catalog.pg_constraint t
    JOIN pg_catalog.pg_attribute  a1 ON a1.attrelid = t.conrelid AND a1.attnum = t.conkey[1]
    JOIN pg_catalog.pg_class      c1 ON c1.oid = t.conrelid
    JOIN pg_catalog.pg_namespace  n1 ON n1.oid = c1.relnamespace
    JOIN pg_catalog.pg_class      c2 ON c2.oid = t.confrelid
    JOIN pg_catalog.pg_namespace  n2 ON n2.oid = c2.relnamespace
    JOIN pg_catalog.pg_attribute  a2 ON a2.attrelid = t.confrelid AND a2.attnum = t.confkey[1]
    WHERE t.contype = 'f'
    AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_index i
        WHERE i.indrelid = t.conrelid
        AND i.indkey[0] = t.conkey[1]
    )
)
SELECT  referencing_tbl,
        referencing_column,
        existing_fk_on_referencing_tbl,
        referenced_tbl,
        referenced_column,
        pg_size_pretty(referencing_tbl_bytes) AS referencing_tbl_size,
        pg_size_pretty(referenced_tbl_bytes) AS referenced_tbl_size,
        suggestion
FROM y
ORDER BY
    referencing_tbl_bytes DESC,
    referenced_tbl_bytes DESC,
    referencing_tbl,
    referenced_tbl,
    referencing_column,
    referenced_column;

-- Duplicate foreign keys
CREATE OR REPLACE VIEW maintenance_schema.report_duplicate_fk AS
SELECT
    pc.conname as constraint_name, 
    pclsc.relname as child_table,
    pac.attname as child_column,
    pclsp.relname as parent_table,
    pap.attname as parent_column,   
    nspname as schema_name
FROM 
    (
    SELECT
         connamespace,
		 conname, 
		 unnest(conkey) as "conkey", 
		 unnest(confkey) as "confkey" , 
		 conrelid, 
		 confrelid, 
		 contype
     FROM
        pg_constraint
    ) pc
    JOIN pg_namespace pn ON pc.connamespace = pn.oid
    JOIN pg_class pclsc ON pc.conrelid = pclsc.oid
    JOIN pg_class pclsp ON      pc.confrelid = pclsp.oid
    JOIN pg_attribute pac ON pc.conkey = pac.attnum    and pac.attrelid =       pclsc.oid
    JOIN pg_attribute pap ON pc.confkey = pap.attnum and pap.attrelid = pclsp.oid

ORDER BY pclsc.relname ;

	
-- report of indexes returning too many lines 
-- run on primary and all standbys
-- turn to partial index or drop entirely
CREATE OR REPLACE VIEW maintenance_schema.report_idx_wtoomanylines
AS
SELECT 
		pg_sui.relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch, pg_c.reltuples as nb_lignes_table,
		idx_tup_read/pg_c.reltuples/idx_scan as tx_efficacite,
		idx_tup_read/idx_scan*100 as tx_scan_read,
		idx_tup_fetch/idx_scan*100 as tx_scan_fetch
FROM pg_stat_user_indexes pg_sui, pg_class pg_c
WHERE pg_c.relname = pg_sui.relname
AND idx_scan <> 0
AND pg_c.reltuples <> 0
AND idx_tup_read/pg_c.reltuples/idx_scan  > 0.5
GROUP BY pg_sui.relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch, idx_tup_fetch/idx_scan*100, pg_c.reltuples
HAVING (idx_tup_read/idx_scan> 1)
ORDER BY idx_tup_read/idx_scan*100 DESC
	;

-- report of duplicate indexes
CREATE OR REPLACE VIEW maintenance_schema.report_duplicate_idx
AS
SELECT c.relname, 
		pg_size_pretty(SUM(pg_relation_size(idx))::BIGINT) AS SIZE,
       (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2,
       (array_agg(idx))[3] AS idx3, (array_agg(idx))[4] AS idx4
FROM (
    SELECT 
			indrelid,
			indexrelid::regclass AS idx, 
			(indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||
                                         COALESCE(indexprs::text,'')||E'\n' || COALESCE(indpred::text,'')) AS KEY
    FROM pg_index) sub
JOIN pg_class c ON sub.indrelid=c.oid
GROUP BY KEY, c.relname HAVING COUNT(*)>1
ORDER BY SUM(pg_relation_size(idx)) DESC;




--report on invalid indexes
CREATE OR REPLACE VIEW maintenance_schema.report_invalid_idx
AS
SELECT 
	  n.nspname as schemaname,
      c2.relname as tablename,	  
	  c.relname as idxname
FROM   pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid 
JOIN  pg_catalog.pg_index i ON i.indexrelid = c.oid
JOIN pg_catalog.pg_class c2 ON c2.oid=i.indrelid
WHERE  (i.indisvalid = false OR i.indisready = false) ;





-- report on locks with time
create or replace VIEW maintenance_schema.report_lock_time AS
 SELECT a.datname,
         c.relname,
         l.transactionid,
         l.mode,
         l.GRANTED,
         a.usename,
         a.query,
         a.query_start,
         age(now(), a.query_start) AS "age",
         a.pid
    FROM  pg_stat_activity a
     JOIN pg_locks         l ON l.pid = a.pid
     JOIN pg_class         c ON c.oid = l.relation
	WHERE 
	 a.usename != current_user
    ORDER BY a.query_start;


-- report on locks with locking and locked transactions
create or replace VIEW maintenance_schema.report_blockin_n_blockd_locks AS
SELECT blocked_locks.pid     AS blocked_pid,
         blocked_activity.usename  AS blocked_user,
         blocking_locks.pid     AS blocking_pid,
         blocking_activity.usename AS blocking_user,
         blocked_activity.query    AS blocked_statement,
         blocking_activity.query   AS current_statement_in_blocking_process
   FROM  pg_catalog.pg_locks         blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks         blocking_locks 
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
 
    JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.GRANTED;

   
-- report of activity summary 
-- from wiki
CREATE OR REPLACE VIEW maintenance_schema.report_activity_summary AS 
-- Statements distribution of tuples. So this is not transactions but number of tuples (rows)
SELECT
 d.datname::text,
  tup_returned as selected, tup_inserted as inserted, tup_updated as updated, tup_deleted as deleted,
 tup_returned + tup_inserted + tup_updated + tup_deleted as total_transactions,
 case 
	when (tup_returned + tup_inserted + tup_updated + tup_deleted) > 0
      then round(1000000.0 * tup_returned / (tup_returned + tup_inserted + tup_updated + tup_deleted)) / 10000
    else 0
 end::numeric(1000, 4) as select_pct,

 case 
    when (tup_returned + tup_inserted + tup_updated + tup_deleted) > 0
      then round(1000000.0 * tup_inserted / (tup_returned + tup_inserted + tup_updated + tup_deleted)) / 10000
    else 0
 end::numeric(1000, 4) as insert_pct ,

 case 
    when (tup_returned + tup_inserted + tup_updated + tup_deleted) > 0
      then round(1000000.0 * tup_updated / (tup_returned + tup_inserted + tup_updated + tup_deleted)) / 10000
    else 0
 end::numeric(1000, 4) as update_pct,

 case 
    when (tup_returned + tup_inserted + tup_updated + tup_deleted) > 0
      then round(1000000.0 * tup_deleted / (tup_returned + tup_inserted + tup_updated + tup_deleted)) / 10000
    else 0
 end::numeric(1000, 4) as delete_pct
from
  pg_stat_database d
right join
  pg_database on d.datname=pg_database.datname
where
not datistemplate and d.datname != 'postgres';

-- report of expected candidate for autovacuum 
-- corrected for 9.4+
CREATE OR REPLACE VIEW maintenance_schema.report_autovacuum_candidates
AS
 SELECT psut.relname,
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
                * pg_class.reltuples) < psut.n_dead_tup
         THEN '*'
		 WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_mod_since_analyze
         THEN '*'
         ELSE ''
     END AS expect_av
 FROM pg_stat_user_tables psut
     JOIN pg_class on psut.relid = pg_class.oid
 WHERE pg_class.reltuples > 10000
   AND (psut.n_dead_tup >0 OR psut.n_mod_since_analyze>0)
 ORDER BY n_tup DESC, dead_tup DESC ;
 

-- report on table(s) missing pk
CREATE OR REPLACE VIEW maintenance_schema.report_missing_pk AS
select 
 tbl.table_schema, 
 tbl.table_name
from information_schema.tables tbl
where table_type = 'BASE TABLE'
  and table_schema not in ('pg_catalog', 'information_schema')
  and not exists (select 1 
  from information_schema.key_column_usage kcu
  where kcu.table_name = tbl.table_name 
  and kcu.table_schema = tbl.table_schema)
;

-- Tables with neither pk nor any index 
CREATE OR REPLACE VIEW maintenance_schema.report_missing_idx AS
 SELECT relid, schemaname, relname, n_live_tup
from pg_stat_user_tables
where relname NOT IN (select relname from pg_stat_user_indexes )
AND schemaname NOT IN ('information_schema','pg_catalog')
;




-- report unused columns 
CREATE OR REPLACE VIEW maintenance_schema.report_unused_columns AS 
SELECT nspname, relname, attname, typname,
    (stanullfrac*100)::INT AS null_percent,
    CASE WHEN stadistinct >= 0 THEN stadistinct ELSE abs(stadistinct)*reltuples END AS "distinct",
    CASE 1 WHEN stakind1 THEN array_to_string(stavalues1, ',', '*') WHEN stakind2 THEN array_to_string(stavalues2, ',', '*') END AS "values"
FROM pg_class c
JOIN pg_namespace ns ON (ns.oid=relnamespace)
JOIN pg_attribute ON (c.oid=attrelid)
JOIN pg_type t ON (t.oid=atttypid)
JOIN pg_statistic ON (c.oid=starelid AND staattnum=attnum)
WHERE nspname NOT LIKE E'pg\\_%' AND nspname != 'information_schema'
  AND relkind='r' AND NOT attisdropped AND attstattarget != 0
  AND reltuples >= 100              -- ignore tables with fewer than 100 rows
  AND stadistinct BETWEEN 0 AND 1   -- 0 to 1 distinct values
ORDER BY nspname, relname, attname
;



-- report on filling of sequences
-- use select * from seqrep \gexec to execute !
CREATE OR REPLACE VIEW maintenance_schema.report_seqrep AS 
select FORMAT('select sequence_name, last_value, increment_by, max_value,  (increment_by::float*last_value::float/max_value::float)::numeric as pct from %I', sequence_name) 
from  information_schema.sequences  ;

-- redundant indexes
CREATE OR REPLACE VIEW maintenance_schema.report_redundant_idx
AS
WITH indexes AS (
  SELECT
    tnsp.nspname AS schema_name,
    trel.relname AS table_name,
    irel.relname AS index_name,
	i.indisprimary AS indisprimary,
	i.indisreplident AS indisreplident,
    string_agg(a.attname, ', ' ORDER BY c.ordinality) AS columns
  FROM pg_index AS i
  JOIN pg_class AS trel ON trel.oid = i.indrelid
  JOIN pg_namespace AS tnsp ON trel.relnamespace = tnsp.oid
  JOIN pg_class AS irel ON irel.oid = i.indexrelid
  JOIN pg_attribute AS a ON trel.oid = a.attrelid
  JOIN LATERAL unnest(i.indkey) 
    WITH ORDINALITY AS c(colnum, ordinality)
      ON a.attnum = c.colnum
  GROUP BY i.indisprimary, i.indisreplident , tnsp.nspname, trel.relname, irel.relname
)
SELECT
  i.table_name,
  i.index_name AS "Deletion candidate index",
  i.columns AS "Deletion candidate columns",
  j.index_name AS "Existing index: KEEP IT!",
  j.columns AS "Existing columns"
FROM indexes i
JOIN indexes j
  ON i.schema_name = j.schema_name
  AND i.table_name = j.table_name
  AND j.columns LIKE i.columns || ',%'
WHERE NOT i.indisprimary
AND NOT i.indisreplident  ;

---------------------------
-- STATEMENTS FOR DBA
---------------------------

-- Suggestion last analyze et vacuum 
CREATE OR REPLACE VIEW maintenance_schema.dba_suggestion_analyze_vacuum AS
select 
       sat.schemaname,
	   c.relname,
	   c.relowner,
	   c.relpages,            
    CASE 
       WHEN sat.last_vacuum IS NULL THEN FORMAT('VACUUM VERBOSE %I.%I',sat.schemaname, c.relname) 
	   ELSE 'No VACUUM to perform' 
	   END as sql_statement_vacuum, 
	CASE
       WHEN sat.last_analyze IS NULL THEN FORMAT('ANALYZE VERBOSE  %I.%I',sat.schemaname, c.relname) 
	   ELSE 'No ANALYZE to perform' 
	   END as sql_statement_analyze
from pg_stat_user_tables sat
JOIN pg_class c ON c.relname= sat.relname 
WHERE c.reltuples>=100  -- minimum 100 tuples 
AND ((sat.last_vacuum < CURRENT_TIMESTAMP - INTERVAL '7 days' OR sat.last_analyze < CURRENT_TIMESTAMP - INTERVAL '7 days') OR (sat.last_vacuum IS NULL OR sat.last_analyze IS NULL ) )
ORDER BY last_vacuum DESC NULLS FIRST,last_analyze DESC;




-- VACUUM & REINDEX statements
-- from check_postgres
CREATE OR REPLACE VIEW maintenance_schema.dba_vacuum_reidx_bloat AS
SELECT 
	current_database, 
	schemaname, 
	tablename, 
	tbloat, 
	CASE WHEN wastedbytes/1024/1024 >100 OR tbloat >1 
		THEN FORMAT('VACUUM VERBOSE %I.%I ', schemaname,tablename) 
		ELSE 'No VACUUM to perform' 
		END AS sql_statement_vacuum, 
	iname, 
	ibloat, 
	CASE WHEN wastedibytes/1024/1024 >100 OR ibloat> 1 
		THEN FORMAT('REINDEX INDEX %I.%I ', schemaname,iname) 
		ELSE 'No REINDEX to perform' 
		END AS sql_statement_reindex
FROM (

SELECT
  current_database(), schemaname, tablename, 
  ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,
  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,
  iname, 
  ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat,
  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes
FROM (
  SELECT
    schemaname, tablename, cc.reltuples, cc.relpages, bs,
    CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,
    COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta -- very rough approximation, assumes all cols
  FROM (
    SELECT
      ma,bs,schemaname,tablename,
      (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,
      (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        schemaname, tablename, hdr, ma, bs,
        SUM((1-null_frac)*avg_width) AS datawidth,
        MAX(null_frac) AS maxfracsum,
        hdr+(
          SELECT 1+COUNT(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
        ) AS nullhdr
      FROM pg_stats s, (
        SELECT
          (SELECT current_setting('block_size')::NUMERIC) AS bs,
          CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  JOIN pg_class cc ON cc.relname = rs.tablename
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml
ORDER BY wastedbytes DESC
) AS t1 
WHERE (tbloat >1.0 OR ibloat >1.0 )
AND (wastedbytes/1024/1024> 100 OR wastedibytes/1024/1024> 100)
;
		
-- drop unused index statements		
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_unused_idx
AS

 SELECT pg_sui.relname, 
        indexrelname, 
		tnsp.nspname as schema_name,
        FORMAT('DROP INDEX CONCURRENTLY IF EXISTS %I.%I',tnsp.nspname, indexrelname) as sql_statement 
	FROM pg_stat_user_indexes pg_sui
	JOIN pg_class pg_c ON pg_sui.relname = pg_c.relname
	JOIN pg_namespace AS tnsp ON pg_c.relnamespace = tnsp.oid
	JOIN pg_stat_user_tables pg_sut ON pg_c.relname = pg_sut.relname
	JOIN pg_indexes pg_is ON pg_sui.indexrelname=pg_is.indexname
	WHERE (pg_sui.idx_scan = 0 OR (pg_sut.idx_scan < pg_sut.seq_scan AND  pg_sui.idx_scan < pg_sut.seq_scan) )
	AND pg_sut.seq_scan <> 0     
	AND pg_c.reltuples > 500
	AND pg_is.indexdef  !~* 'unique' 
	AND seq_scan <> (date_part('day', now()) - date_part('day', GREATEST(last_analyze,last_autoanalyze )))+1
	ORDER BY relname
	;

--single statement index suggestion
CREATE OR REPLACE VIEW maintenance_schema.dba_idx_suggestions
AS	
		SELECT pg_sut.relname as nom_table, 
			   FORMAT('CREATE INDEX CONCURRENTLY IF NOT EXISTS %I_%I_idx ON %I.%I (%I)',pg_sut.relname,  pg_a.attname,schemaname, pg_sut.relname,  pg_a.attname) as sql_statement 

        FROM pg_stat_user_tables pg_sut 
		JOIN pg_class pg_c ON pg_sut.relname = pg_c.relname
		JOIN pg_attribute pg_a ON pg_c.oid=pg_a.attrelid
        WHERE ( seq_tup_read > 3*pg_c.reltuples
        OR idx_scan < 100*seq_scan)
        AND pg_c.reltuples >500 --minimum 500 tuples 
        AND seq_tup_read <> 0
        AND seq_tup_read IS NOT NULL
        AND ((COALESCE(float4(seq_tup_read),0)*1.0/(NULLIF(pg_c.reltuples,0)*1.0)) >0.01
        OR  COALESCE(ROUND((seq_tup_read*1.0)/(NULLIF(idx_tup_fetch,0)*1.0),2),0)> 0.01)
		AND pg_a.attname NOT LIKE '%dropped%'
		AND pg_a.attnum >0
        ORDER BY seq_scan DESC
        ;



	



-- DROP duplicate indexes
-- TO DO : filter using fk
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_duplicate_idx 
AS
SELECT  
		pg_size_pretty(SUM(pg_relation_size(idx))::BIGINT) AS SIZE,
       CASE 
			WHEN (array_agg(indisprimary))[1] IS NULL THEN 'NULL'
			WHEN (array_agg(indisprimary))[1] THEN FORMAT('%I IS A PK! ',  (array_agg(idx))[1])
			WHEN (array_agg(indisreplident))[1] THEN 'Index used for replication' 
			ELSE FORMAT('DROP INDEX CONCURRENTLY IF EXISTS %I.%I',(array_agg(schema_name))[1],(array_agg(idx))[1]) 
			END as sql_statement_idx1,
	   CASE 
	        WHEN (array_agg(indisprimary))[2] IS NULL THEN 'NULL'
			WHEN (array_agg(indisprimary))[2]  THEN FORMAT('%I IS A PK! ',  (array_agg(idx))[2]) 
			WHEN (array_agg(indisreplident))[2] THEN 'Index used for replication' 
			ELSE FORMAT('DROP INDEX CONCURRENTLY IF EXISTS %I.%I',(array_agg(schema_name))[2],(array_agg(idx))[2]) 
			END as sql_statement_idx2,
       CASE 
	        WHEN (array_agg(indisprimary))[3] IS NULL THEN 'NULL'
			WHEN (array_agg(indisprimary))[3] THEN  FORMAT('%I IS A PK! ',  (array_agg(idx))[3]) 
			WHEN (array_agg(indisreplident))[3] THEN 'Index used for replication' 
			ELSE FORMAT('DROP INDEX CONCURRENTLY IF EXISTS %I.%I',(array_agg(schema_name))[3],(array_agg(idx))[3]) 
			END as sql_statement_idx3, 
	   CASE 
			WHEN (array_agg(indisprimary))[4] IS NULL THEN 'NULL'
			WHEN (array_agg(indisprimary))[4] THEN FORMAT('%I IS A PK! ',  (array_agg(idx))[4]) 
			WHEN (array_agg(indisreplident))[4] THEN 'Index used for replication' 
			ELSE FORMAT('DROP INDEX CONCURRENTLY IF EXISTS %I.%I',(array_agg(schema_name))[4],(array_agg(idx))[4]) 
			END as sql_statement_idx4
FROM (
    SELECT pg_sui.schemaname as schema_name, pg_sui.indexrelname,i.indexrelid,indisprimary, indisreplident, i.indexrelid::regclass AS idx, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||
                                         COALESCE(indexprs::text,'')||E'\n' || COALESCE(indpred::text,'')) AS KEY
    FROM pg_index i
    JOIN pg_stat_user_indexes pg_sui ON i.indexrelid=pg_sui.indexrelid
	) sub
GROUP BY  KEY HAVING COUNT(*)>1
ORDER BY SUM(pg_relation_size(idx)) DESC;

--ANALYZE/REINDEX of invalid/notready indexes
CREATE OR REPLACE VIEW maintenance_schema.dba_reindex_invalid_idx 
AS
SELECT 
	  n.nspname as schemaname,
      c2.relname as tablename,	  
	  c.relname as idxname,
	  FORMAT('ANALYZE VERBOSE %I.%I ', n.nspname,c2.relname) as analyze_statement,
	  FORMAT('REINDEX INDEX %I.%I ', n.nspname,c.relname) as reindex_statement
FROM   pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid 
JOIN  pg_catalog.pg_index i ON i.indexrelid = c.oid
JOIN pg_catalog.pg_class c2 ON c2.oid=i.indrelid
WHERE  (i.indisvalid = false OR i.indisready = false) ;



-- create indexes for all columns on tables without any index
-- USE CAREFULLY !
CREATE OR REPLACE VIEW maintenance_schema.dba_create_all_missing_idx AS
 SELECT relid, schemaname, pgsut.relname, n_live_tup,
 FORMAT('CREATE INDEX CONCURRENTLY IF NOT EXISTS %I_%I_idx ON %I.%I (%I)',pgsut.relname,   attname,schemaname, pgsut.relname,  attname)
from pg_stat_user_tables pgsut
JOIN pg_class c ON c.oid = pgsut.relid
JOIN pg_attribute a ON (c.oid=a.attrelid) AND atttypid!=28 AND atttypid!=29 AND atttypid!=27
where pgsut.relname NOT IN (select relname from pg_stat_user_indexes )
AND schemaname NOT IN ('information_schema','pg_catalog')
AND pgsut.n_live_tup> 500 -- minimum number of tuples to make sense
;



-- drop unused columns 
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_unused_columns AS 
SELECT nspname, relname, attname, typname,
    (stanullfrac*100)::INT AS null_percent,
    CASE WHEN stadistinct >= 0 THEN stadistinct ELSE abs(stadistinct)*reltuples END AS "distinct",
    CASE 1 WHEN stakind1 THEN array_to_string(stavalues1, ',', '*') WHEN stakind2 THEN array_to_string(stavalues2, ',', '*') END AS "values",
	FORMAT('ALTER TABLE IF EXISTS %I.%I DROP COLUMN IF EXISTS %I ', nspname,relname,   attname ) AS SQL_statement
FROM pg_class c
JOIN pg_namespace ns ON (ns.oid=relnamespace)
JOIN pg_attribute ON (c.oid=attrelid) 
JOIN pg_type t ON (t.oid=atttypid)
JOIN pg_statistic ON (c.oid=starelid AND staattnum=attnum)
WHERE nspname NOT LIKE E'pg\\_%' AND nspname != 'information_schema'
  AND relkind='r' AND NOT attisdropped AND attstattarget != 0
  AND reltuples >= 100              -- ignore tables with fewer than 100 rows
  AND stadistinct BETWEEN 0 AND 1   -- 0 to 1 distinct values
ORDER BY nspname, relname, attname
;

-- Statements to unlock long running locks 
CREATE OR REPLACE VIEW maintenance_schema.dba_lock_time AS
 SELECT a.datname,
         c.relname,
         l.transactionid,
         l.mode,
         l.GRANTED,
         a.usename,
         a.query,
         a.query_start,
         age(now(), a.query_start) AS "age",
         a.pid, 
		 format('select pg_cancel_backend(%I) ;', a.pid) as cancel_statement,
		 format('select pg_terminate_backend(%I) ;', a.pid) as terminate_statement
    FROM  pg_stat_activity a
     JOIN pg_locks         l ON l.pid = a.pid
     JOIN pg_class         c ON c.oid = l.relation
	WHERE 
	 a.usename != current_user
    ORDER BY a.query_start;

	
-- Statements to unlock either locking or locked transaction
CREATE OR REPLACE VIEW maintenance_schema.dba_unlocking_locks AS
SELECT blocked_locks.pid     AS blocked_pid,
         blocked_activity.usename  AS blocked_user,
         blocking_locks.pid     AS blocking_pid,
         blocking_activity.usename AS blocking_user,
         blocked_activity.query    AS blocked_statement,
         blocking_activity.query   AS current_statement_in_blocking_process, 
		 format('select pg_cancel_backend(%I) ;', blocked_locks.pid) as cancel_blocked_statement,
		 format('select pg_terminate_backend(%I) ;', blocked_locks.pid) as terminate_blocked_statement,
		 format('select pg_cancel_backend(%I) ;', blocking_locks.pid) as cancel_blocking_statement,
		 format('select pg_terminate_backend(%I) ;', blocking_locks.pid) as terminate_blocking_statement
   FROM  pg_catalog.pg_locks         blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks         blocking_locks 
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
 
    JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.GRANTED;
 
-- Statements to DROP redundant indexes
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_redundant_idx 
AS 
   WITH indexes AS (
  SELECT
    tnsp.nspname AS schema_name,
    trel.relname AS table_name,
    irel.relname AS index_name,
	i.indisprimary AS indisprimary,
	i.indisreplident AS indisreplident,
    string_agg(a.attname, ', ' ORDER BY c.ordinality) AS columns
  FROM pg_index AS i
  JOIN pg_class AS trel ON trel.oid = i.indrelid
  JOIN pg_namespace AS tnsp ON trel.relnamespace = tnsp.oid
  JOIN pg_class AS irel ON irel.oid = i.indexrelid
  JOIN pg_attribute AS a ON trel.oid = a.attrelid
  JOIN LATERAL unnest(i.indkey) 
    WITH ORDINALITY AS c(colnum, ordinality)
      ON a.attnum = c.colnum
  GROUP BY i.indisprimary, i.indisreplident , tnsp.nspname, trel.relname, irel.relname
)
SELECT
  i.table_name,
  i.schema_name,
  CASE 
	WHEN i.indisreplident THEN FORMAT('%I is used for REPLICATION! ',  i.index_name)
	WHEN i.indisprimary  THEN FORMAT('%I IS A PK! ',  i.index_name)
	ELSE FORMAT('DROP INDEX CONCURRENTLY IF EXISTS %I.%I',i.schema_name,i.index_name) 
  END AS sql_statement,
  i.columns AS "Deletion candidate columns",
  j.index_name AS "Existing index : KEEP IT!",
  j.columns AS "Existing columns"
FROM indexes i
JOIN indexes j
  ON i.schema_name = j.schema_name
  AND i.table_name = j.table_name
  AND j.columns LIKE i.columns || ',%';
	
	
--------------------------------
-- DEV : PENDING
--------------------------------

-- encoding
CREATE OR REPLACE VIEW maintenance_schema.audit_encoding
AS
 SELECT setting as client_encoding_alert
 from pg_settings
 where name = 'client_encoding'
 and setting!= 'UTF8'
;

---------------------------------
--SETTINGS
---------------------------------

CREATE OR REPLACE VIEW maintenance_schema.audit_settings 
AS
select name, setting, boot_val,reset_val 
from pg_settings 
where boot_val !=reset_val or boot_val!=setting;


GRANT USAGE ON SCHEMA maintenance_schema TO current_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA maintenance_schema TO current_user ;
ALTER ROLE current_user SET search_path TO   "$user", public, maintenance_schema;
ALTER SCHEMA maintenance_schema OWNER TO current_user;
