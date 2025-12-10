\echo Use "CREATE EXTENSION maintenance_schema" to load this file. \quit

/* TODOs :
- audit check : pg_hba.conf, listen_addresses, unix socket,...
- encoding : to perfect
- filter rows on expect_av, foreign_fk (others?)
- add pgstattuple, others ?
*/


CREATE SCHEMA IF NOT EXISTS maintenance_schema AUTHORIZATION current_user;

-- UNLOGGED Tables
CREATE OR REPLACE VIEW maintenance_schema.rpt_unlogged_objects AS
SELECT 
    relname as unlogged_object, 
    CASE 
	    WHEN relkind = 'r' THEN 'table'
		WHEN relkind = 'i' THEN 'index'
		WHEN relkind = 't' THEN 'toast table'
    END relation_kind,
pg_size_pretty(relpages::bigint*8*1024) as relation_size
FROM pg_class
WHERE relpersistence = 'u';

-- VIEW last analyze et vacuum avec filtres sur toutes les tables/bases du cluster
CREATE OR REPLACE VIEW maintenance_schema.rpt_last_analyze_vacuum AS
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
-- prettier with lower limit
CREATE OR REPLACE VIEW maintenance_schema.rpt_bloat_approx AS
SELECT schemaname, tablename, tbloat, wastedbytes, iname, ibloat, wastedibytes
FROM 
(
SELECT
  schemaname, tablename, /*reltuples::bigint, relpages::bigint, otta,*/
  ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,
  pg_size_pretty(CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END) AS wastedbytes,
  iname, /*ituples::bigint, ipages::bigint, iotta,*/
  ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat,
  pg_size_pretty(CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta)::numeric END) AS wastedibytes,
  bs, relpages, otta, ipages, iotta
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
) s
WHERE (tbloat >1.0 OR ibloat >1.0 )
AND (bs*(s.relpages-otta)::BIGINT > 1000000 OR bs*(ipages-iotta)::numeric > 1000000)
;


-- report bloat estimation from ioguix
-- fillfactor and stats uptodate
-- made it prettier
-- This query is compatible with PostgreSQL 9.0 and more
CREATE OR REPLACE VIEW maintenance_schema.rpt_tbloat_approx_fine AS
SELECT 
	schemaname, 
	tblname, 
	pg_size_pretty(bs*tblpages::numeric) AS real_size,
    pg_size_pretty(((tblpages-est_tblpages)*bs)::bigint) AS extra_size,
	  CASE WHEN tblpages - est_tblpages > 0
		THEN round((100 * (tblpages - est_tblpages)/tblpages::float)::numeric,2)
		ELSE 0
	  END AS extra_ratio, 
	  fillfactor, 
	  pg_size_pretty(((tblpages-est_tblpages_ff)*bs)::numeric) AS bloat_size,
	  CASE WHEN tblpages - est_tblpages_ff > 0
		THEN round((100 * (tblpages - est_tblpages_ff)/tblpages::float)::numeric,2)
		ELSE 0
	  END AS bloat_ratio, 
	  is_na
FROM (
  SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages,
    ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff,
    tblpages, fillfactor, bs, tblid, schemaname, tblname, heappages, toastpages, is_na
  FROM (
    SELECT
      ( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
        - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
        - CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
      ) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
      toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, fillfactor, is_na
    FROM (
      SELECT
        tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
        tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
        coalesce(toast.reltuples, 0) AS toasttuples,
        coalesce(substring(
          array_to_string(tbl.reloptions, ' ')
          FROM '%fillfactor=#"___#"%' FOR '#')::smallint, 100) AS fillfactor,
        current_setting('block_size')::numeric AS bs,
        CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
        24 AS page_hdr,
        23 + CASE WHEN MAX(coalesce(null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
           AS tpl_hdr_size,
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size,
        bool_or(att.atttypid = 'pg_catalog.name'::regtype)
          OR count(att.attname) <> count(s.attname) AS is_na
      FROM pg_attribute AS att
        JOIN pg_class AS tbl ON att.attrelid = tbl.oid
        JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
        LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
          AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
        LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
      WHERE att.attnum > 0 AND NOT att.attisdropped
        AND tbl.relkind = 'r'
      GROUP BY 1,2,3,4,5,6,7,8,9,10
      ORDER BY 2,3
    ) AS s
  ) AS s2
) AS s3
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
AND ((tblpages-est_tblpages_ff)*bs <>0 OR tblpages - est_tblpages_ff > 0 )
AND ((tblpages-est_tblpages)*bs) > 1000000;



-- WARNING: executed with a non-superuser role, the query inspect only index on tables you are granted to read.
-- WARNING: rows with is_na = 't' are known to have bad statistics ("name" type is not supported).
-- This query is compatible with PostgreSQL 8.2 and after
-- from ioguix
-- just made it prettier
CREATE OR REPLACE VIEW maintenance_schema.rpt_ibloat_approx_fine AS
SELECT 
	nspname AS schemaname, 
	tblname, 
	idxname, 
	pg_size_pretty((relpages)::bigint) AS real_size,
	pg_size_pretty((relpages-est_pages)::bigint) AS extra_size,
    round((100 * (relpages-est_pages)::float / relpages)::numeric, 2) AS extra_ratio,
    fillfactor, 
	pg_size_pretty((relpages-est_pages_ff)::bigint) AS bloat_size,
    round((100 * (relpages-est_pages_ff)::float / relpages)::numeric, 2) AS bloat_ratio,
    is_na
FROM (
  SELECT coalesce(1 +
       ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0 -- ItemIdData size + computed avg size of a tuple (nulldatahdrwidth)
    ) AS est_pages,
    coalesce(1 +
       ceil(reltuples/floor((bs-pageopqdata-pagehdr)*fillfactor/(100*(4+nulldatahdrwidth)::float))), 0
    ) AS est_pages_ff,
    bs, nspname, table_oid, tblname, idxname, relpages, fillfactor, is_na
  FROM (
    SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam, table_oid, fillfactor,
      ( index_tuple_hdr_bm +
          maxalign - CASE -- Add padding to the index tuple header to align on MAXALIGN
            WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
            ELSE index_tuple_hdr_bm%maxalign
          END
        + nulldatawidth + maxalign - CASE -- Add padding to the data to align on MAXALIGN
            WHEN nulldatawidth = 0 THEN 0
            WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
            ELSE nulldatawidth::integer%maxalign
          END
      )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
      -- , index_tuple_hdr_bm, nulldatawidth -- (DEBUG INFO)
    FROM (
      SELECT
        i.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.relam, a.attrelid AS table_oid,
        current_setting('block_size')::numeric AS bs, fillfactor,
        CASE -- MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?)
          WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
          ELSE 4
        END AS maxalign,
        /* per page header, fixed size: 20 for 7.X, 24 for others */
        24 AS pagehdr,
        /* per page btree opaque data */
        16 AS pageopqdata,
        /* per tuple header: add IndexAttributeBitMapData if some cols are null-able */
        CASE WHEN max(coalesce(s.null_frac,0)) = 0
          THEN 2 -- IndexTupleData size
          ELSE 2 + (( 32 + 8 - 1 ) / 8) -- IndexTupleData size + IndexAttributeBitMapData size ( max num filed per index + 8 - 1 /8)
        END AS index_tuple_hdr_bm,
        /* data len: we remove null values save space using it fractionnal part from stats */
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth,
        max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
      FROM pg_attribute AS a
        JOIN (
          SELECT nspname, tbl.relname AS tblname, idx.relname AS idxname, idx.reltuples, idx.relpages, idx.relam,
            indrelid, indexrelid, indkey::smallint[] AS attnum,
            coalesce(substring(
              array_to_string(idx.reloptions, ' ')
               from 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor
          FROM pg_index
            JOIN pg_class idx ON idx.oid=pg_index.indexrelid
            JOIN pg_class tbl ON tbl.oid=pg_index.indrelid
            JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace
          WHERE pg_index.indisvalid AND tbl.relkind = 'r' AND idx.relpages > 0
        ) AS i ON a.attrelid = i.indexrelid
        JOIN pg_stats AS s ON s.schemaname = i.nspname
          AND ((s.tablename = i.tblname AND s.attname = pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE)) -- stats from tbl
          OR   (s.tablename = i.idxname AND s.attname = a.attname))-- stats from functionnal cols
        JOIN pg_type AS t ON a.atttypid = t.oid
      WHERE a.attnum > 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ) AS s1
  ) AS s2
    JOIN pg_am am ON s2.relam = am.oid WHERE am.amname = 'btree'
) AS sub
WHERE ((relpages-est_pages_ff) !=0 OR round((100 * (relpages-est_pages_ff)::float / relpages)::numeric, 2) !=0)
AND nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY 2,3,4;

-- report or unused indexes
CREATE OR REPLACE VIEW maintenance_schema.rpt_idx_unused
AS
SELECT 
	pg_sui.relname as tbl_name,
	indexrelname as unused_idx_name, 
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
  -- AND pg_c.reltuples > 500
  AND pg_is.indexdef  !~* 'unique' 
  AND seq_scan <> (date_part('day', now()) - date_part('day', GREATEST(last_analyze,last_autoanalyze )))+1
ORDER BY pg_sui.relname
;
		
	
-- report of most full scanned tables: missing indexes
CREATE OR REPLACE VIEW maintenance_schema.rpt_mostfullscaned
AS
SELECT pg_sut.relname as tbl_name, seq_scan, COALESCE(seq_tup_read,0) as fullscan_tuples,
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

-- simpler version 
-- By Laurenz Albe (Github) 
-- Filter bigger tables (at least 500 tuples) an index would not be used if there the table is too small anyway
CREATE OR REPLACE VIEW maintenance_schema.rpt_perf_idx_missing AS						       
SELECT schemaname, 
       relname,
       seq_scan,
       seq_tup_read / seq_scan AS tup_per_scan
FROM pg_stat_user_tables
WHERE seq_scan > 0
AND n_live_tup > 500;						       

		
-- report index usage
-- from wiki
CREATE OR REPLACE VIEW maintenance_schema.rpt_idx_usage
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
CREATE OR REPLACE VIEW maintenance_schema.rpt_idx_usagecounts 
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
CREATE OR REPLACE VIEW maintenance_schema.rpt_fk_unindexed AS
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
WHERE referencing_tbl_bytes > 10000000
ORDER BY
    referencing_tbl_bytes DESC,
    referenced_tbl_bytes DESC,
    referencing_tbl,
    referenced_tbl,
    referencing_column,
    referenced_column;

-- Duplicate foreign keys
CREATE OR REPLACE VIEW maintenance_schema.rpt_fk_duplicate AS
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
ORDER BY child_table, child_column;

-- Useless unique constraints on FK or PK
CREATE OR REPLACE VIEW maintenance_schema.rpt_useless_uconstraint AS
SELECT 
       pgc1.conrelid, pgcl.relname, pgc1.conname, pgc1.contype 
  FROM pg_constraint pgc1 
FULL JOIN 
         pg_constraint pgc2 
      ON pgc1.conrelid = pgc2.conrelid 
JOIN 
         pg_class pgcl 
      ON pgcl.oid=pgc1.conrelid 

WHERE pgc1.conkey = pgc2.conkey 
  AND pgc1.contype ='u'  

GROUP BY pgc1.conrelid, pgcl.relname, pgc1.conname, pgc1.contype  
HAVING count(pgc1.conname) >1;

	
-- report of indexes returning too many lines 
-- run on primary and all standbys
-- turn to partial index or drop entirely
CREATE OR REPLACE VIEW maintenance_schema.rpt_idx_wtoomanylines
AS
SELECT 
		pg_sui.relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch, pg_c.reltuples as n_tbl_rows,
		idx_tup_read/pg_c.reltuples/idx_scan as efficiency_rate,
		idx_tup_read/idx_scan*100 as pct_scan_read,
		idx_tup_fetch/idx_scan*100 as pct_scan_fetch
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
CREATE OR REPLACE VIEW maintenance_schema.rpt_idx_duplicate
AS
SELECT c.relname as tbl_w_dup_idx, 
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
CREATE OR REPLACE VIEW maintenance_schema.rpt_idx_invalid
AS
SELECT 
	  n.nspname as schemaname,
      c2.relname as tablename,	  
	  c.relname as invalid_idxname
FROM   pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid 
JOIN  pg_catalog.pg_index i ON i.indexrelid = c.oid
JOIN pg_catalog.pg_class c2 ON c2.oid=i.indrelid
WHERE  (i.indisvalid = false OR i.indisready = false) ;





-- report on locks with time
create or replace VIEW maintenance_schema.rpt_lock_time AS
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


-- locks  with pg_blocking_pids v9.6+ locked and locking sessions
CREATE OR REPLACE VIEW maintenance_schema.rpt_locks_blocking_pids AS
 SELECT 
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


-- report of long queries >100s
CREATE OR REPLACE VIEW maintenance_schema.rpt_long_queries AS 
 SELECT 
		datid, 
		pid, 
		datname, 
		usename, 
		application_name, 
		client_addr, 
		query_start - now() as duration_query,
		xact_start - now() as duration_xact
 FROM pg_stat_activity 
 WHERE EXTRACT (seconds FROM  query_start - now()) > 100
 AND EXTRACT (seconds FROM  xact_start - now()) > 100;

 
-- report of activity summary 
-- from wiki
CREATE OR REPLACE VIEW maintenance_schema.rpt_activity_summary AS 
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

--report summary of activity on tables 
CREATE OR REPLACE VIEW maintenance_schema.rpt_activity_summary_tables AS
SELECT
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
					    
					    
-- report of expected candidate for autovacuum 
-- corrected for 9.4+
CREATE OR REPLACE VIEW maintenance_schema.rpt_autovacuum_candidates
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
		 WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_dead_tup*1.5
		 THEN 'Consider Tuning AV !'
		 WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_mod_since_analyze*1.5
         THEN 'Consider Tuning AV !'
         ELSE ''
     END AS expect_av
 FROM pg_stat_user_tables psut
     JOIN pg_class on psut.relid = pg_class.oid
 WHERE pg_class.reltuples > 10000 
   AND (psut.n_dead_tup >1000 OR psut.n_mod_since_analyze>1000)
 ORDER BY n_tup DESC, dead_tup DESC ;
 

-- report on table(s) missing pk
CREATE OR REPLACE VIEW maintenance_schema.rpt_pk_missing AS
select 
 tbl.table_schema, 
 tbl.table_name as tblname_pk_missing
from information_schema.tables tbl
where table_type = 'BASE TABLE'
  and table_schema not in ('pg_catalog', 'information_schema')
  and not exists (select 1 
  from information_schema.key_column_usage kcu
  where kcu.table_name = tbl.table_name 
  and kcu.table_schema = tbl.table_schema)
;

-- Tables with neither pk nor any index 
CREATE OR REPLACE VIEW maintenance_schema.rpt_idx_missing AS
 SELECT relid, schemaname, relname as tbl_wo_idx, n_live_tup
from pg_stat_user_tables
where relname NOT IN (select relname from pg_stat_user_indexes )
AND schemaname NOT IN ('information_schema','pg_catalog')
;




-- report useless columns 
-- Columns that have no more than 1 value in said column
-- needs pg_monitor ROLE					 
CREATE OR REPLACE VIEW maintenance_schema.rpt_columns_unused AS 
SELECT nspname as schema_name, relname as table_name, attname as useless_column_name, typname as data_type,
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

-- null columns (stats only) make sure all rows are null and then should be dropped
-- CREATE OR REPLACE VIEW maintenance_schema.rpt_columns_null AS
select schemaname, tablename, attname as null_column, most_common_vals as column_value, null_frac
from pg_stats
where null_frac =1 
and schemaname NOT IN ('pg_catalog','information_schema');


-- columns with the same value in all rows (stats only) make sure all rows are null and then should be dropped 
-- same as rpt_columns_unused without pg_statistics
-- CREATE OR REPLACE VIEW maintenance_schema.rpt_columns_useless AS
select schemaname, tablename, attname as unique_column, most_common_vals as column_value
from pg_stats
where n_distinct =1
and schemaname NOT IN ('pg_catalog','information_schema');


-- report on filling of sequences
-- use select * from seqrep \gexec to execute !
CREATE OR REPLACE VIEW maintenance_schema.rpt_seqrep AS 
select FORMAT('select sequence_name, last_value, increment_by, max_value,  CASE WHEN round((increment_by::float*last_value::float/max_value::float)::numeric,2)>50 THEN round((increment_by::float*last_value::float/max_value::float)::numeric,2) ELSE 0 END as pct from %I.%I ', sequence_schema,sequence_name) 
from  information_schema.sequences  ;


-- redundant indexes
CREATE OR REPLACE VIEW maintenance_schema.rpt_idx_redundant
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

-- pending wrap-around 
-- from gsmith
CREATE OR REPLACE VIEW maintenance_schema.rpt_fxid_wraparound_approaching AS
SELECT
  nspname as schema_name,
  CASE WHEN relkind='t' THEN toastname ELSE relname END AS relation_fxid_approach,
  CASE WHEN relkind='t' THEN 'Toast' ELSE 'Table' END AS kind,
  pg_size_pretty(pg_relation_size(oid)) as table_sz,
  pg_size_pretty(pg_total_relation_size(oid)) as total_sz,
  age(relfrozenxid),
  last_vacuum
FROM
(SELECT
  c.oid,
  c.relkind,
  N.nspname,
  C.relname,
  T.relname AS toastname,
  C.relfrozenxid,
  date_trunc('day',greatest(pg_stat_get_last_vacuum_time(C.oid),pg_stat_get_last_autovacuum_time(C.oid)))::date AS last_vacuum,
  setting::integer as freeze_max_age
 FROM pg_class C
  LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
  LEFT OUTER JOIN pg_class T ON (C.oid=T.reltoastrelid),
  pg_settings
  WHERE C.relkind IN ('r', 't')
-- We want toast items to appear in the wraparound list
    AND N.nspname NOT IN ('pg_catalog', 'information_schema') AND
    name='autovacuum_freeze_max_age'
    AND pg_relation_size(c.oid)>0
) AS av
WHERE age(relfrozenxid) > (0.85 * freeze_max_age)
ORDER BY age(relfrozenxid) DESC, pg_total_relation_size(oid) DESC
;

--autovaccum freeze_max_age
CREATE OR REPLACE VIEW maintenance_schema.rpt_fxid_database_oldest AS
select datname, max(age(datfrozenxid)) as dboldest_fxid
from pg_database  
GROUP BY datname
ORDER BY max(age(datfrozenxid)) DESC;


--XID of all tables >1GB 
--ASC
CREATE OR REPLACE VIEW maintenance_schema.rpt_fxid_table AS
SELECT relname, age(relfrozenxid) as xid_age,
    pg_size_pretty(pg_table_size(oid)) as table_size
FROM pg_class
WHERE relkind = 'r' and pg_table_size(oid) > 1073741824
ORDER BY age(relfrozenxid) DESC LIMIT 20;


-- REPLICATION 
--  for v10+ only
CREATE OR REPLACE VIEW maintenance_schema.rpt_replication AS 
SELECT 
		pg_is_in_recovery() as isinrecovery,
		pg_last_wal_replay_lsn() as last_wallocation, 
		pg_last_xact_replay_timestamp() as last_xact_ts,
		pg_wal_lsn_diff(pg_stat_replication.sent_lsn, pg_stat_replication.replay_lsn) as lag_in_bytes,
		CASE 
			WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn()
            THEN 0
            ELSE EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp())
        END AS log_delay
FROM pg_stat_replication		;

-- ARCHIVING .ready files ALERT
-- for v10+ only 
-- needs pg_monitor ROLE
CREATE OR REPLACE VIEW maintenance_schema.rpt_archive_ready AS 
SELECT 
		count(*) as nbreadyfiles, 
		CASE 
		  WHEN count(*) >150 THEN 'DANGER!'
		  WHEN count(*) >50 THEN 'WARNING'
		  ELSE 'NORMAL'  
		END AS alert_level
FROM (
	SELECT pg_ls_dir('pg_wal/archive_status') AS files ) AS archstat_files
WHERE files like '%ready' ;

-- tables with a single column (might need a more detailed column if PK, if nonPK relation why bother?)
CREATE OR REPLACE VIEW maintenance_schema.rpt_tbl_single_column AS 
SELECT table_catalog,table_schema,table_name as table_single_column, count(column_name) 
FROM information_schema.columns 
WHERE table_schema NOT IN ('information_schema', 'maintenance_schema') 
GROUP BY table_catalog,table_schema,table_name 
HAVING COUNT (column_name)= 1 ;

-- unused tables
CREATE OR REPLACE VIEW maintenance_schema.rpt_tbl_unused
AS 
SELECT schemaname, relname as unused_table 
FROM pg_stat_user_tables
WHERE (idx_tup_fetch + seq_tup_read)= 0; -- tables where no tuple is read either from seqscan or idx


-- schema with tables of the same name (check for same structure and drop ? or rename if possible)
CREATE OR REPLACE VIEW maintenance_schema.rpt_tbl_same_name AS
SELECT
n.nspname as "Schema",
c.relname as "Table with the same Name" FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname IN (SELECT relname FROM pg_catalog.pg_class
WHERE relkind IN ('r')
GROUP BY relname
HAVING COUNT(relname) > 1)
ORDER BY n.nspname,c.relname;


-- empty tables
CREATE OR REPLACE VIEW maintenance_schema.rpt_tbl_empty AS
SELECT schemaname, relname as empty_table 
FROM pg_stat_user_tables
WHERE n_live_tup = 0;


-- replication conflicts
CREATE OR REPLACE VIEW maintenance_schema.rpt_db_replication_conflicts AS
SELECT datname as dbname_w_replica_conflicts, 
		confl_tablespace,
		confl_lock,
		confl_snapshot,
		confl_bufferpin,
		confl_deadlock 
FROM pg_stat_database_conflicts 
WHERE datname NOT LIKE 'template%' 
AND (confl_tablespace + confl_lock + confl_snapshot + confl_bufferpin + confl_deadlock)>0;


--  count for commits, rollbacks and deadlocks when deadlocks are found 
CREATE OR REPLACE VIEW maintenance_schema.rpt_db_deadlocks_count AS
SELECT 
      datname as dbname_w_deadlocks, 
      numbackends, 
      xact_commit, 
      xact_rollback, 
deadlocks 
FROM pg_stat_database 
WHERE datname NOT LIKE 'template%' 
AND deadlocks>0 ;



-- alert on temp files with current work_mem setting 
CREATE OR REPLACE VIEW maintenance_schema.rpt_db_tmpfsize AS
SELECT datname as dbname_w_tmpfiles, 
temp_files as nb_tmp_files, 
pg_size_pretty(temp_bytes) as tmp_fsize, 
pg_size_pretty(temp_bytes/temp_files) as avg_tmpfsize, 
( select setting::numeric || unit  from pg_settings where name = 'work_mem') as current_work_mem 
FROM pg_stat_database 
WHERE temp_files>0;

--[EXPERIMENTAL] Alignment Padding. How many bytes can be saved if columns are ordered better?

-- TODO: not-yet-analyzed tables – show a warning (cannot get n_live_tup -> cannot get total bytes)
-- TODO: NULLs
-- TODO: simplify, cleanup
-- TODO: chunk_size 4 or 8
CREATE OR REPLACE VIEW maintenance_schema.rpt_alignment_padding AS
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
    '{}'::name[] as padded_columns,
    '{}'::int[] as pads,
    (select max(ordinal_position) from columns c where c.table_name = _.table_name and c.table_schema = _.table_schema) as col_cnt,
    array_agg(_.column_name::text order by ordinal_position) as cols,
    array_agg(_.udt_name::text order by ordinal_position) as types,
    array_agg(shift order by ordinal_position) as shifts,
    null::int as curleft,
    null::name as prev_column_name,
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
	      
---------------------------
-- STATEMENTS FOR DBA
---------------------------

-- Suggestion last analyze et vacuum 
CREATE OR REPLACE VIEW maintenance_schema.dba_vacuum_analyze_suggestion AS
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
	schemaname, 
	tablename, 
	tbloat, 
	CASE WHEN (bs*(s.relpages-otta)::BIGINT > 1000000 OR tbloat >1 )
		THEN FORMAT('VACUUM VERBOSE %I.%I ', schemaname,tablename) 
		ELSE 'No VACUUM to perform' 
		END AS sql_statement_vacuum, 
	iname, 
	ibloat, 
	CASE WHEN (bs*(ipages-iotta)::numeric > 1000000 OR ibloat> 1 )
		THEN FORMAT('REINDEX INDEX %I.%I ', schemaname,iname) 
		ELSE 'No REINDEX to perform' 
		END AS sql_statement_reindex
FROM 
(
SELECT
  schemaname, tablename, /*reltuples::bigint, relpages::bigint, otta,*/
  ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,
  pg_size_pretty(CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END) AS wastedbytes,
  iname, /*ituples::bigint, ipages::bigint, iotta,*/
  ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat,
  pg_size_pretty(CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta)::numeric END) AS wastedibytes,
  bs, relpages, otta, ipages, iotta
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
) s
WHERE (tbloat >1.0 OR ibloat >1.0 )
AND (bs*(s.relpages-otta)::BIGINT > 1000000 OR bs*(ipages-iotta)::numeric > 1000000)
;

-- report of expected candidate for autovacuum 
-- corrected for 9.4+
CREATE OR REPLACE VIEW maintenance_schema.dba_tuning_autovacuum
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
                * pg_class.reltuples) < psut.n_dead_tup*1.5
		 THEN FORMAT('ALTER TABLE %I SET (autovacuum_vacuum_scale_factor = 0.1,autovacuum_analyze_scale_factor = 0.05 ) ;', psut.relname)
		 WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_mod_since_analyze*1.5
		 THEN FORMAT('ALTER TABLE %I SET (autovacuum_vacuum_scale_factor = 0.1,autovacuum_analyze_scale_factor = 0.05 ) ;',psut.relname)
         ELSE 'Nothing to do'
     END AS sql_statement_std,
	 CASE 
	 	 WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_dead_tup*1.5
		 THEN FORMAT('ALTER TABLE %I SET (autovacuum_vacuum_cost_limit = 1000, autovacuum_vacuum_cost_delay = 10); ',  psut.relname)
		 WHEN CAST(current_setting('autovacuum_vacuum_threshold') AS bigint)
             + (CAST(current_setting('autovacuum_vacuum_scale_factor') AS numeric)
                * pg_class.reltuples) < psut.n_mod_since_analyze*1.5
		 THEN FORMAT('ALTER TABLE %I SET (autovacuum_vacuum_cost_limit = 1000,autovacuum_vacuum_cost_delay = 10); ',  psut.relname)
         ELSE 'Nothing to do'
     END AS sql_statement_ssd_or_multicores
 FROM pg_stat_user_tables psut
     JOIN pg_class on psut.relid = pg_class.oid
 WHERE pg_class.reltuples > 10000 
   AND (psut.n_dead_tup >1000 OR psut.n_mod_since_analyze>1000)
 ORDER BY n_tup DESC, dead_tup DESC ;
 
	
-- drop unused index statements		
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_idx_unused
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
	-- AND pg_c.reltuples > 500
	AND pg_is.indexdef  !~* 'unique' 
	AND seq_scan <> (date_part('day', now()) - date_part('day', GREATEST(last_analyze,last_autoanalyze )))+1
	ORDER BY relname
	;

--single statement index suggestion
CREATE OR REPLACE VIEW maintenance_schema.dba_create_idx_suggestions
AS	
		SELECT pg_sut.relname as tbl_name, 
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
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_idx_duplicate 
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
CREATE OR REPLACE VIEW maintenance_schema.dba_reindex_idx_invalid 
AS
SELECT 
	  n.nspname as schemaname,
      c2.relname as tablename,	  
	  c.relname as invalid_idxname,
	  FORMAT('ANALYZE VERBOSE %I.%I ', n.nspname,c2.relname) as analyze_statement,
	  FORMAT('REINDEX INDEX %I.%I ', n.nspname,c.relname) as reindex_statement
FROM   pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid 
JOIN  pg_catalog.pg_index i ON i.indexrelid = c.oid
JOIN pg_catalog.pg_class c2 ON c2.oid=i.indrelid
WHERE  (i.indisvalid = false OR i.indisready = false) ;



-- create indexes for all columns on tables without any index
-- USE CAREFULLY !
CREATE OR REPLACE VIEW maintenance_schema.dba_create_idx_all_missing AS
 SELECT relid, schemaname, pgsut.relname as tbl_wo_idx, n_live_tup,
 FORMAT('CREATE INDEX CONCURRENTLY IF NOT EXISTS %I_%I_idx ON %I.%I (%I)',pgsut.relname,   attname,schemaname, pgsut.relname,  attname)
from pg_stat_user_tables pgsut
JOIN pg_class c ON c.oid = pgsut.relid
JOIN pg_attribute a ON (c.oid=a.attrelid) AND atttypid!=28 AND atttypid!=29 AND atttypid!=27
where pgsut.relname NOT IN (select relname from pg_stat_user_indexes )
AND schemaname NOT IN ('information_schema','pg_catalog')
AND pgsut.n_live_tup> 500 -- minimum number of tuples to make sense
;



-- Useless unique constraints on FK or PK
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_useless_uconst AS
SELECT 
       pgc1.conrelid, pgcl.relname, pgc1.conname, pgc1.contype,
       FORMAT('ALTER TABLE IF EXISTS %I.%I DROP CONSTRAINT IF EXISTS %I ', ns.nspname,pgcl.relname, pgc1.conname ) AS SQL_statement
  FROM pg_constraint pgc1 
FULL JOIN 
         pg_constraint pgc2 
      ON pgc1.conrelid = pgc2.conrelid 
JOIN 
         pg_class pgcl 
      ON pgcl.oid=pgc1.conrelid 

JOIN pg_namespace ns ON (ns.oid=pgc1.connamespace)
WHERE pgc1.conkey = pgc2.conkey 
  AND pgc1.contype ='u'  

GROUP BY pgc1.conrelid, pgcl.relname, pgc1.conname, pgc1.contype, ns.nspname
HAVING count(pgc1.conname) >1;

-- drop useless columns 
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_columns_useless AS 
SELECT nspname as schema_name, relname as table_name, attname as useless_column_name, typname as data_type,
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
CREATE OR REPLACE VIEW maintenance_schema.dba_unlock_time AS
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



-- Statement to unlock locks  with pg_blocking_pids v9.6+ locked and locking sessions
CREATE OR REPLACE VIEW maintenance_schema.dba_locks_blocking_pids AS
 SELECT
     pgsa_blocked.usename as blocked_user,
     pgsa_blocked.datname,
     pgsa_blocked.wait_event_type,
     pgsa_blocked.wait_event,
     pgsa_blocked.pid as blocked_pid,
     pg_blocking_pids(pgsa_blocked.pid) AS blocking_pids,
                 format('select pg_cancel_backend(%I) ;', pgsa_blocked.pid ) as cancel_blocked_statement,
                 format('select pg_terminate_backend(%I) ;', pgsa_blocked.pid ) as terminate_blocked_statement,
                 format('select pg_cancel_backend(%I) ;', pg_blocking_pids(pgsa_blocked.pid)) as cancel_blocking_statement,
                 format('select pg_terminate_backend(%I) ;', pg_blocking_pids(pgsa_blocked.pid)) as terminate_blocking_statement
 FROM pg_stat_activity AS pgsa_blocked
 JOIN pg_stat_activity AS pgsa_blocking
   ON  pgsa_blocking.pid = ANY ( SELECT unnest(pg_blocking_pids(pgsa_blocked.pid))) ;


-- CANCEL/TERMINATE long queries >100s
-- TODO : lock handling only on SELECTs and state only idle_in_transaction
CREATE OR REPLACE VIEW maintenance_schema.dba_stop_lqueries AS 
 SELECT 
		datid, 
		pid, 
		datname, 
		usename, 
		application_name, 
		client_addr, 
		query_start - now() as duration_query,
		xact_start - now() as duration_xact, 
		format('select pg_cancel_backend(%I) ;', pid) as cancel_statement, 
		format('select pg_terminate_backend(%I) ;', pid) as terminate_statement
 FROM pg_stat_activity 
 WHERE EXTRACT (seconds FROM  query_start - now()) > 100
 AND EXTRACT (seconds FROM  xact_start - now()) > 100; 
   
-- Statements to DROP redundant indexes
CREATE OR REPLACE VIEW maintenance_schema.dba_drop_idx_redundant 
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
-- AUDIT : in the works...
--------------------------------

-- encoding
CREATE OR REPLACE VIEW maintenance_schema.audit_encoding_setting
AS
 SELECT name, setting as client_encoding_alert
 from pg_settings
 where name ilike '%encoding'
 and setting!= 'UTF8' 
;

-- encoding for every database
-- Non UTF8 only  
CREATE OR REPLACE VIEW maintenance_schema.audit_encoding_db
AS
 SELECT datname, pg_encoding_to_char(encoding) as encoding, datcollate, datctype 
 FROM pg_database 
 WHERE  datallowconn 
 AND NOT datistemplate
 AND pg_encoding_to_char(encoding) != 'UTF8';

---------------------------------
-- SETTINGS: WHAT STICKS OUT
---------------------------------

CREATE OR REPLACE VIEW maintenance_schema.audit_settings 
AS
select name, setting, boot_val,reset_val 
from pg_settings 
where boot_val !=reset_val or boot_val!=setting;

CREATE OR REPLACE VIEW maintenance_schema.audit_options
AS
select relname, reloptions 
from pg_class 
where reloptions is not null ;


---------------------------------------
-- FULL REPORT 
---------------------------------------
-- Create full report in and out the cluster
CREATE OR REPLACE VIEW maintenance_schema.full_report AS
SELECT 
		FORMAT('SELECT * FROM %I.%I  ', schemaname, viewname ) AS SQL_statement
FROM (SELECT 
			schemaname,
			viewname 
		FROM pg_views 
		WHERE schemaname='maintenance_schema' 
		AND viewname NOT LIKE 'dba%'
		AND viewname NOT LIKE 'full%' )AS viewlist
ORDER BY sql_statement;



GRANT USAGE ON SCHEMA maintenance_schema TO current_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA maintenance_schema TO current_user ;
ALTER ROLE current_user SET search_path TO   "$user", public, maintenance_schema;
ALTER SCHEMA maintenance_schema OWNER TO current_user;

-- Part 2: Executive Summary

-- 1. The Executive Dashboard (Red Flags)
CREATE OR REPLACE VIEW maintenance_schema.rpt_executive_summary AS
-- 1. Check for Bloated/Overdue Vacuum Tables
SELECT 
    'HIGH' AS severity, 'Maintenance' AS category,
    count(*) || ' tables are overdue for Vacuum (High Bloat Risk)' AS issue,
    'Check maintenance_schema.rpt_autovacuum_thresholds' AS next_step
FROM maintenance_schema.rpt_autovacuum_thresholds
WHERE vacuum_status = 'OVERDUE (Bloating)'
UNION ALL
-- 2. Check for Dangerous Replication Slots
SELECT 
    'CRITICAL', 'Replication',
    count(*) || ' inactive slots holding WAL files',
    'DROP SLOT via rpt_replication_slots_health'
FROM pg_replication_slots
WHERE active = 'f'
UNION ALL
-- 3. Check for Idle in Transaction
SELECT 
    'CRITICAL', 'Availability',
    count(*) || ' connections IDLE IN TRANSACTION > 1 min',
    'Kill PIDs via rpt_activity_connection_health'
FROM pg_stat_activity
WHERE state = 'idle in transaction' AND age(now(), state_change) > interval '1 minute'
UNION ALL
-- 4. Check for Unindexed Foreign Keys
SELECT 
    'MEDIUM', 'Performance',
    count(*) || ' unindexed Foreign Keys found',
    'Create indexes via rpt_unindexed_foreign_keys'
FROM maintenance_schema.rpt_unindexed_foreign_keys
UNION ALL
-- 5. Check for Sequence Wraparound
SELECT 
    'CRITICAL', 'Capacity',
    count(*) || ' sequences > 90% full',
    'Alter sequence max value or cycle'
FROM information_schema.sequences
WHERE (last_value::numeric / NULLIF(maximum_value::numeric, 0)) > 0.90;

-- Part 3: Database & Storage Architecture

-- 2. Database Metadata Overview
CREATE OR REPLACE VIEW maintenance_schema.rpt_database_overview AS
SELECT 
    d.datname AS database_name,
    pg_catalog.pg_get_userbyid(d.datdba) AS owner,
    pg_catalog.pg_encoding_to_char(d.encoding) AS encoding,
    d.datcollate AS collation,
    pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) AS size,
    t.spcname AS tablespace,
    d.datconnlimit AS connection_limit,
    age(d.datfrozenxid) AS xid_age
FROM pg_catalog.pg_database d
JOIN pg_catalog.pg_tablespace t ON d.dattablespace = t.oid
ORDER BY pg_catalog.pg_database_size(d.datname) DESC;

-- 3. Detailed Storage (Heap, TOAST, FSM)
CREATE OR REPLACE VIEW maintenance_schema.rpt_storage_architecture AS
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    pg_size_pretty(pg_relation_size(c.oid, 'main')) AS heap_size,
    pg_size_pretty(pg_relation_size(c.oid, 'fsm')) AS fsm_size,
    pg_size_pretty(pg_relation_size(c.oid, 'vm')) AS vm_size,
    CASE WHEN c.reltoastrelid > 0 THEN pg_size_pretty(pg_relation_size(c.reltoastrelid)) ELSE '0 bytes' END AS toast_size,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'maintenance_schema')
ORDER BY pg_total_relation_size(c.oid) DESC;

-- 4. "Bad" Data Types (Risk Audit)
CREATE OR REPLACE VIEW maintenance_schema.rpt_bad_data_types AS
SELECT
    table_schema, table_name, column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
  AND (
      data_type IN ('money', 'real', 'double precision') 
      OR (data_type = 'character' AND character_maximum_length IS NOT NULL)
      OR (data_type = 'timestamp without time zone')
  )
ORDER BY table_name, data_type;

-- 1. Fragmentation Risk (Text/Random PKs)
CREATE OR REPLACE VIEW maintenance_schema.rpt_risky_pks_fragmentation AS
SELECT 
    n.nspname AS schema_name,
    c.relname AS table_name,
    a.attname AS pk_column,
    t.typname AS data_type,
    
    -- Estimate Table Size
    pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
    
    -- Severity
    CASE 
        WHEN t.typname IN ('text', 'varchar', 'bpchar') THEN 'HIGH (Fragmentation Risk)'
        WHEN t.typname = 'uuid' THEN 'CHECK (Ensure v7/Sequential)'
        ELSE 'LOW'
    END AS risk_level,
    
    'Consider using BIGINT or UUIDv7 for clustered performance' AS recommendation

FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
JOIN pg_type t ON a.atttypid = t.oid
WHERE i.indisprimary
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'maintenance_schema')
  AND t.typname IN ('text', 'varchar', 'bpchar', 'uuid')
ORDER BY pg_relation_size(c.oid) DESC;

-- 2. The "Bad Habits" Sweep (Money, Char, JSON, Timezone)
CREATE OR REPLACE VIEW maintenance_schema.rpt_risky_datatypes_audit AS
SELECT 
    table_schema,
    table_name,
    column_name,
    data_type,
    udt_name,
    
    CASE 
        WHEN data_type = 'money' 
            THEN 'Legacy Type: Hard to migrate/calculate. Use NUMERIC.'
        WHEN data_type = 'character' -- This is char(n)
            THEN 'Padding Issue: Wastes space. Use TEXT/VARCHAR.'
        WHEN data_type = 'timestamp without time zone' 
            THEN 'Timezone Risk: ambiguous data. Use TIMESTAMPTZ.'
        WHEN data_type = 'json' 
            THEN 'Performance: Slow parsing. Use JSONB.'
        WHEN data_type = 'real' OR data_type = 'double precision' 
            THEN 'Precision Risk: Do not use for financial data. Use NUMERIC.'
    END AS risk_analysis
    
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'maintenance_schema')
  AND (
      data_type IN ('money', 'real', 'double precision', 'json')
      OR (data_type = 'character' AND character_maximum_length IS NOT NULL)
      OR (data_type = 'timestamp without time zone')
  )
ORDER BY table_schema, table_name;

-- 3. The "Integer Cliff" (Capacity Planning)
-- PG 10 compatible 
CREATE OR REPLACE VIEW maintenance_schema.rpt_integer_overflow_risk AS
SELECT 
    schemaname,
    sequencename AS sequence_name,
    last_value,
    data_type,
    max_value,
    
    -- Percentage used
    round((last_value::numeric / NULLIF(max_value::numeric, 0)) * 100, 4) AS pct_used,
           
    CASE 
        -- Hard check for the int4 limit (2.1 Billion) just in case max_value was set weirdly
        WHEN last_value > 2000000000 THEN 'CRITICAL: Immediate Upgrade Required'
        -- Percentage based checks
        WHEN (last_value::numeric / NULLIF(max_value::numeric, 0)) > 0.90 THEN 'CRITICAL: >90% Used'
        WHEN (last_value::numeric / NULLIF(max_value::numeric, 0)) > 0.75 THEN 'WARNING: >75% Used'
        ELSE 'OK'
    END AS status

FROM pg_sequences
-- Filter strictly for 4-byte integers using the regtype cast
WHERE data_type = 'integer'::regtype 
ORDER BY pct_used DESC;

-- Filling of Sequences (and ALTER STATEMENT)
CREATE OR REPLACE VIEW maintenance_schema.rpt_sequences_filling AS
SELECT 
    schemaname,
    sequencename AS sequence_name,
    data_type,
    last_value,
    max_value,
    
    -- Percentage Used
    round((last_value::numeric / NULLIF(max_value::numeric, 0)) * 100, 2) AS pct_used,
    
    -- Status
    CASE 
        WHEN (last_value::numeric / NULLIF(max_value::numeric, 0)) > 0.90 
            THEN 'CRITICAL: Exhaustion Imminent'
        WHEN (last_value::numeric / NULLIF(max_value::numeric, 0)) > 0.75 
            THEN 'WARNING: >75% Full'
        WHEN data_type = 'integer'::regtype AND last_value > 2000000000 
            THEN 'CRITICAL: Integer Edge Case'
        ELSE 'OK'
    END AS status,

    -- THE FIX (Sequence Only - Fast)
    CASE 
        -- If it's an INT sequence, upgrade it to BIGINT
        WHEN data_type = 'integer'::regtype AND (last_value::numeric / max_value::numeric) > 0.50 THEN 
            format('ALTER SEQUENCE %I.%I AS bigint MAXVALUE 9223372036854775807;', schemaname, sequencename)
            
        -- If it's already BIGINT but has a weirdly low MAXVALUE set manually
        WHEN data_type = 'bigint'::regtype AND (last_value::numeric / max_value::numeric) > 0.75 THEN 
            format('ALTER SEQUENCE %I.%I MAXVALUE 9223372036854775807;', schemaname, sequencename)
            
        ELSE 'No action needed'
    END AS sql_fix_sequence,

    -- CONSULTANT WARNING (The Column Problem)
    CASE 
        WHEN data_type = 'integer'::regtype THEN 
            'WARNING: You must ALSO change the Table Column to BIGINT. This requires a table rewrite (Downtime) unless performed via new_column+backfill.'
        ELSE 'Metadata change only (Safe)'
    END AS downtime_risk

FROM pg_sequences
WHERE schemaname NOT IN ('information_schema', 'pg_catalog') 
ORDER BY pct_used DESC;

-- 5. Denormalization Detector (Incremental Columns)
CREATE OR REPLACE VIEW maintenance_schema.rpt_schema_denormalization_patterns AS
SELECT 
    table_schema, table_name, 
    substring(column_name FROM '^(.*[^0-9])') AS column_prefix,
    count(*) AS distinct_columns,
    array_agg(column_name ORDER BY column_name) AS suspected_columns
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'maintenance_schema')
  AND column_name ~ '[0-9]$'
GROUP BY table_schema, table_name, column_prefix
HAVING count(*) > 1
ORDER BY distinct_columns DESC, table_name;

-- Part 4: Performance & Query Analysis

-- 6. Top 10 Time & IO Consumers
CREATE OR REPLACE VIEW maintenance_schema.rpt_pss_top_time_io AS
SELECT
    substring(query, 1, 100) AS short_query, calls,
    round(total_exec_time::numeric, 2) AS total_time_ms,
    round(mean_exec_time::numeric, 2) AS mean_time_ms,
    round((blk_read_time + blk_write_time)::numeric, 2) AS io_time_ms,
    round((100 * (blk_read_time + blk_write_time) / NULLIF(total_exec_time, 0))::numeric, 2) AS io_percent
FROM pg_stat_statements
ORDER BY total_exec_time DESC LIMIT 10;

-- 7. Top 10 Temp Files & Memory Spillers
CREATE OR REPLACE VIEW maintenance_schema.rpt_pss_top_temp_memory AS
SELECT
    substring(query, 1, 100) AS short_query, calls,
    temp_blks_written,
    pg_size_pretty(temp_blks_written * 8192) AS temp_disk_usage,
    CASE WHEN (shared_blks_hit + shared_blks_read) > 0 
         THEN round(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 2) ELSE 0 END AS cache_hit_pct
FROM pg_stat_statements
ORDER BY temp_blks_written DESC, shared_blks_read DESC LIMIT 10;

-- 8. Real-Time Blocking Tree
CREATE OR REPLACE VIEW maintenance_schema.rpt_activity_blocking_tree AS
SELECT 
    blocked.pid AS blocked_pid, blocked.usename AS blocked_user, blocked.wait_event,
    age(now(), blocked.query_start) AS blocked_duration,
    blocking.pid AS blocking_pid, blocking.usename AS blocking_user, blocking.state AS blocking_state,
    substring(blocking.query, 1, 80) AS blocking_query_short
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
ORDER BY blocked_duration DESC;

-- Part 5: Indexing Strategy

-- 9. Missing Indexes ("Smart" Seq Scan Detector)
CREATE OR REPLACE VIEW maintenance_schema.rpt_missing_index_candidates AS
SELECT
    schemaname, relname AS table_name,
    pg_size_pretty(pg_relation_size(relid)) AS table_size,
    seq_scan, idx_scan,
    round((seq_scan + 1) / (idx_scan + 1)::numeric, 2) AS seq_to_idx_ratio,
    CASE 
        WHEN seq_scan > idx_scan AND seq_tup_read > 10000000 THEN 'CRITICAL: Huge IO impact.'
        WHEN seq_scan > idx_scan THEN 'WARNING: Missing Index likely.'
        ELSE 'Monitor'
    END AS status
FROM pg_stat_user_tables
WHERE n_live_tup > 10000 AND pg_relation_size(relid) > 10 * 1024 * 1024
ORDER BY seq_tup_read DESC;

-- 10. Unindexed Foreign Keys (Locking Prevention)
CREATE OR REPLACE VIEW maintenance_schema.rpt_unindexed_foreign_keys AS
SELECT 
    c_parent.relname AS parent_table, c_child.relname AS child_table,
    a_child.attname AS child_column, con.conname AS constraint_name,
    format('CREATE INDEX ON %I.%I (%I);', n.nspname, c_child.relname, a_child.attname) AS suggested_fix
FROM pg_constraint con
JOIN pg_attribute a_child ON a_child.attrelid = con.conrelid AND a_child.attnum = con.conkey[1]
JOIN pg_class c_child ON con.conrelid = c_child.oid
JOIN pg_class c_parent ON con.confrelid = c_parent.oid
JOIN pg_namespace n ON c_child.relnamespace = n.oid
WHERE con.contype = 'f' 
  AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c_child.oid AND i.indkey[0] = con.conkey[1])
ORDER BY pg_relation_size(c_child.oid) DESC;

-- 11. Inefficient Indexes (Bloat)
CREATE OR REPLACE VIEW maintenance_schema.rpt_inefficient_indexes AS
SELECT
    schemaname, relname AS table_name, indexrelname AS index_name,
    idx_scan, idx_tup_read AS entries_scanned, idx_tup_fetch AS table_rows_fetched,
    CASE WHEN idx_tup_fetch > 0 THEN round(idx_tup_read::numeric / idx_tup_fetch::numeric, 2) ELSE 0 END AS scan_to_fetch_ratio,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE idx_scan > 100 AND idx_tup_read > 10000 AND (idx_tup_fetch = 0 OR (idx_tup_read::numeric / idx_tup_fetch::numeric) > 10)
ORDER BY idx_tup_read DESC;

-- Part 6: Maintenance & Autovacuum

-- 12. Autovacuum Thresholds & Bloat
CREATE OR REPLACE VIEW maintenance_schema.rpt_autovacuum_thresholds AS
SELECT 
    psut.schemaname, psut.relname, psut.n_live_tup, psut.n_dead_tup,
    (current_setting('autovacuum_vacuum_threshold')::bigint + (current_setting('autovacuum_vacuum_scale_factor')::numeric * psut.n_live_tup))::bigint AS vacuum_threshold,
    psut.last_autovacuum,
    CASE 
        WHEN psut.n_dead_tup > (current_setting('autovacuum_vacuum_threshold')::bigint + (current_setting('autovacuum_vacuum_scale_factor')::numeric * psut.n_live_tup))
            THEN 'OVERDUE (Bloating)'
        ELSE 'Safe'
    END AS vacuum_status
FROM pg_stat_user_tables psut
JOIN pg_class c ON psut.relid = c.oid
WHERE psut.n_live_tup > 1000
ORDER BY psut.n_dead_tup DESC;

-- 13. Tuning Candidates
CREATE OR REPLACE VIEW maintenance_schema.rpt_autovacuum_tuning_candidates AS
SELECT
    n.nspname, c.relname, pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
    c.reltuples::bigint AS estimated_rows,
    array_to_string(c.reloptions, ', ') AS current_custom_settings,
    CASE 
        WHEN c.reloptions::text ~ 'autovacuum_vacuum_scale_factor' THEN 'Custom Settings Exist (Good)'
        WHEN c.reltuples > 10000000 THEN 'CRITICAL: Set scale_factor=0.01 or 0.02'
        ELSE 'Default likely OK'
    END AS recommendation
FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND c.reltuples > 1000000
ORDER BY c.reltuples DESC;

-- Part 7: Replication & High Availability

-- 14. Replication Slots Health
CREATE OR REPLACE VIEW maintenance_schema.rpt_replication_slots_health AS
SELECT 
    slot_name, plugin, active,
    CASE WHEN NOT active THEN 'DANGER: Inactive Slot (Holding WAL)' ELSE 'OK' END AS status,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal_size,
    restart_lsn
FROM pg_replication_slots
ORDER BY active ASC, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;

-- 15. Replication Lag (Primary View)
CREATE OR REPLACE VIEW maintenance_schema.rpt_replication_primary_status AS
SELECT 
    client_addr AS replica_ip, usename, state, sync_state,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) AS send_lag_size,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)) AS total_lag_size,
    replay_lag AS time_lag
FROM pg_stat_replication;

-- Part 8: Security & Logic

-- 16. Roles & Security Audit
CREATE OR REPLACE VIEW maintenance_schema.rpt_roles_security_audit AS
SELECT 
    r.rolname, r.rolsuper, r.rolcreaterole,
    CASE WHEN r.rolpassword IS NULL THEN 'No Password set' ELSE 'Password set' END AS password_status,
    r.rolvaliduntil AS password_expiry,
    r.rolconnlimit
FROM pg_catalog.pg_roles r
WHERE r.rolname NOT LIKE 'pg_%'
ORDER BY r.rolsuper DESC, r.rolname;

-- 17. Functions Inventory (Risk & Type)
CREATE OR REPLACE VIEW maintenance_schema.rpt_functions_inventory AS
SELECT 
    n.nspname, p.proname, l.lanname AS language,
    CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' WHEN 'v' THEN 'VOLATILE' END AS volatility,
    CASE WHEN p.prosecdef THEN 'SECURITY DEFINER (Risky)' ELSE 'INVOKER (Safe)' END AS security_context
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
JOIN pg_language l ON p.prolang = l.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND p.prokind = 'f';
