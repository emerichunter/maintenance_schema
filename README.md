# maintenance_schema

A DBA framework for postgres using views. 

* 0.0.4 is corrected for PG12
* Default is version 0.0.3 intended for PG10, PG11 ~~and higher~~. 
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




VERSION COMPATIBILITY
=====================

All views should be compatible from 9.5 and up.
Some views might be compatible starting with 9.4.
I will try to maintain all currently supported versions.


WRAPAROUND 
========================

Percentage to monitor (from pgmonitor) / database
~~~~sql
WITH max_age AS ( 
    SELECT 2000000000 as max_old_xid
        , setting AS autovacuum_freeze_max_age 
        FROM pg_catalog.pg_settings 
        WHERE name = 'autovacuum_freeze_max_age' )
, per_database_stats AS ( 
    SELECT datname
        , m.max_old_xid::int
        , m.autovacuum_freeze_max_age::int
        , age(d.datfrozenxid) AS oldest_current_xid 
    FROM pg_catalog.pg_database d 
    JOIN max_age m ON (true) 
    WHERE d.datallowconn ) 
SELECT max(oldest_current_xid) AS oldest_current_xid
    , max(ROUND(100*(oldest_current_xid/max_old_xid::float))) AS percent_towards_wraparound
    , max(ROUND(100*(oldest_current_xid/autovacuum_freeze_max_age::float))) AS percent_towards_emergency_autovac 
FROM per_database_stats;
~~~~

per table 
~~~~sql 

SELECT datname
    , age(datfrozenxid)
    , current_setting('autovacuum_freeze_max_age') 
FROM pg_database 
ORDER BY 2 DESC;
~~~~

Most urgent tables 
~~~~sql
SELECT c.oid::regclass
    , age(c.relfrozenxid)
    , pg_size_pretty(pg_total_relation_size(c.oid)) 
FROM pg_class c
JOIN pg_namespace n on c.relnamespace = n.oid
WHERE relkind IN ('r', 't', 'm') 
AND n.nspname NOT IN ('pg_toast')
ORDER BY 2 DESC LIMIT 100;
~~~~

script for vacuum freeze table list one at a time 

~~~~sh
\t \o /tmp/vacuum.sql select 'vacuum freeze analyze verbose ' || oid::regclass || ';' from pg_class where relkind in ('r', 't', 'm') order by age(relfrozenxid) desc limit 100; \o \t \set ECHO all \i /tmp/vacuum.sql
~~~~
