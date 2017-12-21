# maintenance_schema

A DBA framework for postgres using views.

# INSTALL

Put both files in the extension folder.

Use `CREATE EXTENSION` in the wanted database(s).

## **WARNING**: log out and back in for the schema to be loaded in the current user path.

## **NOTE**: intended for postgres user or user with SUPER.

* `rpt_` views are meant as information 
* `audit_` views serve as warnings 
* `dba_` views contain DDL/DML statement (just \gexec it !)

# **TODOs** :
- add `ALTER TABLE` statements for autovacuum and autoanalyze based on size
- add `ALTER TABLE... SET STATISTICS` if the need is confirmed, watch table/column special statistics in audit
- audit check : pg_hba.conf, listen_addresses, unix socket,...  security in general, (others?) -> On the way for version 0.0.2
- encoding : to perfect (add server side...)
- perfect row filtering on expect_av, foreign_fk (others?),
- add extensions pgstattuple, others ?
- Version compatibility handling for 9.3 and 9.4

VERSION COMPATIBILITY
=====================

All views should be compatible from 9.5 and up.
Some views might be compatible starting with 9.4.


