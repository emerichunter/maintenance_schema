# maintenance_schema

A DBA framework for postgres using views.

Put both files in the extension folder.

Use `CREATE EXTENSION` in the wanted database.

**WARNING**: log out and back in for the schema to be loaded in the current user path.

**NOTE**: intended for postgres user or user with SUPER.

* `report_` views are meant as information 
* `audit_` views serve as warnings 
* `dba_` views contain DDL/DML statement (just \gexec it !)

**TODOs** :
- add `ALTER TABLE` statements for autovacuum and autoanalyze based on size
- add `ALTER TABLE... SET STATISTICS` if the need is confirmed
- audit check : pg_hba.conf, listen_addresses, unix socket,...
- encoding : to perfect (server side...)
- audit : security, (others?) -> On the way for version 0.0.2
- perfect row filtering on expect_av, foreign_fk (others?),
- add extensions pgstattuple, others ?

3
