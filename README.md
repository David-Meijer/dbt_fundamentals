This dbt project serves as a personal referencing project using the canonical Jaffle Shop example.

It can be run locally without any connections to a cloud data platform by making use of DuckDB.
Therefore, the only dependency is dbt-duckdb

As DBT cannot directly read from local flat files without 'misusing' the seeds functionality, i have created a second database (raw_files_database.duckdb) in the /raw folder where the raw files are stored as tables.
This approach can be replicated or altered by opening the ingestion.py script. Make sure to make the necesarry changes (if any) in the profiles.yml and _sources_*.yml files!
