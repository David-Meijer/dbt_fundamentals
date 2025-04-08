This dbt project serves as a personal referencing project using the canonical Jaffle Shop example.

It can be run locally without any connections to a cloud data platform by making use of DuckDB.
Therefore, the only dependency is dbt-duckdb

As DBT cannot directly read from local flat files without 'misusing' the seeds functionality, the DBT project uses another 'raw database' (raw_files_database.duckdb) in the /raw folder 

1. Run the ingestion.py script
2. Build/run the dbt project
