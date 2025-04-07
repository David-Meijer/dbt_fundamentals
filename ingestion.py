#####################################################################################################
#as dbt cannot read from flat files stored locally, we will create a raw database using duckdb first#
#####################################################################################################

import duckdb

#connect duckdb to persisted database file
#change name of the duckdb database file in order to create another database file
con = duckdb.connect('raw_files/raw_files_database.duckdb')

con.sql('CREATE SCHEMA IF NOT EXISTS raw;')

#ingest jaffle_shop orders from csv into the raw database
sql = """
DROP TABLE IF EXISTS raw.jaffle_shop_orders;
CREATE TABLE raw.jaffle_shop_orders(
     ID bigint
    ,USER_ID bigint
    ,ORDER_DATE DATE
    ,STATUS varchar
);
COPY raw.jaffle_shop_orders FROM 'raw_files/jaffle_shop_orders.csv' (DELIMITER ',', HEADER);
"""
con.sql(sql)

#ingest jaffle_shop customers from csv into the raw database
sql = """
DROP TABLE IF EXISTS raw.jaffle_shop_customers;
CREATE TABLE raw.jaffle_shop_customers(
     ID bigint
    ,FIRST_NAME varchar
    ,LAST_NAME varchar
);
COPY raw.jaffle_shop_customers FROM 'raw_files/jaffle_shop_customers.csv' (DELIMITER ',', HEADER);
"""
con.sql(sql)

#ingest stripe_payments from csv into the raw database
sql = """
DROP TABLE IF EXISTS raw.stripe_payments;
CREATE TABLE raw.stripe_payments(
     ID bigint
    ,ORDERID bigint
    ,PAYMENTMETHOD varchar
    ,STATUS varchar
    ,AMOUNT int
    ,CREATED date
);
COPY raw.stripe_payments FROM 'raw_files/stripe_payments.csv' (DELIMITER ',', HEADER);
"""
con.sql(sql)

##Control
# print(con.sql('SELECT COUNT(*) FROM raw.jaffle_shop_orders;')) #99
# print(con.sql('SELECT COUNT(*) FROM raw.jaffle_shop_customers;')) #100
# print(con.sql('SELECT COUNT(*) FROM raw.stripe_payments;')) #120