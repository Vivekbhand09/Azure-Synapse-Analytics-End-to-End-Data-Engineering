-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Synapse#Learn2026!Key';

--------------------------------
--- Databricks Scoped Credential
--------------------------------

CREATE DATABASE SCOPED CREDENTIAL viv_creds
WITH IDENTITY = 'Managed Identity'

--------------------------------
--- External Data Source
--------------------------------

CREATE EXTERNAL DATA SOURCE silver_source
WITH
(
   LOCATION = 'https://azurewarehouselake.dfs.core.windows.net/silver/',
   CREDENTIAL = viv_creds
)

CREATE EXTERNAL DATA SOURCE gold_source
WITH
(
   LOCATION = 'https://azurewarehouselake.dfs.core.windows.net/gold/',
   CREDENTIAL = viv_creds
)

--------------------------------
--- External File Format
--------------------------------

CREATE EXTERNAL FILE FORMAT parquet
WITH 
(
    FORMAT_TYPE= PARQUET,
    DATA_COMPRESSION= 'org.apache.hadoop.io.compress.SnappyCodec'
)

CREATE EXTERNAL FILE FORMAT delta
WITH 
(
    FORMAT_TYPE= DELTA
)


