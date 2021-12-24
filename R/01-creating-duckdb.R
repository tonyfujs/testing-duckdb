library(DBI)
library(dplyr)
library(duckdb)
library(arrow)


# Check existing data -----------------------------------------------------

# Initiate empty database
con <- dbConnect(drv = duckdb::duckdb(), 
                 dbdir = "./output/duckdb/pip.duckdb", 
                 read_only = FALSE)
# Populate database with a table of all survey data
dbSendQuery(con, "CREATE TABLE survey_data AS SELECT * FROM parquet_scan('input/*.parquet')")

dbDisconnect(con)
