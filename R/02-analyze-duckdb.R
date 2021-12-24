library(DBI)
library(dplyr)
library(duckdb)
library(arrow)
library(tictoc)

# Initiate connection
con <- dbConnect(drv = duckdb::duckdb(), 
                 dbdir = "./output/duckdb/pip.duckdb", 
                 read_only = TRUE)

tbl(con, "survey_data") %>%
  # arrow::to_arrow() %>%
  select(country_code) %>%
  distinct() %>%
  collect()
  
tbl(con, "survey_data") %>%
  count() 
