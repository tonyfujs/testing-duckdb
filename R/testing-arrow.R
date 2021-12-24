library(DBI)
library(dplyr)
library(duckdb)
library(arrow)
library(tictoc)


# Check existing data -----------------------------------------------------
# Number of files and total size
svy_data <- fs::dir_info("input", recurse = TRUE) %>%
  filter(type == "file") %>% 
  summarise(n = n(), size = sum(size)) 
glue::glue("There are {svy_data$n} files, totaling {svy_data$size}!")

# Total number of rows (without loading dataset in memory)
ds <- open_dataset("input")
# full_collect <- summarise(ds, n = n()) %>% 
#   collect() %>% 
#   pull(n)
# n_rows <- scales::unit_format(unit = "millions", scale = 1e-6, 
#                               accuracy = 1)(full_collect)
# glue::glue("There are approximately {n_rows} rows!")

# Compute welfare means by sub-groups
# Weighted means not supported yet
# tic()
# ds %>%
#   select(country_code, year, welfare_type, reporting_level, welfare, weight) %>%
#   # use arrow to populate directly into a duckdb
#   arrow::to_duckdb() %>% 
#   # calculate a new column, on disk!
#   group_by(country_code, year, reporting_level, welfare_type) %>%
#   summarise(
#     mean = mean(welfare)
#     ) %>%
#   collect() %>%
#   print()
# toc()

con <- dbConnect(drv = duckdb::duckdb(), 
                 dbdir = "./output/duckdb/pip.duckdb", 
                 read_only = FALSE)
duckdb::duckdb_register_arrow(conn = con, 
                              name = "pip", 
                              arrow_scannable = ds)

dbListTables(con)
dbAppendTable()
