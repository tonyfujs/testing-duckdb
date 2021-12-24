library(pipapi)
library(arrow)

lkups <- pipapi::create_versioned_lkups(Sys.getenv("PIPAPI_DATA_ROOT_FOLDER"))
svy_lkup <- lkups$versions_paths$latest_release$svy_lkup


#' Create a dataframe from survey data and metadata
#'
#' @param lkup_row 
#'
#' @return data.frame
create_svy_file <- function(lkup_row) {
  tmp <- fst::read_fst(lkup_row$path)
  tmp$country_code <- lkup_row$country_code
  tmp$year <- lkup_row$reporting_year
  tmp$reporting_level <- lkup_row$reporting_level
  tmp$welfare_type <- lkup_row$welfare_type
  
  return(tmp)
}


#' Save created data.frame as parquet file for ingestion into duckdb or use with arrow
#'
#' @param df 
#' @param file_name 
#' @param path 
#' 
save_svy_file <- function(df, file_name, path) {
  arrow::write_parquet(df, sink = paste0(path, "/", file_name, ".parquet"))
} 

# Loop over all available .fst files and create .parquet
offset <- 0
for (i in (seq_along(svy_lkup$cache_id) + offset)) {
  lkup_row <- svy_lkup[i, ]
  tmp <- create_svy_file(lkup_row)
  save_svy_file(tmp, file_name = lkup_row$cache_id, path = "./input")
  print(cat(offset + i, ": ", lkup_row$cache_id))
  gc()
}
