# Web scrape data from NLRB

# Restore environment ---------------------------------------------------------

renv::restore()

# Import ----------------------------------------------------------------------

library(chromote)
library(here)
library(lubridate)
library(tidyverse)
library(rvest)

# Constants -------------------------------------------------------------------

DATA_DIR <- here("data", "nlrb-2022-09-23")
PAGE_FIRST <- 1162
PAGE_LAST <- 1363
DATE_START <- "01/01/1990"
DATE_END <- strftime(now() - days(1), "%m/%d/%Y")

# Functions -------------------------------------------------------------------

fetch_html <- function(page_number) {
  url <- str_glue("https://www.nlrb.gov/reports/graphs-data/recent-election-results/date_issued/asc?date_start={DATE_START}&date_end={DATE_END}&page={page_number}")
  data_flag <- "pager__item"
  html <- ""
  
  browser <- ChromoteSession$new()
  browser$Page$navigate(url)
  
  # Wait until the data you need to scrape has loaded into the webpage
  while(! str_detect(html, data_flag)) {
    Sys.sleep(1.5)
    doc <- browser$DOM$getDocument()
    html <- browser$DOM$getOuterHTML(doc$root$nodeId)[[1]]
  }
  
  read_html(html)
}

scrape_page <- function(page_number, save = FALSE) {
  
  print(str_glue("Scraping page {page_number}"))
  
  html <- fetch_html(page_number)
  
  rows <- html |> 
    html_elements(".rer-content") |> 
    html_text2() |> 
    str_split("\n")
  
  page_data <- map_dfr(rows, function(row) {
    
    tibble(
      company_name = row[[1]],
      case_number = str_sub(row[[2]], 14, -1),
      tally_issued_date = dmy(str_sub(row[[3]], 14, -1)),
      tally_type = str_sub(row[[4]], 13, -1),
      ballot_type = str_sub(row[[5]], 14, -1),
      date_filed = dmy(str_sub(row[[6]], 13, -1)),
      status = str_sub(row[[7]], 9, -1),
      n_eligible_voters = as.numeric(str_sub(row[[8]], 25, -1)),
      void_ballots = as.numeric(str_sub(row[[9]], 15, -1)),
      unit_location = str_sub(row[[10]], 16, -1),
      region_assigned = str_sub(row[[11]], 18, -1),
      unit_id = str_sub(row[[12]], 10, -1),
      votes_against = as.numeric(str_sub(row[[13]], 16, -1)),
      total_ballots_counted = as.numeric(str_sub(row[[14]],24, -1)),
      votes_for_labour_union = get_votes_for_labour_union(row),
      labour_union = get_labour_union(row),
      union_to_certify = get_union_to_certify(row),
      voting_unit = get_voting_unit(row))
  })
  
  if (save == TRUE) {
    page_file <- str_glue("nlrb-election-results-page-{page_number}.csv")
    page_data |> write_csv(here("data", page_file))
  }  
  
  page_data
}

scrape_page_with_retries <- function(page_number, save = FALSE) {
  page_data <- NULL
  incomplete = TRUE 
  while (incomplete) {
    tryCatch(
      {
        page_data <- scrape_page(page_number, save = save)
        incomplete <- FALSE
      },
      error = function(e) {
        print(str_glue("Caught error: Retrying page {page_number} ..."))
      })
  }
  page_data
}

# Functions to find optional fields -------------------------------------------

get_votes_for_labour_union <- function(row) {
  for (i in 1:length(row)) {
    if (str_starts(row[[i]], "Votes for Labor Union1: ")) {
      return(as.numeric(str_sub(row[[i]], 25, -1)))
    }
  }
  return(NA)
}

get_labour_union <- function(row) {
  for (i in 1:length(row)) {
    if (str_starts(row[[i]], "Labor Union1: ")) {
      return(str_sub(row[[i]], 15, -1))
    }
  }
  return(NA)
}

get_union_to_certify <- function(row) {
  for (i in 1:length(row)) {
    if (str_starts(row[[i]], "Union to Certify: ")) {
      return(str_sub(row[[i]], 19, -1))
    }
  }
  return(NA)
}

get_voting_unit <- function(row) {
  for (i in 1:length(row)) {
    if (str_starts(row[[i]], "Voting Unit \\(Unit A\\): ")) {
      return(str_sub(row[[i]], 23, -1))
    }
  }
  return(NA)
}

# Scrape all pages ------------------------------------------------------------

scrape_pages <- function(page_numbers, save = TRUE) {
  map_dfr(page_numbers, ~ scrape_page_with_retries(., save = save))
}

# Compile scraped pages -------------------------------------------------------

compile_dataset <- function(data_dir = DATA_DIR) {
  files <- list.files(data_dir)
  map_dfr(files, function(f) {
    read_csv(here(data_dir, f), show_col_types = FALSE)
  })
}

# Main ------------------------------------------------------------------------

data <- scrape_pages(PAGE_FIRST:PAGE_LAST)
dataset <- compile_dataset()
