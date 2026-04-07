setwd("C:/Users/TOURE/Documents/PADACORD/IM")
setwd("C:/Users/TOURE/Mes documents/REPOSITORIES/IM_raw_data/IM_raw/")
library(tidyverse)
library(dplyr)
library(sf)
library(flextable)
library(stringr)
library(stringi)
library(lubridate) 
library(readxl)
library(data.table)
library(arrow)      # For parquet files
library(qs)         # For qs files

# ============================================================
# FUNCTION TO READ MULTIPLE FILE FORMATS WITH ERROR HANDLING
# ============================================================

read_data_file <- function(file_path) {
  # Extract file extension
  file_ext <- tolower(tools::file_ext(file_path))
  
  # Read based on file extension with error handling
  result <- NULL
  
  tryCatch({
    result <- switch(file_ext,
                     # CSV files
                     csv = {
                       cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Reading CSV file:", file_path, "\n")
                       read_csv(file_path, show_col_types = FALSE)
                     },
                     # Excel files
                     xlsx = {
                       cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Reading Excel file:", file_path, "\n")
                       read_excel(file_path)
                     },
                     xls = {
                       cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Reading Excel file:", file_path, "\n")
                       read_excel(file_path)
                     },
                     # RDS files
                     rds = {
                       cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Reading RDS file:", file_path, "\n")
                       # Try different RDS read methods
                       tryCatch({
                         readRDS(file_path)
                       }, error = function(e) {
                         cat("  ⚠️ Standard readRDS failed, trying with gzcon...\n")
                         con <- gzcon(file(file_path, "rb"))
                         result <- tryCatch({
                           readRDS(con)
                         }, finally = {
                           close(con)
                         })
                         return(result)
                       })
                     },
                     # QS files (high-performance serialization)
                     qs = {
                       cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Reading QS file:", file_path, "\n")
                       qs::qread(file_path)
                     },
                     # Parquet files
                     parquet = {
                       cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Reading Parquet file:", file_path, "\n")
                       arrow::read_parquet(file_path)
                     },
                     # Default case
                     {
                       stop(paste("Unsupported file format:", file_ext, 
                                  "\nSupported formats: csv, xlsx, xls, rds, qs, parquet"))
                     }
    )
  }, error = function(e) {
    cat("  ❌ Error reading", file_path, ":", e$message, "\n")
    return(NULL)
  })
  
  if (is.null(result)) {
    stop(paste("Failed to read file:", file_path))
  }
  
  # Convert to tibble if not already
  result <- as_tibble(result)
  
  cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Successfully read", nrow(result), "rows and", ncol(result), "columns\n")
  return(result)
}

# ============================================================
# SPECIFY INPUT FILE - FILE "7178" IN ANY FORMAT
# ============================================================

# Define the base filename without extension
base_filename <- "7178"

# List all supported files that start with "7178" (case insensitive)
supported_files <- list.files(pattern = paste0("^", base_filename, "\\.(csv|rds|qs|parquet|xlsx|xls)$"), ignore.case = TRUE)

if (length(supported_files) == 0) {
  # Try a more flexible pattern
  supported_files <- list.files(pattern = paste0(base_filename, ".*\\.(csv|rds|qs|parquet|xlsx|xls)$"), ignore.case = TRUE)
  if (length(supported_files) == 0) {
    stop(paste("No supported file found for base name '7178'. Supported formats: .csv, .rds, .qs, .parquet, .xlsx, .xls"))
  }
}

# Display available files
cat("\n========================================\n")
cat("Available files for '7178':\n")
for (i in seq_along(supported_files)) {
  file_info <- file.info(supported_files[i])
  file_size <- round(file_info$size / 1024, 2)  # Size in KB
  cat(sprintf("  [%d] %s (size: %.2f KB, modified: %s)\n", 
              i, supported_files[i], file_size, file_info$mtime))
}

# Try each file format until one works
input_file <- NULL
for (i in seq_along(supported_files)) {
  cat("\n📌 Trying file:", supported_files[i], "\n")
  test_read <- tryCatch({
    read_data_file(supported_files[i])
  }, error = function(e) {
    cat("  ❌ Failed to read:", e$message, "\n")
    return(NULL)
  })
  
  if (!is.null(test_read)) {
    input_file <- supported_files[i]
    AB <- test_read
    cat("\n✅ Successfully loaded:", input_file, "\n")
    break
  }
}

if (is.null(input_file)) {
  stop("Could not read any of the available files. Please check file integrity.")
}

# Add Country column
AB <- AB |> mutate(Country = "NIE")

# Display first few column names to understand structure
cat("\n📋 First 20 column names in the dataset:\n")
print(head(names(AB), 20))

# Check data types of key columns
cat("\n📊 Data types of potential key columns:\n")
key_cols <- c("today", "date", "Date", "states", "state", "lgas", "lga")
for (col in key_cols) {
  if (col %in% names(AB)) {
    cat("  -", col, ":", class(AB[[col]])[1], "\n")
  }
}

# ============================================================
# COLUMN MAPPING AND STANDARDIZATION
# ============================================================

# Try to identify date column
if (!"today" %in% names(AB)) {
  if ("date" %in% names(AB)) {
    AB <- AB |> rename(today = date)
    cat("\n✓ Mapped 'date' column to 'today'\n")
  } else if ("Date" %in% names(AB)) {
    AB <- AB |> rename(today = Date)
    cat("\n✓ Mapped 'Date' column to 'today'\n")
  } else if ("assessment_date" %in% names(AB)) {
    AB <- AB |> rename(today = assessment_date)
    cat("\n✓ Mapped 'assessment_date' column to 'today'\n")
  } else {
    # Try to find any date column
    date_cols <- names(AB)[sapply(AB, function(x) inherits(x, c("Date", "POSIXct", "POSIXt")))]
    if (length(date_cols) > 0) {
      AB <- AB |> rename(today = all_of(date_cols[1]))
      cat("\n✓ Mapped", date_cols[1], "column to 'today'\n")
    }
  }
}

# Try to identify region/state column
if (!"states" %in% names(AB)) {
  if ("state" %in% names(AB)) {
    AB <- AB |> rename(states = state)
    cat("✓ Mapped 'state' column to 'states'\n")
  } else if ("Region" %in% names(AB)) {
    AB <- AB |> rename(states = Region)
    cat("✓ Mapped 'Region' column to 'states'\n")
  } else if ("region" %in% names(AB)) {
    AB <- AB |> rename(states = region)
    cat("✓ Mapped 'region' column to 'states'\n")
  }
}

# Try to identify district/LGA column
if (!"lgas" %in% names(AB)) {
  if ("lga" %in% names(AB)) {
    AB <- AB |> rename(lgas = lga)
    cat("✓ Mapped 'lga' column to 'lgas'\n")
  } else if ("District" %in% names(AB)) {
    AB <- AB |> rename(lgas = District)
    cat("✓ Mapped 'District' column to 'lgas'\n")
  } else if ("district" %in% names(AB)) {
    AB <- AB |> rename(lgas = district)
    cat("✓ Mapped 'district' column to 'lgas'\n")
  } else if ("LGA" %in% names(AB)) {
    AB <- AB |> rename(lgas = LGA)
    cat("✓ Mapped 'LGA' column to 'lgas'\n")
  }
}

# Check if we have the required columns
required_cols <- c("today", "states", "lgas")
missing_cols <- required_cols[!required_cols %in% names(AB)]

if (length(missing_cols) > 0) {
  cat("\n⚠️ Still missing required columns:", paste(missing_cols, collapse=', '), "\n")
  cat("Available columns:\n")
  print(names(AB))
  stop("Cannot proceed without required columns")
}

# ============================================================
# PROCESS NIGERIA IM DATA
# ============================================================

cat("\n📊 Processing Nigeria IM data...\n")

# Initial filtering and cleaning
AC <- AB |> 
  filter(!is.na(today)) |> 
  mutate(today = as.Date(today)) |>  # Simplified date conversion
  mutate(year = year(today)) |>
  mutate(year = as.numeric(year)) |> 
  filter(year > 2019)

cat("  - Filtered to", nrow(AC), "rows after initial cleaning\n")

# Identify house columns - look for patterns
imm_cols <- names(AC)[grepl("Imm_Seen_house", names(AC), ignore.case = TRUE)]
unimm_cols <- names(AC)[grepl("unimm_h", names(AC), ignore.case = TRUE)]

if (length(imm_cols) == 0) {
  # Try alternative patterns
  imm_cols <- names(AC)[grepl("children.*seen|child.*seen|u5.*seen", names(AC), ignore.case = TRUE)]
  cat("  - Found alternative immunized columns:", length(imm_cols), "\n")
}

if (length(unimm_cols) == 0) {
  # Try alternative patterns
  unimm_cols <- names(AC)[grepl("unimmun|missed|not.*immun", names(AC), ignore.case = TRUE)]
  cat("  - Found alternative unimmunized columns:", length(unimm_cols), "\n")
}

# Convert to numeric
if (length(imm_cols) > 0) {
  AC <- AC |> mutate(across(all_of(imm_cols), as.numeric))
}
if (length(unimm_cols) > 0) {
  AC <- AC |> mutate(across(all_of(unimm_cols), as.numeric))
}

# Calculate aggregates
AD <- AC |> 
  mutate(
    u5_FM = if (length(imm_cols) > 0) rowSums(across(all_of(imm_cols)), na.rm = TRUE) else 0,
    missed_child = if (length(unimm_cols) > 0) rowSums(across(all_of(unimm_cols)), na.rm = TRUE) else 0,
    u5_present = (u5_FM + missed_child)
  )

# Select and create month
AE <- AD |> 
  select(any_of(c("Country", "states", "lgas", "today", "siatype", "vactype", "vactype_other"))) |>
  rename(Region = states, District = lgas, date = today) |>
  mutate(month = month(date, label = TRUE, abbr = TRUE))

# Add back calculated columns
AE$u5_present <- AD$u5_present
AE$u5_FM <- AD$u5_FM
AE$missed_child <- AD$missed_child

# Clean vactype_other if column exists
if ("vactype_other" %in% names(AE)) {
  AE$vactype_other <- tolower(AE$vactype_other)
  AE$vactype_other[AE$vactype_other %in% c("b0pv", "bopv")] <- "bOPV"
  AE$vactype_other[AE$vactype_other %in% c("cmopv2", "mopv2", "mopv")] <- "mOPV"
  AE$vactype_other[AE$vactype_other %in% c("fipv", "fipv+nopv2", "fipv plus", "fipv plus nopv2")] <- "FIPV+nOPV2"
  AE$vactype_other[AE$vactype_other %in% c("n opv", "n opv3", "nopv2")] <- "nOPV2"
  AE$vactype_other[AE$vactype_other == "hpv"] <- "HPV"
}

# Continue with the rest of the processing...
# (Continue with roundNumber, Vaccine.type, Response assignments as in previous version)

# Initial roundNumber assignment based on month
AF <- AE |> 
  mutate(roundNumber = case_when(
    str_detect(month, pattern = "janv|jan") ~ "Rnd1",
    str_detect(month, pattern = "févr|feb") ~ "Rnd2",
    str_detect(month, pattern = "mars|mar") ~ "Rnd3",
    str_detect(month, pattern = "avr|apr") ~ "Rnd4",
    str_detect(month, pattern = "mai|may") ~ "Rnd5",
    str_detect(month, pattern = "juin|jun") ~ "Rnd6",
    str_detect(month, pattern = "juil|jul") ~ "Rnd7",
    str_detect(month, pattern = "août|aug") ~ "Rnd8",
    str_detect(month, pattern = "sept|sep") ~ "Rnd9",
    str_detect(month, pattern = "oct") ~ "Rnd10",
    str_detect(month, pattern = "nov") ~ "Rnd11",
    str_detect(month, pattern = "déc|dec") ~ "Rnd12"))

# Refine roundNumber based on year
AG <- AF |> 
  mutate(year = year(date),
         roundNumber = case_when(
           year == 2025 & month %in% c("jan", "janv", "févr", "feb", "mars", "mar", "avr", "apr", "mai", "may") ~ "Rnd1",
           year == 2025 & month %in% c("juin", "jun", "juil", "jul", "août", "aug") ~ "Rnd2",
           year == 2025 & month %in% c("sept", "sep", "oct", "nov") ~ "Rnd3",
           year == 2025 & month == "déc" ~ "Rnd4",
           year == 2024 & month %in% c("févr", "feb", "mars", "mar") ~ "Rnd1",
           year == 2024 & month %in% c("avr", "apr", "mai", "may", "juin", "jun", "juil", "jul", "août", "aug") ~ "Rnd2",
           year == 2024 & month %in% c("sept", "sep", "oct") ~ "Rnd3",
           year == 2024 & month == "nov" ~ "Rnd4",
           year == 2024 & month == "déc" ~ "Rnd5",
           TRUE ~ roundNumber))

# Assign Response based on year and month
AH <- AG |> 
  mutate(Response = case_when(
    year == 2025 & month %in% c("jan", "janv") ~ "OBR1",
    year == 2025 & month %in% c("févr", "feb", "mars", "mar", "avr", "apr", "mai", "may", "juin", "jun", "juil", "jul", "août", "aug") ~ "NIE-2025-04-01_nOPV_NIDs",
    year == 2025 & month %in% c("sept", "sep", "oct", "nov") ~ "NIE-2025-10-01_nOPV_sNID",
    year == 2024 ~ "NIE-2024-nOPV2",
    year == 2023 & month %in% c("mai", "may", "juin", "jun") ~ "NIE-2023-04-02_nOPV",
    year == 2023 ~ "NIE-2023-07-03_nOPV",
    TRUE ~ "Unknown"))

# Assign Vaccine.type
AH <- AH |> 
  mutate(Vaccine.type = case_when(
    year >= 2024 ~ "nOPV2",
    year == 2023 & month %in% c("mai", "may", "juin", "jun", "juil", "jul", "sept", "sep") ~ "fIPV+nOPV2",
    year == 2023 ~ "nOPV2",
    year == 2022 & month == "juil" ~ "nOPV2",
    year == 2022 ~ "bOPV",
    year == 2021 & month %in% c("mars", "mar", "avr", "apr", "mai", "may", "juin", "jun", "juil", "jul", "août", "aug", "sept", "sep", "oct") ~ "nOPV2",
    year == 2021 ~ "bOPV",
    TRUE ~ "nOPV2"))

# ============================================================
# AGGREGATION TO DISTRICT LEVEL
# ============================================================

cat("\n📊 Aggregating to district level...\n")

AK <- AH |> 
  filter(!is.na(Region), !is.na(District)) |>
  group_by(Country, Region, District, Response, Vaccine.type, roundNumber) |>
  summarise(
    start_date = min(date, na.rm = TRUE),
    end_date = max(date, na.rm = TRUE),
    year = first(year),
    month = first(month),
    u5_present = sum(u5_present, na.rm = TRUE),
    u5_FM = sum(u5_FM, na.rm = TRUE),
    missed_child = sum(missed_child, na.rm = TRUE),
    cv = ifelse(u5_present > 0, round(u5_FM / u5_present, 2), NA_real_),
    .groups = "drop"
  )

cat("  - Aggregated to", nrow(AK), "district-level records\n")

# ============================================================
# SAVE RESULTS
# ============================================================

# Create output directory if it doesn't exist
output_dir <- "C:/Users/TOURE/Mes documents/REPOSITORIES/IM_raw_data/IM_level/"
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
  cat("\n📁 Created output directory:", output_dir, "\n")
}

# Generate output filename with date
output_base <- paste0(output_dir, "NIE_IM_7178_", format(Sys.Date(), "%Y%m%d"))

# Save as CSV (most compatible)
output_file_csv <- paste0(output_base, ".csv")
write_csv(AK, output_file_csv)
cat("\n💾 Saved CSV:", output_file_csv)

# Save as RDS
output_file_rds <- paste0(output_base, ".rds")
saveRDS(AK, output_file_rds)
cat("\n💾 Saved RDS:", output_file_rds)

# ============================================================
# SUMMARY STATISTICS
# ============================================================

cat("\n\n========================================\n")
cat("✅ PROCESSING COMPLETE\n")
cat("========================================\n")
cat("Input file:", input_file, "\n")
cat("Total records:", nrow(AK), "\n")
cat("Unique Regions:", length(unique(AK$Region)), "\n")
cat("Unique Districts:", length(unique(AK$District)), "\n")
cat("\n📊 Unique Responses:\n")
for (resp in unique(AK$Response)) {
  cat("  -", resp, "\n")
}
cat("\n💉 Unique Vaccine types:\n")
for (vac in unique(AK$Vaccine.type)) {
  cat("  -", vac, "\n")
}
cat("\n📅 Date range:\n")
if(nrow(AK) > 0) {
  cat("  From:", min(AK$start_date, na.rm = TRUE), "\n")
  cat("  To:", max(AK$end_date, na.rm = TRUE), "\n")
}
cat("\n📁 Output files saved to:", output_dir, "\n")
cat("   Base filename:", output_base, "\n")
cat("========================================\n\n")