setwd("C:/Users/TOURE/Documents/PADACORD/IM")
# setwd("C:/Users/TOURE/Mes documents/REPOSITORIES/IM_raw_data/IM_raw/")
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
# FUNCTION TO READ FILE WITH MULTIPLE METHODS
# ============================================================

read_file_with_multiple_methods <- function(file_path) {
  cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Attempting to read:", file_path, "\n")
  
  # Get file extension
  file_ext <- tolower(tools::file_ext(file_path))
  
  # Define reading methods to try based on extension and fallbacks
  reading_methods <- list()
  
  # Primary method based on extension
  if (file_ext == "rds") {
    # For .rds files, try qs first (since you mentioned it's actually qs format)
    reading_methods <- list(
      list(name = "qs::qread", 
           func = function(f) qs::qread(f),
           description = "Reading as QS format (despite .rds extension)"),
      list(name = "readRDS (standard)", 
           func = function(f) readRDS(f),
           description = "Reading as standard RDS"),
      list(name = "readRDS with gzcon", 
           func = function(f) {
             con <- gzcon(file(f, "rb"))
             on.exit(close(con))
             readRDS(con)
           },
           description = "Reading RDS with gzcon wrapper")
    )
  } else if (file_ext == "qs") {
    reading_methods <- list(
      list(name = "qs::qread", 
           func = function(f) qs::qread(f),
           description = "Reading as QS format"),
      list(name = "readRDS (fallback)", 
           func = function(f) readRDS(f),
           description = "Falling back to RDS")
    )
  } else if (file_ext == "parquet") {
    reading_methods <- list(
      list(name = "arrow::read_parquet", 
           func = function(f) arrow::read_parquet(f),
           description = "Reading as Parquet"),
      list(name = "readRDS (fallback)", 
           func = function(f) readRDS(f),
           description = "Falling back to RDS")
    )
  } else if (file_ext == "csv") {
    reading_methods <- list(
      list(name = "read_csv", 
           func = function(f) read_csv(f, show_col_types = FALSE),
           description = "Reading as CSV"),
      list(name = "fread", 
           func = function(f) data.table::fread(f) %>% as_tibble(),
           description = "Reading with fread")
    )
  } else {
    # Generic fallbacks for any file
    reading_methods <- list(
      list(name = "qs::qread", 
           func = function(f) qs::qread(f),
           description = "Attempting QS format"),
      list(name = "readRDS", 
           func = function(f) readRDS(f),
           description = "Attempting RDS format"),
      list(name = "read_csv", 
           func = function(f) read_csv(f, show_col_types = FALSE),
           description = "Attempting CSV format"),
      list(name = "arrow::read_parquet", 
           func = function(f) arrow::read_parquet(f),
           description = "Attempting Parquet format")
    )
  }
  
  # Try each reading method
  result <- NULL
  for (method in reading_methods) {
    cat("  🔄 Trying method:", method$name, "-", method$description, "\n")
    
    result <- tryCatch({
      method$func(file_path)
    }, error = function(e) {
      cat("    ❌ Failed:", e$message, "\n")
      return(NULL)
    })
    
    if (!is.null(result)) {
      cat("    ✅ Successfully read using:", method$name, "\n")
      break
    }
  }
  
  if (is.null(result)) {
    stop(paste("Failed to read file with any method:", file_path))
  }
  
  # Convert to tibble
  result <- as_tibble(result)
  
  cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "- Successfully read", nrow(result), "rows and", ncol(result), "columns\n")
  return(result)
}

# ============================================================
# FUNCTION TO TRY ALL FILES IN DIRECTORY
# ============================================================

find_and_read_data <- function(base_filename = "7178") {
  # List all files with the base filename (any extension)
  all_files <- list.files(pattern = paste0("^", base_filename, ".*\\..+$"), ignore.case = TRUE)
  
  if (length(all_files) == 0) {
    # Try more flexible pattern
    all_files <- list.files(pattern = base_filename, ignore.case = TRUE)
  }
  
  if (length(all_files) == 0) {
    stop(paste("No files found with base name:", base_filename))
  }
  
  # Display all available files
  cat("\n========================================\n")
  cat("Available files:\n")
  for (i in seq_along(all_files)) {
    file_info <- file.info(all_files[i])
    file_size <- round(file_info$size / 1024, 2)
    cat(sprintf("  [%d] %s (size: %.2f KB, modified: %s)\n", 
                i, all_files[i], file_size, file_info$mtime))
  }
  cat("========================================\n")
  
  # Try each file until one works
  for (i in seq_along(all_files)) {
    cat("\n📂 Trying file", i, "of", length(all_files), ":", all_files[i], "\n")
    cat("─────────────────────────────────────────\n")
    
    data <- tryCatch({
      read_file_with_multiple_methods(all_files[i])
    }, error = function(e) {
      cat("  ❌ All methods failed for:", all_files[i], "\n")
      cat("  Error:", e$message, "\n")
      return(NULL)
    })
    
    if (!is.null(data)) {
      cat("\n✅ SUCCESS! Loaded data from:", all_files[i], "\n")
      return(list(data = data, filename = all_files[i]))
    }
  }
  
  stop("Could not read any of the available files.")
}

# ============================================================
# READ THE DATA
# ============================================================

# Try to find and read the data
result <- find_and_read_data("7178")
AB <- result$data
input_file <- result$filename

# Add Country column
AB <- AB |> mutate(Country = "NIE")

# Display basic info about the data
cat("\n📋 Data overview:\n")
cat("  - Dimensions:", nrow(AB), "rows ×", ncol(AB), "columns\n")
cat("  - Column names (first 20):\n")
for (i in 1:min(20, ncol(AB))) {
  cat("    •", names(AB)[i], ":", class(AB[[i]])[1], "\n")
}

# ============================================================
# COLUMN MAPPING AND STANDARDIZATION
# ============================================================

cat("\n🔄 Standardizing column names...\n")

# Define column mapping patterns
column_mappings <- list(
  # Date columns
  date_cols = list(
    target = "today",
    patterns = c("today", "date", "Date", "assessment_date", "visit_date", "survey_date")
  ),
  # Region/State columns
  state_cols = list(
    target = "states",
    patterns = c("states", "state", "Region", "region", "admin1", "province")
  ),
  # District/LGA columns
  district_cols = list(
    target = "lgas",
    patterns = c("lgas", "lga", "District", "district", "admin2", "ward")
  )
)

# Apply mappings
for (mapping in column_mappings) {
  target <- mapping$target
  if (!target %in% names(AB)) {
    for (pattern in mapping$patterns) {
      if (pattern %in% names(AB)) {
        AB <- AB |> rename(!!target := all_of(pattern))
        cat("  ✓ Mapped '", pattern, "' → '", target, "'\n", sep="")
        break
      }
    }
  }
}

# Check for required columns
required_cols <- c("today", "states", "lgas")
missing_cols <- required_cols[!required_cols %in% names(AB)]

if (length(missing_cols) > 0) {
  cat("\n⚠️ Missing required columns:", paste(missing_cols, collapse=", "), "\n")
  cat("\nAll available columns:\n")
  print(names(AB))
  stop("Cannot proceed without required columns. Please check column names.")
}

# ============================================================
# PROCESS NIGERIA IM DATA
# ============================================================

cat("\n📊 Processing Nigeria IM data...\n")

# Convert date
AC <- AB |> 
  filter(!is.na(today)) |> 
  mutate(today = as.Date(today)) |>
  mutate(year = year(today)) |>
  filter(year > 2019)

cat("  - Filtered to", nrow(AC), "rows after initial cleaning\n")

# Find immunized and unimmunized columns
# Look for various naming patterns
imm_patterns <- c("Imm_Seen_house", "immunized", "vaccinated", "u5_FM", "children_seen", "child_seen")
unimm_patterns <- c("unimm_h", "unimmunized", "not_vaccinated", "missed", "children_not_seen")

# Find columns matching patterns
imm_cols <- c()
for (pattern in imm_patterns) {
  cols <- names(AC)[grepl(pattern, names(AC), ignore.case = TRUE)]
  if (length(cols) > 0) {
    imm_cols <- c(imm_cols, cols)
    cat("  - Found immunized columns with pattern '", pattern, "': ", length(cols), " columns\n", sep="")
  }
}

unimm_cols <- c()
for (pattern in unimm_patterns) {
  cols <- names(AC)[grepl(pattern, names(AC), ignore.case = TRUE)]
  if (length(cols) > 0) {
    unimm_cols <- c(unimm_cols, cols)
    cat("  - Found unimmunized columns with pattern '", pattern, "': ", length(cols), " columns\n", sep="")
  }
}

# Remove duplicates
imm_cols <- unique(imm_cols)
unimm_cols <- unique(unimm_cols)

cat("  - Total immunized columns:", length(imm_cols), "\n")
cat("  - Total unimmunized columns:", length(unimm_cols), "\n")

# Convert to numeric
if (length(imm_cols) > 0) {
  AC <- AC |> mutate(across(all_of(imm_cols), ~ as.numeric(as.character(.))))
}
if (length(unimm_cols) > 0) {
  AC <- AC |> mutate(across(all_of(unimm_cols), ~ as.numeric(as.character(.))))
}

# Calculate aggregates
AD <- AC |> 
  mutate(
    u5_FM = if (length(imm_cols) > 0) rowSums(across(all_of(imm_cols)), na.rm = TRUE) else 0,
    missed_child = if (length(unimm_cols) > 0) rowSums(across(all_of(unimm_cols)), na.rm = TRUE) else 0,
    u5_present = (u5_FM + missed_child)
  )

# Select and prepare final dataset - FIXED THE MONTH FUNCTION HERE
AE <- AD |> 
  select(any_of(c("Country", "states", "lgas", "today", "siatype", "vactype", "vactype_other"))) |>
  rename(Region = states, District = lgas, date = today) |>
  mutate(
    month_num = month(date),
    month = month(date, label = TRUE),  # This works with lubridate
    month_abbr = month(date, label = TRUE, abbr = TRUE)  # This also works
  )

# Add calculated columns
AE$u5_present <- AD$u5_present
AE$u5_FM <- AD$u5_FM
AE$missed_child <- AD$missed_child

# Clean vactype_other if exists
if ("vactype_other" %in% names(AE)) {
  AE$vactype_other <- tolower(AE$vactype_other)
  vactype_mappings <- list(
    "b0pv|bopv" = "bOPV",
    "cmopv2|mopv2|mopv" = "mOPV",
    "fipv.*|fipv\\+nopv2|fipv plus" = "FIPV+nOPV2",
    "n opv|n opv3|nopv2" = "nOPV2",
    "hpv" = "HPV"
  )
  for (pattern in names(vactype_mappings)) {
    AE$vactype_other[grepl(pattern, AE$vactype_other)] <- vactype_mappings[[pattern]]
  }
}

# Assign roundNumber, Response, and Vaccine.type
AF <- AE |> 
  mutate(
    year = year(date),
    Response = case_when(
      year == 2025 & month_abbr == "Jan" ~ "OBR1",
      year == 2025 & month_abbr %in% c("Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug") ~ "NIE-2025-04-01_nOPV_NIDs",
      year == 2025 & month_abbr %in% c("Sep", "Oct", "Nov") ~ "NIE-2025-10-01_nOPV_sNID",
      year == 2024 ~ "NIE-2024-nOPV2",
      year == 2023 & month_abbr %in% c("May", "Jun") ~ "NIE-2023-04-02_nOPV",
      year == 2023 ~ "NIE-2023-07-03_nOPV",
      TRUE ~ as.character(siatype)
    ),
    roundNumber = case_when(
      year == 2025 ~ case_when(
        month_abbr %in% c("Jan", "Feb", "Mar", "Apr", "May") ~ "Rnd1",
        month_abbr %in% c("Jun", "Jul", "Aug") ~ "Rnd2",
        month_abbr %in% c("Sep", "Oct", "Nov") ~ "Rnd3",
        month_abbr == "Dec" ~ "Rnd4"
      ),
      year == 2024 ~ case_when(
        month_abbr %in% c("Feb", "Mar") ~ "Rnd1",
        month_abbr %in% c("Apr", "May", "Jun", "Jul", "Aug") ~ "Rnd2",
        month_abbr %in% c("Sep", "Oct") ~ "Rnd3",
        month_abbr == "Nov" ~ "Rnd4",
        month_abbr == "Dec" ~ "Rnd5"
      ),
      year == 2023 ~ case_when(
        month_abbr %in% c("Jan", "Feb", "Mar", "Apr", "May") ~ "Rnd1",
        month_abbr %in% c("Jun", "Jul", "Aug") ~ "Rnd2",
        month_abbr %in% c("Sep", "Oct") ~ "Rnd3",
        month_abbr == "Nov" ~ "Rnd4",
        month_abbr == "Dec" ~ "Rnd5"
      ),
      TRUE ~ NA_character_
    ),
    Vaccine.type = case_when(
      year >= 2024 ~ "nOPV2",
      year == 2023 & month_abbr %in% c("May", "Jun", "Jul", "Sep") ~ "fIPV+nOPV2",
      year == 2023 ~ "nOPV2",
      year == 2022 & month_abbr == "Jul" ~ "nOPV2",
      year == 2022 ~ "bOPV",
      year == 2021 & month_abbr %in% c("Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct") ~ "nOPV2",
      year == 2021 ~ "bOPV",
      TRUE ~ "nOPV2"
    )
  )

# Apply special district rules
districts_special <- c("YUSUFARI", "GURI", "BIRINIWA", "KIRI KASAMA", "NGURU", "MACHINA", "KARASUWA", "BARDE")
province_special <- c("Adamawa", "Bauchi", "Borno", "Jigawa", "Kano", "Yobe")

AF <- AF |> 
  mutate(Vaccine.type = case_when(
    District %in% districts_special & Response == "NIE-2025-04-01_nOPV_NIDs" ~ "nOPV2 & bOPV",
    Region %in% province_special & Response == "NIE-2025-10-01_nOPV_sNID" ~ "nOPV2 & bOPV",
    TRUE ~ Vaccine.type
  ))

# ============================================================
# AGGREGATION TO DISTRICT LEVEL
# ============================================================

cat("\n📊 Aggregating to district level...\n")

AK <- AF |> 
  filter(!is.na(Region), !is.na(District)) |>
  group_by(Country, Region, District, Response, Vaccine.type, roundNumber) |>
  summarise(
    start_date = min(date, na.rm = TRUE),
    end_date = max(date, na.rm = TRUE),
    year = first(year),
    month = first(month_abbr),
    u5_present = sum(u5_present, na.rm = TRUE),
    u5_FM = sum(u5_FM, na.rm = TRUE),
    missed_child = sum(missed_child, na.rm = TRUE),
    cv = ifelse(u5_present > 0, round(u5_FM / u5_present, 2), NA_real_),
    number_of_records = n(),
    .groups = "drop"
  )

cat("  - Aggregated to", nrow(AK), "district-level records\n")

# ============================================================
# SAVE RESULTS IN MULTIPLE FORMATS
# ============================================================

# Create output directory
output_dir <- "C:/Users/TOURE/Mes documents/REPOSITORIES/IM_raw_data/IM_level/"
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Generate output filename with date and input file info
output_base <- paste0(output_dir, "NIE_IM_7178_", format(Sys.Date(), "%Y%m%d"))

# Save as CSV
output_file_csv <- paste0(output_base, ".csv")
write_csv(AK, output_file_csv)
cat("\n💾 Saved CSV:", output_file_csv)

# Save as RDS
output_file_rds <- paste0(output_base, ".rds")
saveRDS(AK, output_file_rds)
cat("\n💾 Saved RDS:", output_file_rds)

# Save as QS (fast and compressed)
output_file_qs <- paste0(output_base, ".qs")
qs::qsave(AK, output_file_qs)
cat("\n💾 Saved QS:", output_file_qs)

# Save as Parquet (if arrow is available)
output_file_parquet <- paste0(output_base, ".parquet")
tryCatch({
  arrow::write_parquet(AK, output_file_parquet)
  cat("\n💾 Saved Parquet:", output_file_parquet)
}, error = function(e) {
  cat("\n⚠️ Could not save Parquet file:", e$message)
})

# ============================================================
# SUMMARY STATISTICS
# ============================================================

cat("\n\n========================================\n")
cat("✅ PROCESSING COMPLETE\n")
cat("========================================\n")
cat("Input file:", input_file, "\n")
cat("Total aggregated records:", nrow(AK), "\n")
cat("Unique Regions:", length(unique(AK$Region)), "\n")
cat("Unique Districts:", length(unique(AK$District)), "\n")
cat("\n📊 Unique Responses:\n")
for (resp in sort(unique(AK$Response))) {
  count <- nrow(AK[AK$Response == resp,])
  cat("  -", resp, "(", count, "records)\n")
}
cat("\n💉 Unique Vaccine types:\n")
for (vac in sort(unique(AK$Vaccine.type))) {
  count <- nrow(AK[AK$Vaccine.type == vac,])
  cat("  -", vac, "(", count, "records)\n")
}
cat("\n📅 Date range:\n")
if(nrow(AK) > 0) {
  cat("  From:", min(AK$start_date, na.rm = TRUE), "\n")
  cat("  To:", max(AK$end_date, na.rm = TRUE), "\n")
}
cat("\n📁 Output files saved to:", output_dir, "\n")
cat("   Base filename:", output_base, "\n")
cat("========================================\n\n")