# PROJECT:  Let's automate munging of the GRM budget!
# AUTHOR:   J. Lara
# PURPOSE:  
# REF ID:   f51b1207 
# LICENSE:  MIT
# DATE:     2025-11-13
# UPDATED: 

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(janitor)
library(readxl)
source("Scripts/function_code.R")

# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "f51b1207"

xls_files <- list.files(
  path = "Data/test/",
  pattern = "\\.xlsx$",
  full.names = TRUE
)

# 1. Load Mapping Table for UGBs --------------------------------------------

dim_ugb <- read_excel("Documents/xxx_Orcamento_DB_ficheiro branco.xlsm",
                      sheet = "Organica da Educação") %>% 
  clean_names() %>% 
  select(codigo_ugb:nivel_da_instituicao)

# Create a vector of valid UGB IDs to filter the main budget file
vec_ugb <- dim_ugb %>% 
  distinct(codigo_ugb) %>% 
  pull()

dim_ced <- read_excel(
  "Documents/OrganicaEducação.xlsx",
  sheet = "ced_desc",
  col_types = "text"
) %>% 
  clean_names() %>%
  mutate(
    ced = str_replace(ced, "\\.0+$", "")   # strip trailing .0 etc
  )

dim_fr <- read_excel(
  "Documents/OrganicaEducação.xlsx",
  sheet = "fr_desc",
  col_types = "text"
) %>% 
  clean_names()

lookup_list <- list(
  ugb = dim_ugb,
  ced = dim_ced,
  fr  = dim_fr
)

rm(dim_ced, dim_fr, dim_ugb)

# 2. Process Source Data and Union --------------------------------------------


df_all_orcamento <- xls_files %>%
  set_names() %>%
  map(~ build_dotacao_table(
    path_fonte = .x, 
    df_map = lookup_list$ugb),
    .progress = TRUE) %>% 
  bind_rows(.id = "source_file")


df_all_despesa <- xls_files %>% 
  set_names() %>% 
  map(~ build_despesa_table(
    path_fonte = .x, 
    df_map = lookup_list$ugb),
    .progress = TRUE) %>% 
  bind_rows(.id = "source_file")


df_final <- bind_rows(df_all_orcamento, df_all_despesa) %>% 
  left_join(lookup_list$ugb, by = c("ugb_id" = "codigo_ugb")) %>%
  left_join(lookup_list$ced, by = "ced") %>% 
  #left_join(lookup_list$fr,  by = "fr")
  relocate(ced_desc, .after = ced) %>% 
  #relocate(df_fr_desc, .after = fr) %>% 
  mutate(across(
    c(provincia, distrito),
    ~ ifelse(.x == "-", "Central", .x)
  )) %>% 
  select(
    ficheiro_fonte = source_file,
    adm2020_24,
    adm2025_29,
    reporte_tipo,
    starts_with("data_"),
    ano,
    mes,
    ugb_id,
    funcao,
    programa,
    fr,
    starts_with("ced"),
    nivel_da_instituicao,
    ambito,
    provincia,
    distrito,
    metrica,
    valor
    
  ) %>% 
  glimpse()



df_final %>% 
  distinct(data_reporte, data_extraido, reporte_tipo)


df_final %>% 
  distinct(provincia)

# 3. Write Processed Data to Disk --------------------------------------------

write_csv(
  df_final,
  "Dataout/orcamento_final_test.csv",
  na = ""
)
