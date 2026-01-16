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


# LOAD --------------------------------------------------------------------

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

# 1. Load Metadata --------------------------------------------

meta <- extract_file_info(path = xls_files)

# Extract dynamic source_* columns only if present
meta_source_cols <- meta %>% select(starts_with("source_"))


# Orcamento Testing -------------------------------------------------------

df <- read_excel(xls_files) %>% 
  clean_names() %>% 
  mutate(
    ugb_id = substr(ugb, 1, 9),
    across(dotacao_inicial:ad_fundos_concedidos_af, as.numeric)
  ) %>% 
  relocate(ugb_id, .before = everything()) %>% 
  filter(ugb_id %in% vec_ugb)

df <- df %>% 
  select(ugb_id:ad_fundos_concedidos_af) %>% 
  filter(
    !is.na(ced),
    !if_all(where(is.numeric), ~ .x == 0)
  ) %>%
  mutate(
    ced_base_2 = str_sub(ced, 1, 2),
    ced_base_3 = str_sub(ced, 1, 3),
    ced_base_4 = str_sub(ced, 1, 4),
    dotacao_inicial_texto = as.character(dotacao_inicial),
    id_row_ced_3d = str_c(ugb, funcao, programa, fr, ced_base_3, sep = "_"),
    id_row_ced_4d = str_c(ugb, funcao, programa, fr, ced_base_4, sep = "_"),
    id_row_dot = str_c(ugb, funcao, programa, fr, ced_base_3, dotacao_inicial_texto, sep = "_")
  ) %>%
  distinct(id_row_dot, .keep_all = TRUE) %>%
  relocate(starts_with("ced_base"), .after = ced) %>%
  relocate(starts_with("id_row"), .before = everything()) %>%
  relocate(dotacao_inicial_texto, .before = dotacao_inicial) %>%
  select(!dotacao_inicial_texto)




# Despesas Testing --------------------------------------------------------

df_base <- read_excel(xls_files) %>% 
  clean_names() %>% 
  mutate(
    ugb_id = substr(ugb, 1, 9),
    across(despesa_paga_via_directa_dp:liq_ad_fundos_via_directa_lafvd, as.numeric)
  )

df <- read_excel(xls_files) %>% 
  clean_names() %>% 
  mutate(
    ugb_id = substr(ugb, 1, 9),
    across(despesa_paga_via_directa_dp:liq_ad_fundos_via_directa_lafvd, as.numeric)
  ) %>% 
  relocate(ugb_id, .before = everything()) %>% 
  filter(ugb_id %in% vec_ugb)

df_despesa <- df %>% 
  select(
    c(ugb_id:ced),
    despesa_paga_via_directa_dp:liq_ad_fundos_via_directa_lafvd
  ) %>% 
  filter(
    !is.na(ced),
    !if_all(where(is.numeric), ~ .x == 0)
  ) %>% 
  mutate(
    ced_base_2 = str_sub(ced, 1, 2),
    ced_base_3 = str_sub(ced, 1, 3),
    ced_base_4 = str_sub(ced, 1, 4),
    
    txt_dp = as.character(despesa_paga_via_directa_dp),
    
    id_row_ced_3d = str_c(ugb, funcao, programa, fr, ced_base_3, sep = "_"),
    id_row_ced_4d = str_c(ugb, funcao, programa, fr, ced_base_4, sep = "_"),
    
    id_row_dot = str_c(ugb, funcao, programa, fr, ced_base_3, txt_dp, sep = "_")
  ) %>% 
  distinct(id_row_dot, .keep_all = TRUE) %>% 
  relocate(starts_with("ced_base"), .after = ced) %>% 
  relocate(starts_with("id_row"), .before = everything()) %>% 
  relocate(txt_dp, .before = despesa_paga_via_directa_dp) %>% 
  select(!txt_dp)

dot_cols <- df_despesa %>% select(where(is.numeric)) %>% names()

# ---
rows_4d <- df_despesa %>% 
  group_by(id_row_ced_4d) %>% 
  filter(n() > 1) %>% 
  pull(id_row_ced_4d)

rows_3d <- df_despesa %>% 
  group_by(id_row_ced_3d) %>% 
  filter(n() > 1) %>% 
  filter(!id_row_ced_4d %in% rows_4d) %>% 
  pull(id_row_ced_3d)

# ---
df_adjust_4d <- df_despesa %>% 
  filter(id_row_ced_4d %in% rows_4d) %>% 
  group_by(id_row_ced_4d) %>% 
  mutate(
    is_parent = str_ends(ced, "000"),
    across(all_of(dot_cols),
           ~ ifelse(is_parent, . - sum(. [!is_parent], na.rm = TRUE), .))
  ) %>% 
  ungroup() %>% 
  select(-is_parent)

# ---
df_adjust_3d <- df_despesa %>% 
  filter(id_row_ced_3d %in% rows_3d) %>% 
  group_by(id_row_ced_3d) %>% 
  mutate(
    is_parent = str_ends(ced, "00"),
    across(all_of(dot_cols),
           ~ ifelse(is_parent, . - sum(. [!is_parent], na.rm = TRUE), .))
  ) %>% 
  ungroup() %>% 
  select(-is_parent)

# ---
df_leaf <- df_despesa %>% 
  filter(
    !(id_row_ced_4d %in% rows_4d),
    !(id_row_ced_3d %in% rows_3d)
  )

# ---
df_final <- bind_rows(df_adjust_4d, df_adjust_3d, df_leaf)


write_csv(
  df,
  "Dataout/testing-simulations/orig.csv",
  ""
)

write_csv(
  df_despesa,
  "Dataout/testing-simulations/proc.csv",
  ""
)
