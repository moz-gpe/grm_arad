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
source("Scripts/function_code2.R")

df_raw <- read_excel("Data/test/DemonstrativoConsolidadoOrcamentoFuncionamentoPorUGBFuncionalProgramaFRCED_Provincial_REP_OGDP_20250831_20250905-2025-09-10_11-56-30.592 1.xlsx")


# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "f51b1207"

paths <- list.files(
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



# 2. Begin budget munge ---------------------------------------------------

# step 1: clean, select, filter, and code for ugb_id
df <- read_excel(paths) %>% 
  clean_names() %>% 
  select(!ends_with("percent")) %>% 
  mutate(
    across(dotacao_inicial:liq_ad_fundos_via_directa_lafvd, as.numeric),
    ugb_id = substr(ugb, 1, 9),
    ced_base_5 = str_sub(ced, 1, 5),
    id_row_ced_5d = str_c(ugb, funcao, programa, fr, ced_base_5, sep = "_"),
    ced_base_4 = str_sub(ced, 1, 4),
    id_row_ced_4d = str_c(ugb, funcao, programa, fr, ced_base_4, sep = "_"),
    ced_base_3 = str_sub(ced, 1, 3),
    id_row_ced_3d = str_c(ugb, funcao, programa, fr, ced_base_3, sep = "_"),
    ced_base_2 = str_sub(ced, 1, 2),
    id_row_ced_2d = str_c(ugb, funcao, programa, fr, ced_base_2, sep = "_")
  ) %>% 
  mutate(mec_ugb_class = ifelse(ugb_id %in% vec_ugb, "Keep", "Remove")) %>% 
  mutate(ced_blank_class = ifelse(!is.na(ced), "Keep", "Remove")) %>% 
  mutate(
    ced_group = case_when(
      !str_ends(ced, "00") ~ "A",
      str_ends(ced, "00") & !str_ends(ced, "000") & !str_ends(ced, "0000") ~ "B",
      str_ends(ced, "000") & !str_ends(ced, "0000") ~ "C",
      str_ends(ced, "0000") ~ "D",
      TRUE ~ NA_character_
    )
  ) %>% 
  mutate(id_row_ced_group = str_c(ugb, funcao, programa, fr, ced_group, sep = "_")) %>% 
  relocate(ugb_id, .before = everything()) %>% 
  filter(ugb_id == "50B105761",
         funcao == "01121 - ADMINISTRACAO FINANCEIRA E FISCAL") %>% 
  filter(!is.na(ced))

num_cols <- df %>% 
  select(where(is.numeric)) %>% 
  names()


# CREATE C-D LEVEL MATH -----------------------------------------------------


df_d <- df %>% 
  filter(
    ced_group %in% c("C", "D")
  ) %>% 
  mutate(ced = trimws(ced)) %>% 
  group_by(id_row_ced_3d) %>% 
  
  # identify parent rows
  mutate(is_parent = str_ends(ced, "0000")) %>% 
  
  # compute child sums
  group_by(id_row_ced_2d) %>%
  mutate(
    across(
      .cols = all_of(num_cols),
      .fns = ~ ifelse(
        is_parent,
        .x - sum(.x[!is_parent], na.rm = TRUE),   # subtract children
        .x                                       # children unchanged
      )
    )
  ) %>%
  ungroup() %>% 
  filter(ced_group == "D")



# CREATE C-B LEVEL MATH ----------------------------------------------------


df_c <- df %>% 
  filter(
    ced_group %in% c("B", "C")
  ) %>% 
  mutate(ced = trimws(ced)) %>% 
  group_by(id_row_ced_4d) %>% 
  
  # identify parent rows
  mutate(is_parent = str_ends(ced, "000")) %>% 
  
  # compute child sums
  group_by(id_row_ced_3d) %>%
  mutate(
    across(
      .cols = all_of(num_cols),
      .fns = ~ ifelse(
        is_parent,
        .x - sum(.x[!is_parent], na.rm = TRUE),   # subtract children
        .x                                       # children unchanged
      )
    )
  ) %>%
  ungroup() %>% 
  filter(ced_group == "C")



# CREATE B-A LEVEL MATH ----------------------------------------------------

