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

# GLOBAL VARIABLES --------------------------------------------------------

ref_id <- "f51b1207"

path_fonte <- "Data/DemonstrativoConsolidadoOrcamentoFuncionamentoPorUGBFuncionalProgramaFRCED_A_Central_20231231_20240106-2024-01-15_12-05-45.500.xls"

# 1. Load Mapping Table for UGBs --------------------------------------------

df_map <- read_excel("Documents/OrganicaEducação.xlsx") %>% 
  clean_names() %>% 
  select(codigo_ugb:nivel_da_instituicao)

# Create a vector of valid UGB IDs to filter the main budget file
vec_ugb <- df_map %>% 
  distinct(codigo_ugb) %>% 
  pull()


# 2. Load Raw Budget File and Filter to Recognized UGBs ----------------------

df <- read_excel(path_fonte) %>% 
  clean_names() %>% 
  mutate(
    # Extract 9-digit UGB ID from the long UGB string
    ugb_id = substr(ugb, 1, 9)
  ) %>% 
  relocate(ugb_id, .before = everything()) %>% 
  filter(ugb_id %in% vec_ugb) %>% 
  glimpse()


# 3. Build Base Budget Dataframe --------------------------------------------

df_despesa <- df %>% 
  
  # Keep only budget-related columns
  select(c(ugb_id:ced), 
         despesa_paga_via_directa_dp:liq_ad_fundos_via_directa_lafvd) %>% 
  
  # Remove rows with missing CED code or where ALL dotação values are zero
  filter(
    !is.na(ced),
    !if_all(where(is.numeric), ~ .x == 0)
  ) %>% 
  
  # Create hierarchical CED bases (first 2, 3, and 4 digits)
  mutate(
    ced_base_2 = str_sub(ced, 1, 2),
    ced_base_3 = str_sub(ced, 1, 3),
    ced_base_4 = str_sub(ced, 1, 4),
    
    # Helper column used to identify duplicate rows
    despesa_paga_via_directa_dp_texto = as.character(despesa_paga_via_directa_dp),
    
    # Unique row IDs at 3-digit and 4-digit grouping levels
    id_row_ced_3d = str_c(ugb, funcao, programa, fr, ced_base_3, sep = "_"),
    id_row_ced_4d = str_c(ugb, funcao, programa, fr, ced_base_4, sep = "_"),
    
    # Ultra-unique row ID combining dotação value (for removing exact duplicates)
    id_row_dot = str_c(ugb, funcao, programa, fr, ced_base_3, despesa_paga_via_directa_dp_texto, sep = "_")
  ) %>% 
  
  # Remove duplicates based on the dotação-based identifier
  distinct(id_row_dot, .keep_all = TRUE) %>% 
  
  # Reorganize columns for clarity
  relocate(starts_with("ced_base"), .after = ced) %>% 
  relocate(starts_with("id_row"), .before = everything()) %>% 
  relocate(despesa_paga_via_directa_dp_texto, .before = despesa_paga_via_directa_dp) %>% 
  
  # Drop the helper text version of dotação
  select(!despesa_paga_via_directa_dp)


# 4. Identify Numeric Budget Columns ----------------------------------------

# These are the columns used in the subtraction logic
dot_cols <- df_despesa %>%
  select(where(is.numeric)) %>%
  names()


# 5. Identify Parent–Child Groups (4-digit and 3-digit CEDs) ----------------

# Group where 4-digit CED blocks (XXXX) have more than one row (parent + children)
rows_4d <- df_despesa %>%
  group_by(id_row_ced_4d) %>%
  filter(n() > 1) %>%
  pull(id_row_ced_4d)

# Group where 3-digit CED blocks (XXX) have more than one row,
# but exclude those already handled in 4-digit groups
rows_3d <- df_despesa %>%
  group_by(id_row_ced_3d) %>%
  filter(n() > 1) %>%
  filter(!id_row_ced_4d %in% rows_4d) %>%   # prevents duplication
  pull(id_row_ced_3d)


# 6. Adjust Parent Values (4-digit Parent: ends with "000") ------------------

df_adjust_4d <- df_despesa %>%
  filter(id_row_ced_4d %in% rows_4d) %>%
  group_by(id_row_ced_4d) %>%
  mutate(
    # Parent is identified by a CED ending in "000"
    is_parent = str_ends(ced, "000"),
    
    # Subtract all children values from the parent
    across(
      all_of(dot_cols),
      ~ ifelse(is_parent, . - sum(. [!is_parent], na.rm = TRUE), .)
    )
  ) %>%
  ungroup() %>%
  select(-is_parent)


# 7. Adjust Parent Values (3-digit Parent: ends with "00") -------------------

df_adjust_3d <- df_despesa %>%
  filter(id_row_ced_3d %in% rows_3d) %>%
  group_by(id_row_ced_3d) %>%
  mutate(
    # Parent is identified by a CED ending in "00"
    is_parent = str_ends(ced, "00"),
    
    # Subtract all children values from the parent
    across(
      all_of(dot_cols),
      ~ ifelse(is_parent, . - sum(. [!is_parent], na.rm = TRUE), .)
    )
  ) %>%
  ungroup() %>%
  select(-is_parent)


# 8. All Remaining Rows (not part of any parent-child group) -----------------

df_leaf <- df_despesa %>%
  filter(
    !(id_row_ced_4d %in% rows_4d),
    !(id_row_ced_3d %in% rows_3d)
  )

# 9. Final Budget Table Without Duplicates ----------------------------------

df_final <- bind_rows(df_adjust_4d, df_adjust_3d, df_leaf) %>% 
  pivot_longer(
    cols = where(is.numeric),
    names_to = "metrica",
    values_to = "valor"
  ) %>% 
  left_join(df_map, join_by(ugb_id == codigo_ugb)) %>%
  select(
    ugb_id,
    ugb_nome = nome_ugb,
    funcao,
    programa,
    nivel_da_instituicao,
    provincia,
    distrito,
    fr,
    ced,
    metrica,
    valor
  )


