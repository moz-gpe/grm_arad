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
  mutate(
    id_row_ced_group = str_c(ugb, funcao, programa, fr, ced, ced_group, sep = "_")
  ) %>% 
  relocate(ugb_id, .before = everything()) %>% 
  # filter(ugb_id == "50B105761",
  #        funcao == "01121 - ADMINISTRACAO FINANCEIRA E FISCAL") %>% 
  #filter(!is.na(ced))

num_cols <- df %>% 
  select(where(is.numeric)) %>% 
  names()



# D dataframe-working ----------------------------------------------------------

###########################################
# Stage 1: Identify D-groups that contain C
###########################################
df_dc_flag_c <- df %>%
  group_by(id_row_ced_2d) %>%      # C-level grouping for D
  summarise(has_C = any(ced_group == "C"), .groups = "drop")

# Groups with C → normal D/C subtraction
vec_dc_keep_c <- df_dc_flag_c %>%
  filter(has_C) %>%
  pull(id_row_ced_2d)

# Groups without C → need fallback (B or A)
vec_dc_no_c <- df_dc_flag_c %>%
  filter(!has_C) %>%
  pull(id_row_ced_2d)


###########################################
# Dataset for normal D/C subtraction
###########################################
df_dc_with_c <- df %>%
  filter(
    id_row_ced_2d %in% vec_dc_keep_c,
    ced_group %in% c("C", "D")
  )


###########################################
# D/C parent–child collapse
# Parent group → id_row_ced_3d  (D-level)
# Child group  → id_row_ced_2d  (C-level)
# Parent rows end with "0000"
###########################################
df_dc2 <- df_dc_with_c %>%
  group_by(id_row_ced_3d) %>%
  mutate(is_parent = str_ends(ced, "0000")) %>%
  group_by(id_row_ced_2d) %>%
  mutate(
    across(
      .cols = all_of(num_cols),
      .fns = ~ ifelse(
        is_parent,
        .x - sum(.x[!is_parent], na.rm = TRUE),
        .x
      )
    )
  ) %>%
  ungroup()

###########################################
# Stage 2: Detect B children for D-groups
# Only for groups that did NOT have C
###########################################
df_db_flag_b <- df %>%
  filter(id_row_ced_2d %in% vec_dc_no_c) %>%  # only C-missing groups
  group_by(id_row_ced_2d) %>%
  summarise(has_B = any(ced_group == "B"), .groups = "drop")

# Groups where D has B (fallback level 1)
vec_db_keep_b <- df_db_flag_b %>%
  filter(has_B) %>%
  pull(id_row_ced_2d)

# Groups where D has neither C nor B → fallback to A
vec_db_no_b <- df_db_flag_b %>%
  filter(!has_B) %>%
  pull(id_row_ced_2d)


###########################################
# Dataset for fallback D/B subtraction
###########################################
df_db_with_b <- df %>%
  filter(
    id_row_ced_2d %in% vec_db_keep_b,
    ced_group %in% c("B", "D")
  )

###########################################
# D/B parent–child collapse
# Parent group → id_row_ced_3d  (D-level)
# Child group  → id_row_ced_2d  (B-level)
# Parent rows end with "0000"
###########################################
df_db2 <- df_db_with_b %>%
  group_by(id_row_ced_3d) %>%
  mutate(is_parent = str_ends(ced, "0000")) %>%
  group_by(id_row_ced_2d) %>%
  mutate(
    across(
      .cols = all_of(num_cols),
      .fns = ~ ifelse(
        is_parent,
        .x - sum(.x[!is_parent], na.rm = TRUE),
        .x
      )
    )
  ) %>%
  ungroup()

###########################################
# Stage 3: Dataset for final fallback D/A subtraction
###########################################
df_da_fallback <- df %>%
  filter(
    id_row_ced_2d %in% vec_db_no_b,
    ced_group %in% c("A", "D")
  )

###########################################
# D/A parent–child collapse
# Parent group → id_row_ced_3d  (D-level)
# Child group  → id_row_ced_2d  (A-level)
# Parent rows end with "0000"
###########################################
df_da2 <- df_da_fallback %>%
  group_by(id_row_ced_3d) %>%
  mutate(is_parent = str_ends(ced, "0000")) %>%
  group_by(id_row_ced_2d) %>%
  mutate(
    across(
      .cols = all_of(num_cols),
      .fns = ~ ifelse(
        is_parent,
        .x - sum(.x[!is_parent], na.rm = TRUE),
        .x
      )
    )
  ) %>%
  ungroup()

###########################################
# Stage 4: Combine:
#   - df_dc2 → D/C collapse
#   - df_db2 → D/B fallback collapse
#   - df_da2 → D/A fallback collapse
###########################################
df_dc_final <- bind_rows(df_dc2, df_db2, df_da2)
rm(df_dc2, df_dc_with_c, df_dc_flag_c, df_db2, df_db_with_b, df_db_flag_b, df_da2, df_da_fallback)



# C dataframe-working -------------------------------------------------------------


###########################################
# 1. Identify which id_row_ced_3d groups 
#    have at least one B-level record
###########################################
df_cb_flag_b <- df %>%
  group_by(id_row_ced_3d) %>%
  summarise(has_B = any(ced_group == "B"), .groups = "drop")


# vec_cb_keep_b  = groups where B exists (normal B/C collapse)
# vec_cb_no_b    = groups where B does NOT exist (fallback to A)
vec_cb_keep_b <- df_cb_flag_b %>%
  filter(has_B) %>%
  pull(id_row_ced_3d)

# (A) For groups where B exists, keep only B and C rows
vec_cb_no_b <- df_cb_flag_b %>%
  filter(!has_B) %>%
  pull(id_row_ced_3d)

###########################################
# 2. Split dataset into TWO subsets:
#
#   (A) Groups with B → will apply C − B subtraction
#   (B) Groups without B → fallback to C − A subtraction
###########################################

# (B) For groups where B does NOT exist, keep A and C rows
df_cb_with_b <- df %>%
  filter(
    id_row_ced_3d %in% vec_cb_keep_b,
    ced_group %in% c("B", "C")
  )

# (B) For groups where B does NOT exist, keep A and C rows
df_ca_fallback <- df %>%
  filter(
    id_row_ced_3d %in% vec_cb_no_b,
    ced_group %in% c("A", "C")
  )

###########################################
# 3. Perform B/C parent–child subtraction
#
# Parent group: id_row_ced_4d 
#   → identifies B-level parent row
#
# Child group:  id_row_ced_3d
#   → contains C-level children rows
#
# Parent rows end with "000"
###########################################

df_cb2 <- df_cb_with_b %>%
  group_by(id_row_ced_4d) %>%   # B/C parent level
  mutate(is_parent = str_ends(ced, "000")) %>%
  group_by(id_row_ced_3d) %>%   # B/C child level
  mutate(
    across(
      .cols = all_of(num_cols),
      .fns = ~ ifelse(
        is_parent,
        .x - sum(.x[!is_parent], na.rm = TRUE),
        .x
      )
    )
  ) %>%
  ungroup()


###########################################
# 4. Perform fallback C/A parent–child subtraction
#
# This happens ONLY for groups where B does not exist.
#
# Parent group: id_row_ced_4d 
# Child group:  id_row_ced_3d
#
# Parent rows (A-level) end with "00"
###########################################

df_ca2 <- df_ca_fallback %>%
  group_by(id_row_ced_4d) %>%   # parent level for A/C
  mutate(is_parent = str_ends(ced, "00")) %>%
  group_by(id_row_ced_3d) %>%   # child level for A/C
  mutate(
    across(
      .cols = all_of(num_cols),
      .fns = ~ ifelse(
        is_parent,
        .x - sum(.x[!is_parent], na.rm = TRUE),
        .x
      )
    )
  ) %>%
  ungroup()

###########################################
# 5. Recombine the two processed subsets:
#
#   - df_cb2 → C − B collapse 
#   - df_ca2 → fallback C − A collapse
#
# This produces the full corrected B/C level.
###########################################
df_cb_final <- bind_rows(df_cb2, df_ca2)
rm(df_cb2, df_cb_with_b, df_cb_flag_b, df_ca2, df_ca_fallback)




# B dataframe-working -------------------------------------------------------------
###########################################
# Identify which id_row_ced_4d groups have A
# (A-level records for B parents)
###########################################
df_ba_flag_a <- df %>%
  group_by(id_row_ced_4d) %>%                      # child-level for A
  summarise(has_A = any(ced_group == "A"), 
            .groups = "drop")

# Keep only groups where A is present
vec_ba_keep_a <- df_ba_flag_a %>%
  filter(has_A) %>%
  pull(id_row_ced_4d)

###########################################
# Build dataset containing only B and A rows
# for the groups where A exists
###########################################
df_ba <- df %>%
  filter(
    id_row_ced_4d %in% vec_ba_keep_a,
    ced_group %in% c("A", "B")
  )

###########################################
# Perform B/A parent–child collapse
#
# Parent group → id_row_ced_5d  (B-level)
# Child group  → id_row_ced_4d  (A-level)
# Parent rows end with "00"
###########################################

df_ba_final <- df_ba %>%
  group_by(id_row_ced_5d) %>%                      # parent-level for B
  mutate(is_parent = str_ends(ced, "00")) %>%      # B-level parent suffix
  group_by(id_row_ced_4d) %>%                      # child-level for A
  mutate(
    across(
      .cols = all_of(num_cols),
      .fns = ~ ifelse(
        is_parent,
        .x - sum(.x[!is_parent], na.rm = TRUE),    # subtract A from B
        .x
      )
    )
  ) %>%
  ungroup()

###########################################
rm(df_ba2, df_ba_flag_a, df_ba)



# A dataframe -------------------------------------------------------------

###########################################
# FINAL STEP:
# Collect all A-level rows that NEVER appeared
# in any parent–child calculation above.
#
# These are A rows that:
#   - were not used in D/C (df_dc2)
#   - not used in D/B fallback (df_db2)
#   - not used in D/A fallback (df_da2)
#   - not used in C/B (df_cb2)
#   - not used in C/A fallback (df_ca2)
#   - not used in B/A (df_ba2)
#
# The groups for these subsets are defined by:
#   vec_dc_keep_c, vec_dc_no_c
#   vec_db_keep_b, vec_db_no_b
#   vec_cb_keep_b, vec_cb_no_b
#   vec_ba_keep_a
#
# Any A row not belonging to any of the above
# groups should be appended as-is.
###########################################

# Combine ALL id_row_ced_2d group IDs used in ANY collapse
used_ids <- unique(c(
  vec_dc_keep_c,
  vec_dc_no_c,
  vec_db_keep_b,
  vec_db_no_b,
  vec_cb_keep_b,
  vec_cb_no_b,
  vec_ba_keep_a
))

###########################################
# Extract remaining A rows
# These A rows do NOT belong to any used hierarchy
###########################################
df_a_final <- df %>%
  filter(
    ced_group == "A",          # Must be A-level rows
    !(id_row_ced_2d %in% used_ids)   # Must NOT belong to any processed group
  ) %>% 
  mutate(is_parent = NA)

###########################################
# df_a_remaining now contains:
#   - Standalone A groups
#   - A groups with no D/C/B parents
#   - A rows that were never reach by fallback logic
# These will be appended UNMODIFIED.
###########################################



# Compile -----------------------------------------------------------------

df_dc_final  <- df_dc_final  %>% mutate(level_priority = 4)  # highest
df_cb_final  <- df_cb_final  %>% mutate(level_priority = 3)
df_ba_final  <- df_ba_final  %>% mutate(level_priority = 2)
df_a_final   <- df_a_final   %>% mutate(level_priority = 1)  # lowest

df_final <- bind_rows(
  df_dc_final,
  df_cb_final,
  df_ba_final,
  df_a_final) %>%
  arrange(ced, level_priority) %>%     # ensure highest priority first
  distinct(ced, .keep_all = TRUE) %>%  # keep best version of each "ced"
  select(-level_priority) %>%         # optional: remove helper column
  left_join(lookup_list$ugb, by = c("ugb_id" = "codigo_ugb")) %>%
  left_join(lookup_list$ced, by = "ced") %>% 
  relocate(ced_desc, .after = ced) %>% 
  mutate(across(
    c(provincia, distrito),
    ~ ifelse(.x == "-", "Central", .x)
  )) %>%
  select(
    ends_with("_class"),
    #ficheiro_fonte = source_file,
    adm2020_24,
    adm2025_29,
    #reporte_tipo,
    #starts_with("data_"),
    #ano,
    #mes,
    ugb_id,
    funcao,
    programa,
    fr,
    ced,
    ced_desc,
    nivel_da_instituicao,
    ambito,
    provincia,
    distrito,
    dotacao_inicial,
    dotacao_revista,
    dotacao_actualizada_da,
    dotacao_disponivel,
    dotacao_cabimentada_dc,
    ad_fundos_concedidos_af,
    despesa_paga_via_directa_dp,
    ad_fundos_desp_paga_vd_afdp,
    ad_fundos_liquidados_laf,
    despesa_liquidada_via_directa_lvd,
    liq_ad_fundos_via_directa_lafvd
  ) %>% 
  glimpse()
  
  
rm(df_dc_final, df_cb_final, df_ba_final, df_a_final)

