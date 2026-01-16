library(tidyverse)
library(janitor)
library(readxl)
library(arrow)

path <- list.files(
  path = "Data/test_round_b",
  pattern = "\\.xlsx$",
  full.names = TRUE,
  ignore.case = TRUE
)

# LOAD LOOKUP TABLES -------------------------------------------------------

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

paths <- list.files(
  path = "Data/test_round_b",
  pattern = "\\.xlsx$",
  full.names = TRUE,
  ignore.case = TRUE
)

# ============================================================
# --------------- LOAD & PREPARE SOURCE DATA------------------
# ============================================================

df <- readxl::read_excel(path,
                         col_type = "text") %>% 
  janitor::clean_names() %>% 
  dplyr::select(!dplyr::ends_with("percent")) %>% 
  dplyr::mutate(
    dplyr::across(dotacao_inicial:liq_ad_fundos_via_directa_lafvd, as.numeric),
    ugb_id = substr(ugb, 1, 9),
    
    # CED base reductions
    ced_base_5 = stringr::str_sub(ced, 1, 5),
    id_row_ced_5d = stringr::str_c(ugb, funcao, programa, fr, ced_base_5, sep = "_"),
    
    ced_base_4 = stringr::str_sub(ced, 1, 4),
    id_row_ced_4d = stringr::str_c(ugb, funcao, programa, fr, ced_base_4, sep = "_"),
    
    ced_base_3 = stringr::str_sub(ced, 1, 3),
    id_row_ced_3d = stringr::str_c(ugb, funcao, programa, fr, ced_base_3, sep = "_"),
    
    ced_base_2 = stringr::str_sub(ced, 1, 2),
    id_row_ced_2d = stringr::str_c(ugb, funcao, programa, fr, ced_base_2, sep = "_")
  ) %>% 
  
  dplyr::mutate(
    mec_ugb_class  = ifelse(ugb_id %in% vec_ugb, "Keep", "Remove"),
    ced_blank_class = ifelse(!is.na(ced), "Keep", "Remove")
    
    # determine CED group for each row
  ) %>% 
  dplyr::mutate(
    ced_group = dplyr::case_when(
      !stringr::str_ends(ced, "00")                                           ~ "A",
      stringr::str_ends(ced, "00") & !stringr::str_ends(ced, "000") &
        !stringr::str_ends(ced, "0000")                                      ~ "B",
      stringr::str_ends(ced, "000") & !stringr::str_ends(ced, "0000")        ~ "C",
      stringr::str_ends(ced, "0000")                                         ~ "D",
      TRUE                                                                   ~ NA_character_
    )
  ) %>% 
  dplyr::mutate(
    id_row_ced_group = stringr::str_c(ugb, funcao, programa, fr, ced, ced_group, sep = "_")
  ) %>% 
  dplyr::relocate(ugb_id, .before = tidyselect::everything())

num_cols <- df %>% 
  dplyr::select(where(is.numeric)) %>% 
  names()

# ============================================================
# ----------------------- D LEVEL -----------------------------
# ============================================================

# Stage 1: Normal C subtraction
df_dc_has_c <- df %>%
  dplyr::group_by(id_row_ced_2d) %>%
  dplyr::summarise(has_C = any(ced_group == "C"), .groups = "drop")

vec_dc_keep_c <- df_dc_has_c %>% dplyr::filter(has_C) %>% dplyr::pull(id_row_ced_2d)

df_dc <- df %>%
  dplyr::filter(id_row_ced_2d %in% vec_dc_keep_c, ced_group %in% c("C", "D")) %>% 
  dplyr::group_by(id_row_ced_3d) %>%
  dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
  dplyr::group_by(id_row_ced_2d) %>%
  dplyr::mutate(across(
    .cols = all_of(num_cols),
    .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
  )) %>%
  dplyr::ungroup()

# Stage 2: Fallback B subtraction
vec_dc_no_c <- df_dc_has_c %>% dplyr::filter(!has_C) %>% dplyr::pull(id_row_ced_2d)

df_db_has_b <- df %>%
  dplyr::filter(id_row_ced_2d %in% vec_dc_no_c) %>%
  dplyr::group_by(id_row_ced_2d) %>%
  dplyr::summarise(has_B = any(ced_group == "B"), .groups = "drop")

vec_db_keep_b <- df_db_has_b %>% dplyr::filter(has_B) %>% dplyr::pull(id_row_ced_2d)

df_db <- df %>%
  dplyr::filter(id_row_ced_2d %in% vec_db_keep_b, ced_group %in% c("B", "D")) %>% 
  dplyr::group_by(id_row_ced_3d) %>%
  dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
  dplyr::group_by(id_row_ced_2d) %>%
  dplyr::mutate(across(
    .cols = all_of(num_cols),
    .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
  )) %>%
  dplyr::ungroup()

# Stage 3: Fallback A subtraction
vec_db_no_b <- df_db_has_b %>% dplyr::filter(!has_B) %>% dplyr::pull(id_row_ced_2d)

df_da_has_a <- df %>%
  dplyr::filter(id_row_ced_2d %in% vec_db_no_b) %>%
  dplyr::group_by(id_row_ced_2d) %>%
  dplyr::summarise(has_A = any(ced_group == "A"), .groups = "drop")

vec_da_keep_a <- df_da_has_a %>% dplyr::filter(has_A) %>% dplyr::pull(id_row_ced_2d)

df_da <- df %>%
  dplyr::filter(id_row_ced_2d %in% vec_da_keep_a, ced_group %in% c("A", "D")) %>% 
  dplyr::group_by(id_row_ced_3d) %>%
  dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
  dplyr::group_by(id_row_ced_2d) %>%
  dplyr::mutate(across(
    .cols = all_of(num_cols),
    .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
  )) %>%
  dplyr::ungroup()

# Stage 4: bind to create final D
df_d_final <- dplyr::bind_rows(df_dc, df_db, df_da)
rm(df_da, df_da_has_a, df_db, df_db_has_b, df_dc, df_dc_has_c)

# ============================================================
# ----------------------- C LEVEL -----------------------------
# ============================================================

# Stage 1: Normal B subtraction
df_cb_has_b <- df %>%
  dplyr::group_by(id_row_ced_3d) %>%
  dplyr::summarise(has_B = any(ced_group == "B"), .groups = "drop")

vec_cb_keep_b <- df_cb_has_b %>% dplyr::filter(has_B) %>% pull(id_row_ced_3d)

df_cb <- df %>%
  dplyr::filter(id_row_ced_3d %in% vec_cb_keep_b, ced_group %in% c("B", "C")) %>% 
  dplyr::group_by(id_row_ced_4d) %>%
  dplyr::mutate(is_parent = stringr::str_ends(ced, "000")) %>%
  dplyr::group_by(id_row_ced_3d) %>%
  dplyr::mutate(across(
    .cols = all_of(num_cols),
    .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
  )) %>%
  dplyr::ungroup()

# Stage 2: Fallback A subtraction
vec_cb_no_b   <- df_cb_has_b %>% dplyr::filter(!has_B) %>% pull(id_row_ced_3d)

df_ca_has_a <- df %>%
  dplyr::filter(id_row_ced_3d %in% vec_cb_no_b) %>%
  dplyr::group_by(id_row_ced_3d) %>%
  dplyr::summarise(has_A = any(ced_group == "A"), .groups = "drop")

vec_ca_keep_a <- df_ca_has_a %>% dplyr::filter(has_A) %>% dplyr::pull(id_row_ced_3d)

df_ca <- df %>%
  dplyr::filter(id_row_ced_3d %in% vec_ca_keep_a, ced_group %in% c("A", "C")) %>% 
  dplyr::group_by(id_row_ced_4d) %>%
  dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
  dplyr::group_by(id_row_ced_3d) %>%
  dplyr::mutate(across(
    .cols = all_of(num_cols),
    .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
  )) %>%
  dplyr::ungroup()

# Stage 3: bind to create final C
df_c_final <- dplyr::bind_rows(df_cb, df_ca)
rm(df_cb, df_cb_has_b, df_ca, df_ca_has_a)

# ============================================================
# ----------------------- B LEVEL -----------------------------
# ============================================================

# Stage 1: Normal A subtraction
df_ba_has_a <- df %>%
  dplyr::group_by(id_row_ced_4d) %>%
  dplyr::summarise(has_A = any(ced_group == "A"), .groups = "drop")

vec_ba_keep_a <- df_ba_has_a %>% dplyr::filter(has_A) %>% pull(id_row_ced_4d)

df_b_final <- df %>%
  dplyr::filter(id_row_ced_4d %in% vec_ba_keep_a, ced_group %in% c("A", "B")) %>% 
  dplyr::group_by(id_row_ced_5d) %>%
  dplyr::mutate(is_parent = stringr::str_ends(ced, "00")) %>%
  dplyr::group_by(id_row_ced_4d) %>%
  dplyr::mutate(across(
    .cols = all_of(num_cols),
    .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
  )) %>%
  dplyr::ungroup()

rm(df_ba_has_a)

# ============================================================
# ----------------------- A LEVEL -----------------------------
# ============================================================

df_a_final <- df %>%
  dplyr::filter(ced_group == "A") %>% 
  dplyr::mutate(is_parent = NA)


# ============================================================
# --------------------- PRIORITIZE -------------------------__
# ============================================================

df_d_final <- df_d_final %>% dplyr::mutate(level_priority = 4)
df_c_final <- df_c_final %>% dplyr::mutate(level_priority = 3)
df_b_final <- df_b_final %>% dplyr::mutate(level_priority = 2)
df_a_final <- df_a_final %>% dplyr::mutate(level_priority = 1)

# ============================================================
# ----------------------- FINALIZE ---------------------------
# ============================================================

df_final <- dplyr::bind_rows(df_d_final, df_c_final, df_b_final, df_a_final) %>% 
  dplyr::arrange(id_row_ced_5d, level_priority) %>%
  dplyr::distinct(id_row_ced_5d, .keep_all = TRUE) %>%
  dplyr::select(-level_priority)
  

