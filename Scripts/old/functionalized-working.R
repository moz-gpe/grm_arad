# Working single file
process_budget_file <- function(path, vec_ugb) {
  
  # ---- Load & Prepare ----
  df <- readxl::read_excel(path) %>% 
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
  
  # Stage 1: D groups that have C
  df_dc_flag_c <- df %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::summarise(has_C = any(ced_group == "C"), .groups = "drop")
  
  vec_dc_keep_c <- df_dc_flag_c %>% dplyr::filter(has_C) %>% dplyr::pull(id_row_ced_2d)
  vec_dc_no_c   <- df_dc_flag_c %>% dplyr::filter(!has_C) %>% dplyr::pull(id_row_ced_2d)
  
  # Dataset for normal D/C subtraction
  df_dc_with_c <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_dc_keep_c, ced_group %in% c("C", "D"))
  
  df_dc2 <- df_dc_with_c %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::mutate(across(
      .cols = all_of(num_cols),
      .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
    )) %>%
    dplyr::ungroup()
  
  # Stage 2: fallback B
  df_db_flag_b <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_dc_no_c) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::summarise(has_B = any(ced_group == "B"), .groups = "drop")
  
  vec_db_keep_b <- df_db_flag_b %>% dplyr::filter(has_B) %>% dplyr::pull(id_row_ced_2d)
  vec_db_no_b   <- df_db_flag_b %>% dplyr::filter(!has_B) %>% dplyr::pull(id_row_ced_2d)
  
  df_db_with_b <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_db_keep_b, ced_group %in% c("B", "D"))
  
  df_db2 <- df_db_with_b %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::mutate(across(
      .cols = all_of(num_cols),
      .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
    )) %>%
    dplyr::ungroup()
  
  # Stage 3: fallback A
  df_da_fallback <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_db_no_b, ced_group %in% c("A", "D"))
  
  df_da2 <- df_da_fallback %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::mutate(across(
      .cols = all_of(num_cols),
      .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
    )) %>%
    dplyr::ungroup()
  
  df_dc_final <- dplyr::bind_rows(df_dc2, df_db2, df_da2)
  
  
  # ============================================================
  # ----------------------- C LEVEL -----------------------------
  # ============================================================
  
  df_cb_flag_b <- df %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::summarise(has_B = any(ced_group == "B"), .groups = "drop")
  
  vec_cb_keep_b <- df_cb_flag_b %>% dplyr::filter(has_B) %>% pull(id_row_ced_3d)
  vec_cb_no_b   <- df_cb_flag_b %>% dplyr::filter(!has_B) %>% pull(id_row_ced_3d)
  
  df_cb_with_b <- df %>%
    dplyr::filter(
      id_row_ced_3d %in% vec_cb_keep_b,
      ced_group %in% c("B", "C")
    )
  
  df_ca_fallback <- df %>%
    dplyr::filter(
      id_row_ced_3d %in% vec_cb_no_b,
      ced_group %in% c("A", "C")
    )
  
  df_cb2 <- df_cb_with_b %>%
    dplyr::group_by(id_row_ced_4d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "000")) %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(across(
      .cols = all_of(num_cols),
      .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
    )) %>%
    dplyr::ungroup()
  
  df_ca2 <- df_ca_fallback %>%
    dplyr::group_by(id_row_ced_4d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "00")) %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(across(
      .cols = all_of(num_cols),
      .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
    )) %>%
    dplyr::ungroup()
  
  df_cb_final <- dplyr::bind_rows(df_cb2, df_ca2)
  
  
  # ============================================================
  # ----------------------- B LEVEL -----------------------------
  # ============================================================
  
  df_ba_flag_a <- df %>%
    dplyr::group_by(id_row_ced_4d) %>%
    dplyr::summarise(has_A = any(ced_group == "A"), .groups = "drop")
  
  vec_ba_keep_a <- df_ba_flag_a %>% dplyr::filter(has_A) %>% dplyr::pull(id_row_ced_4d)
  
  df_ba <- df %>%
    dplyr::filter(
      id_row_ced_4d %in% vec_ba_keep_a,
      ced_group %in% c("A", "B")
    )
  
  df_ba_final <- df_ba %>%
    dplyr::group_by(id_row_ced_5d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "00")) %>%
    dplyr::group_by(id_row_ced_4d) %>%
    dplyr::mutate(across(
      .cols = all_of(num_cols),
      .fns  = ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
    )) %>%
    dplyr::ungroup()
  
  
  # ============================================================
  # ----------------------- A LEVEL -----------------------------
  # ============================================================
  
  used_ids <- unique(c(
    vec_dc_keep_c,
    vec_dc_no_c,
    vec_db_keep_b,
    vec_db_no_b,
    vec_cb_keep_b,
    vec_cb_no_b,
    vec_ba_keep_a
  ))
  
  df_a_final <- df %>%
    dplyr::filter(
      ced_group == "A",
      !(id_row_ced_2d %in% used_ids)
    ) %>%
    dplyr::mutate(is_parent = NA)
  
  
  # ============================================================
  # ----------------------- FINAL MERGE -------------------------
  # ============================================================
  
  df_dc_final <- df_dc_final %>% dplyr::mutate(level_priority = 4)
  df_cb_final <- df_cb_final %>% dplyr::mutate(level_priority = 3)
  df_ba_final <- df_ba_final %>% dplyr::mutate(level_priority = 2)
  df_a_final  <- df_a_final  %>% dplyr::mutate(level_priority = 1)
  
  df_final <- dplyr::bind_rows(
    df_dc_final,
    df_cb_final,
    df_ba_final,
    df_a_final
  ) %>%
    dplyr::arrange(id_row_ced_5d, level_priority) %>%
    dplyr::distinct(id_row_ced_5d, .keep_all = TRUE) %>%
    dplyr::select(-level_priority)
  
  return(df_final)
  
}


df_all <-  process_budget_file(path = paths,
                               vec_ugb = vec_ugb)


# works but without file meta info
process_multiple_budget_files <- function(paths, vec_ugb) {
  
  # Safety wrapper: if a file fails, returns empty tibble
  safe_process <- purrr::possibly(
    .f = ~{
      df <- process_budget_file(.x, vec_ugb)
      df$source_file <- basename(.x)
      df
    },
    otherwise = dplyr::tibble()   # empty if error
  )
  
  # Progress bar
  pb <- progress::progress_bar$new(
    total = length(paths),
    format = "  Processing files [:bar] :percent (:current/:total) eta::eta"
  )
  
  # Apply to all files with progress
  results <- purrr::map_df(paths, function(p) {
    pb$tick()
    safe_process(p)
  })
  
  return(results)
}

paths <- list.files(
  path = "Data/",
  pattern = "\\.xls$",
  full.names = TRUE,
  ignore.case = TRUE
)

df_all <- process_multiple_budget_files(paths, vec_ugb)




# working with meta data PRODUCTION?
process_multiple_budget_files <- function(paths, vec_ugb) {
  
  safe_process <- purrr::possibly(
    .f = ~{
      
      # Extract metadata correctly
      meta <- extract_file_info(basename(.x))
      
      # Run main processing
      df <- process_budget_file(.x, vec_ugb)
      
      # Attach metadata at the end
      df <- df %>%
        dplyr::mutate(
          source_file   = basename(.x),
          fonte_reporte = meta$file_name,
          reporte_tipo  = meta$reporte_tipo,
          data_reporte  = meta$data_reporte,
          data_extraido = meta$data_extraido,
          ano           = meta$ano,
          mes           = meta$mes
        )
      
      return(df)
    },
    otherwise = dplyr::tibble()
  )
  
  pb <- progress::progress_bar$new(
    total = length(paths),
    format = "  Processing files [:bar] :percent (:current/:total) eta::eta"
  )
  
  results <- purrr::map_df(paths, function(p) {
    pb$tick()
    safe_process(p)
  })
  
  return(results)
}

paths <- list.files("Data", pattern = "\\.xls$", full.names = TRUE)

df_all <- process_multiple_budget_files(paths, vec_ugb)

df_all_final <- df_all %>% 
  left_join(lookup_list$ugb, by = c("ugb_id" = "codigo_ugb")) %>%
  left_join(lookup_list$ced, by = "ced") %>% 
  relocate(ced_desc, .after = ced) %>% 
  mutate(across(
    c(provincia, distrito),
    ~ ifelse(.x == "-", "Central", .x)
  )) %>%
  select(
    ends_with("_class"),
    fonte_reporte,
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


write_csv(df_all_final,
          "Dataout/full_budget_processed.csv",
          na = "")
