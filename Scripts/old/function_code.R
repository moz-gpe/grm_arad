
# NEW WITH FILE TYPE, DATES ---------------------------------------------------------------------

extract_file_info <- function(path) {
  fname <- basename(path)
  
  # ---- Report type classification ----
  if (str_detect(fname, "InvestimentoCompExterna")) {
    report_type <- "Investimento Externo"
  } else if (str_detect(fname, "InvestimentoCompInterna")) {
    report_type <- "Investimento Interno"
  } else if (str_detect(fname, "OrcamentoFuncionamento")) {
    report_type <- "Funcionamento"
  } else {
    report_type <- NA_character_
  }
  
  # ---- Extract dates ----
  dates <- str_extract_all(fname, "\\d{8}")[[1]]
  ref_date     <- dates[1] %||% NA_character_
  extract_date <- dates[2] %||% NA_character_
  
  # Convert to Date
  ref_dt     <- as.Date(ref_date, "%Y%m%d")
  extract_dt <- as.Date(extract_date, "%Y%m%d")
  
  # ---- Portuguese month names ----
  meses_pt <- c(
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
  )
  
  # Extract ano + mes
  ano <- if (!is.na(ref_dt)) year(ref_dt) else NA_integer_
  mes <- if (!is.na(ref_dt)) meses_pt[month(ref_dt)] else NA_character_
  
  tibble(
    file_name = fname,
    reporte_tipo = report_type,
    data_reporte = ref_dt,
    data_extraido = extract_dt,
    ano = ano,
    mes = mes
  )
}

build_dotacao_table <- function(path_fonte, df_map) {
  
  # ---- Filename metadata ----
  meta <- extract_file_info(path_fonte)
  
  # Extract dynamic source_* columns only if present
  meta_source_cols <- meta %>% select(starts_with("source_"))
  
  # ---- Load file ----
  df <- read_excel(path_fonte) %>% 
    clean_names() %>% 
    mutate(
      ugb_id = substr(ugb, 1, 9),
      across(dotacao_inicial:ad_fundos_concedidos_af, as.numeric)
    ) %>% 
    relocate(ugb_id, .before = everything()) %>% 
    filter(ugb_id %in% vec_ugb)
  
  # ---- Base table ----
  df_orcamento <- df %>% 
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
  
  # ---- Numeric cols ----
  dot_cols <- df_orcamento %>% select(where(is.numeric)) %>% names()
  
  # ---- Parent-child groups ----
  rows_4d <- df_orcamento %>% 
    group_by(id_row_ced_4d) %>% 
    filter(n() > 1) %>% 
    pull(id_row_ced_4d)
  
  rows_3d <- df_orcamento %>% 
    group_by(id_row_ced_3d) %>% 
    filter(n() > 1) %>% 
    filter(!id_row_ced_4d %in% rows_4d) %>% 
    pull(id_row_ced_3d)
  
  df_adjust_4d <- df_orcamento %>% 
    filter(id_row_ced_4d %in% rows_4d) %>% 
    group_by(id_row_ced_4d) %>% 
    mutate(
      is_parent = str_ends(ced, "000"),
      across(all_of(dot_cols), ~ ifelse(is_parent, . - sum(. [!is_parent], na.rm = TRUE), .))
    ) %>% 
    ungroup() %>% select(-is_parent)
  
  df_adjust_3d <- df_orcamento %>% 
    filter(id_row_ced_3d %in% rows_3d) %>% 
    group_by(id_row_ced_3d) %>% 
    mutate(
      is_parent = str_ends(ced, "00"),
      across(all_of(dot_cols), ~ ifelse(is_parent, . - sum(. [!is_parent], na.rm = TRUE), .))
    ) %>% 
    ungroup() %>% select(-is_parent)
  
  df_leaf <- df_orcamento %>% 
    filter(!(id_row_ced_4d %in% rows_4d), !(id_row_ced_3d %in% rows_3d))
  
  # ---- Final table ----
  df_final <- bind_rows(df_adjust_4d, df_adjust_3d, df_leaf) %>% 
    pivot_longer(cols = where(is.numeric), names_to = "metrica", values_to = "valor") %>% 
    #left_join(df_map, join_by(ugb_id == codigo_ugb)) %>%
    mutate(
      source_file      = meta$file_name,
      reporte_tipo     = meta$reporte_tipo,
      data_reporte     = meta$data_reporte,
      data_extraido    = meta$data_extraido,
      ano              = meta$ano,
      mes              = meta$mes
    )
  
  # Only bind dynamic source_* columns if they exist
  if (ncol(meta_source_cols) > 0) {
    df_final <- df_final %>% bind_cols(meta_source_cols)
  }
  
  # ---- Final column order ----
  df_final <- df_final %>% 
    select(
      source_file, 
      reporte_tipo, 
      data_reporte, 
      data_extraido, 
      ano, 
      mes,
      ugb_id, 
      #ugb_nome = nome_ugb,
      funcao, 
      programa, 
      #nivel_da_instituicao, 
      #provincia, 
      #distrito, 
      fr,
      ced,
      metrica, 
      valor
    )
  
  return(df_final)
}

build_despesa_table <- function(path_fonte, df_map) {
  
  # ---- 1. Extract metadata ----
  meta <- extract_file_info(path_fonte)
  
  # ---- 2. Load data ----
  df <- read_excel(path_fonte) %>% 
    clean_names() %>% 
      mutate(
        ugb_id = substr(ugb, 1, 9),
        across(despesa_paga_via_directa_dp:liq_ad_fundos_via_directa_lafvd, as.numeric)
      ) %>% 
    relocate(ugb_id, .before = everything()) %>% 
    filter(ugb_id %in% vec_ugb)
  
  # ---- 3. Base DESPESA table ----
  df_despesa <- df %>% 
    select(
      c(ugb_id:ced),
      despesa_paga_via_directa_dp : liq_ad_fundos_via_directa_lafvd
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
  
  # ---- 4. Numeric cols ----
  dot_cols <- df_despesa %>% select(where(is.numeric)) %>% names()
  
  # ---- 5. Identify parent/child ----
  rows_4d <- df_despesa %>% 
    group_by(id_row_ced_4d) %>% 
    filter(n() > 1) %>% 
    pull(id_row_ced_4d)
  
  rows_3d <- df_despesa %>% 
    group_by(id_row_ced_3d) %>% 
    filter(n() > 1) %>% 
    filter(!id_row_ced_4d %in% rows_4d) %>% 
    pull(id_row_ced_3d)
  
  # ---- 6. Adjust 4-digit parents ----
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
  
  # ---- 7. Adjust 3-digit parents ----
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
  
  # ---- 8. Leaf rows ----
  df_leaf <- df_despesa %>% 
    filter(
      !(id_row_ced_4d %in% rows_4d),
      !(id_row_ced_3d %in% rows_3d)
    )
  
  # ---- 9. Merge + reshape ----
  df_final <- bind_rows(df_adjust_4d, df_adjust_3d, df_leaf) %>% 
    pivot_longer(
      cols = where(is.numeric),
      names_to = "metrica",
      values_to = "valor"
    ) %>% 
    # left_join(df_map, join_by(ugb_id == codigo_ugb)) %>% 
    
    # ---- 10. Add metadata (guaranteed to exist) ----
  mutate(
    source_file      = meta$file_name,
    reporte_tipo     = meta$reporte_tipo,
    data_reporte     = meta$data_reporte,
    data_extraido    = meta$data_extraido,
    ano              = meta$ano,
    mes              = meta$mes
  ) %>% 
    
    # ---- 11. Final order ----
  select(
    source_file, 
    reporte_tipo, 
    data_reporte, 
    data_extraido, 
    ano, 
    mes,
    ugb_id, 
    #ugb_nome = nome_ugb,
    funcao, 
    programa, 
    #nivel_da_instituicao, 
    #provincia, 
    #distrito, 
    fr,
    ced,
    metrica, 
    valor
  )
  
  return(df_final)
}








collapse_hierarchy <- function(df, groups, num_cols) {
  
  # Automatically determine suffix and grouping columns
  rules <- list(
    "CD" = list(suffix = "0000",
                parent_group = rlang::sym("id_row_ced_3d"),
                child_group  = rlang::sym("id_row_ced_2d")),
    
    "BC" = list(suffix = "000",
                parent_group = rlang::sym("id_row_ced_4d"),
                child_group  = rlang::sym("id_row_ced_3d")),
    
    "AB" = list(suffix = "00",
                parent_group = rlang::sym("id_row_ced_5d"),
                child_group  = rlang::sym("id_row_ced_4d"))
  )
  
  # Convert groups input to key ("CD", "BC", "AB")
  key <- paste0(groups, collapse = "")
  
  if (!key %in% names(rules)) {
    stop("Unsupported group combination. Use one of: c('C','D'), c('B','C'), c('A','B').")
  }
  
  settings <- rules[[key]]
  
  df %>%
    filter(ced_group %in% groups) %>%
    mutate(ced = trimws(ced)) %>%
    
    # identify parents
    group_by(!!settings$parent_group) %>%
    mutate(is_parent = str_ends(ced, settings$suffix)) %>%
    
    # subtract children
    group_by(!!settings$child_group) %>%
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
}

