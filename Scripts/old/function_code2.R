
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

#processes with extract_file_info
collapse_hierarchy <- function(path, groups) {
  
  # ---- 0. Extract metadata from filename ----
  meta <- extract_file_info(path)
  
  # ---- 1. Read and prepare df ----
  df <- read_excel(path) %>%
    clean_names() %>%
    select(!ends_with("percent")) %>%
    mutate(
      across(dotacao_inicial:liq_ad_fundos_via_directa_lafvd, as.numeric),
      ugb_id      = substr(ugb, 1, 9),
      
      # Base CED strings (2, 3, 4, 5 digits)
      ced_base_5  = str_sub(ced, 1, 5),
      id_row_ced_5d = str_c(ugb, funcao, programa, fr, ced_base_5, sep = "_"),
      ced_base_4  = str_sub(ced, 1, 4),
      id_row_ced_4d = str_c(ugb, funcao, programa, fr, ced_base_4, sep = "_"),
      ced_base_3  = str_sub(ced, 1, 3),
      id_row_ced_3d = str_c(ugb, funcao, programa, fr, ced_base_3, sep = "_"),
      ced_base_2  = str_sub(ced, 1, 2),
      id_row_ced_2d = str_c(ugb, funcao, programa, fr, ced_base_2, sep = "_")
    ) %>%
    mutate(
      mec_ugb_class   = ifelse(ugb_id %in% vec_ugb, "Keep", "Remove"),
      ced_blank_class = ifelse(!is.na(ced), "Keep", "Remove"),
      
      # Define hierarchical grouping
      ced_group = case_when(
        !str_ends(ced, "00")                                       ~ "A",
        str_ends(ced, "00")   & !str_ends(ced, "000") & !str_ends(ced, "0000") ~ "B",
        str_ends(ced, "000")  & !str_ends(ced, "0000")             ~ "C",
        str_ends(ced, "0000")                                     ~ "D",
        TRUE ~ NA_character_
      )
    ) %>%
    relocate(ugb_id, .before = everything())
  
  # numeric columns to collapse
  num_cols <- df %>% 
    select(where(is.numeric)) %>%
    names()
  
  # ---- 2. Automatic hierarchy rules ----
  rules <- list(
    "CD" = list(
      suffix       = "0000",
      parent_group = rlang::sym("id_row_ced_3d"),
      child_group  = rlang::sym("id_row_ced_2d")
    ),
    "BC" = list(
      suffix       = "000",
      parent_group = rlang::sym("id_row_ced_4d"),
      child_group  = rlang::sym("id_row_ced_3d")
    ),
    "AB" = list(
      suffix       = "00",
      parent_group = rlang::sym("id_row_ced_5d"),
      child_group  = rlang::sym("id_row_ced_4d")
    )
  )
  
  key <- paste0(groups, collapse = "")
  if (!key %in% names(rules)) {
    stop("Unsupported group combination: use c('C','D'), c('B','C'), or c('A','B').")
  }
  
  settings <- rules[[key]]
  
  # ---- 3. Collapse hierarchy ----
  df_out <- df %>%
    filter(ced_group %in% groups) %>%
    
    # mark parent rows
    group_by(!!settings$parent_group) %>%
    mutate(is_parent = str_ends(ced, settings$suffix)) %>%
    
    # subtract children from parent
    group_by(!!settings$child_group) %>%
    mutate(
      across(
        all_of(num_cols),
        ~ ifelse(
          is_parent,
          .x - sum(.x[!is_parent], na.rm = TRUE),
          .x
        )
      )
    ) %>%
    ungroup()
  
  # ---- 4. Attach metadata from extract_file_info() ----
  df_out <- df_out %>%
    mutate(
      file_name     = meta$file_name,
      reporte_tipo  = meta$reporte_tipo,
      data_reporte  = meta$data_reporte,
      data_extraido = meta$data_extraido,
      ano           = meta$ano,
      mes           = meta$mes
    )
  
  return(df_out)
}
