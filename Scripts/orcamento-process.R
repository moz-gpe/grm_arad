# PROJECT:  Let's automate munging of the GRM budget!
# AUTHOR:   J. Lara
# PURPOSE:  
# REF ID:   2833cef5 
# LICENSE:  MIT
# DATE:     2025-11-19
# UPDATED: 

# DEPENDENCIES ------------------------------------------------------------
  
  library(tidyverse)
  library(janitor)
  #library(easystafe)
  library(readxl)
  library(writexl)
  library(arrow)
  source("Scripts/utilities3.R")


# GLOBAL VARIABLES --------------------------------------------------------
  
  ref_id <- "2833cef5"
  
  paths <- list.files(
    path = "Data/2025DecNew/",
    pattern = "\\.xls$",
    full.names = TRUE,
    ignore.case = TRUE
  )
  
  #bucket <- s3_bucket("giz-mec-data-joe", region = "eu-north-1")
  
# LOAD LOOKUP TABLES -------------------------------------------------------
  
  # Load lookup table for education UGB and other metadata
  dim_ugb <- read_excel("Documents/OrganicaEducação_20260211.xlsx",
                        sheet = "Sheet1") %>% 
    clean_names() %>% 
    select(codigo_ugb:nivel_da_instituicao) %>% 
    mutate(distrito = str_to_title(distrito))
  
  # Create a vector of valid UGB IDs to filter the main budget file
  vec_ugb <- dim_ugb %>% 
    distinct(codigo_ugb) %>% 
    pull()
  
  # Load lookup table for expense type
  dim_ced <- read_excel(
    "Documents/OrganicaEducação_20260211.xlsx",
    sheet = "ReferenciaNomes",
    skip = 1,
    col_types = "text"
  ) %>% 
    clean_names() %>%
    select(
      ced = codigo,
      ced_desc = designacao
    ) %>% 
    filter(!is.na(ced)) %>% 
    mutate(
      ced = str_replace(ced, "\\.0+$", "")   # strip trailing .0 etc
    )

  
  # Load lookup table for expense type
  # dim_ced <- read_excel(
  #   "Documents/OrganicaEducação.xlsx",
  #   sheet = "ced_desc",
  #   col_types = "text"
  # ) %>% 
  #   clean_names() %>%
  #   mutate(
  #     ced = str_replace(ced, "\\.0+$", "")   # strip trailing .0 etc
  #   )
  
  # Load lookup table for budget source
  # dim_fr <- read_excel(
  #   "Documents/OrganicaEducação.xlsx",
  #   sheet = "fr_desc",
  #   col_types = "text"
  # ) %>%
  #   clean_names()
  
  # Load lookup table for expense type
  dim_fr <- read_excel(
    "Documents/OrganicaEducação_20260211.xlsx",
    sheet = "ReferenciaNomes",
    skip = 1,
    col_types = "text"
  ) %>% 
    clean_names() %>%
    select(
      fr = codigo_receita,
      fr_nome = tipo_receita,
      fr_modalidade = financiador
    ) %>% 
    mutate(
      fr = str_replace(fr, "\\.0+$", "")   # strip trailing .0 etc
    ) %>% 
    filter(!is.na(fr))
  
  lookup_list <- list(
    ugb = dim_ugb,
    ced = dim_ced,
    fr  = dim_fr
  )
  
  rm(dim_ced, dim_fr, dim_ugb)
  

# MUNGE -------------------------------------------------------------------
  
  
  ##### add district, province, and institution in dataset
  ##### add ced 3/4
  
  df <- processar_esistafe_extracto(paths, vec_ugb)

  df_final <- df %>% 
    left_join(lookup_list$ugb, by = c("ugb_id" = "codigo_ugb")) %>%
    left_join(lookup_list$ced, by = "ced") %>% 
    left_join(lookup_list$fr, by = "fr") %>% 
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
      ugb,
      funcao,
      programa,
      fr,
      fr_nome,
      fr_modalidade,
      ced,
      ced_desc,
      ced_base_3,
      ced_base_4,
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
    )
  

# WRITE TO DISK -----------------------------------------------------------

  write_csv(
    df_final,
    "Dataout/orcamento-processado.csv",
    na = ""
  )
  
  writexl::write_xlsx(
    df_final,
    "Dataout/orcamento-processado.xlsx"
  )
  
  arrow::write_parquet(
    df_final,
    "Dataout/orcamento-processado.parquet"
  )
  
  write_parquet(df_final, bucket$path("esistafe.parquet"))
  
  
  
  
  
  