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

# LOAD DATA ------------------------------------------------------------------

  df_map <- read_excel("Documents/OrganicaEducação.xlsx") %>% 
    clean_names() %>% 
    select(codigo_ugb:nivel_da_instituicao)

  vec_ugb <- df_map %>% 
    distinct(codigo_ugb) %>% 
    pull()
  
  df <- read_excel("Data/DemonstrativoConsolidadoOrcamentoFuncionamentoPorUGBFuncionalProgramaFRCED_A_Central_20231231_20240106-2024-01-15_12-05-45.500.xls") %>% 
    clean_names() %>% 
    mutate(ugb_id = substr(ugb, 1, 9)) %>% 
    relocate(ugb_id, .before = everything()) %>% 
    filter(ugb_id %in% vec_ugb) %>% 
    glimpse()

  # create a base budget dataframe
  df_orcamento <- df %>% 
    select(ugb_id:despesa_liquidada_via_directa_lvd) %>% # select budget columns
    filter(!is.na(ced), # remove rows where ced is NULL
           !if_all(c(dotacao_inicial, # remove rows where all the following columns are value 0
                     dotacao_revista, 
                     dotacao_revista,
                     dotacao_actualizada_da,
                     dotacao_disponivel,
                     dotacao_cabimentada_dc,
                     ad_fundos_concedidos_af), ~ .x == 0)) %>% 
    mutate(ced_base_2 = str_sub(ced, 1, 2), # create new columns for ced with 2, 3, 4 digits
           ced_base_3 = str_sub(ced, 1, 3),
           ced_base_4 = str_sub(ced, 1, 4),
           dotacao_inicial_texto = as.character(dotacao_inicial), # create column of dotacao_inicial in character format
           id_row_ced_3d = str_c(ugb, funcao, programa, fr, ced_base_3, sep = "_"), # create row id's 
           id_row_ced_4d = str_c(ugb, funcao, programa, fr, ced_base_4, sep = "_"),
           id_row_dot = str_c(ugb, funcao, programa, fr, ced_base_3, dotacao_inicial_texto, sep = "_")
           ) %>% 
    distinct(id_row_dot, .keep_all = TRUE) %>% # remove rows where id_row_dot is duplicate (this is the id concatenate of meta info with dotacao_inicial )
    relocate(starts_with("ced_base"), .after = ced) %>% 
    relocate(starts_with("id_row"), .before = everything()) %>% 
    relocate(dotacao_inicial_texto, .before = dotacao_inicial) %>% 
    select(!dotacao_inicial_texto)

  # create a vector of the columns to subtract
  dot_cols <- df_orcamento %>%
    select(where(is.numeric)) %>%
    names()
  
  # Rows that participate in 4-digit parent/child logic
  rows_4d <- df_orcamento %>%
    group_by(id_row_ced_4d) %>%
    filter(n() > 1) %>%
    pull(id_row_ced_4d)
  
  # Rows that participate in 3-digit parent/child logic
  # but exclude any already used in 4d groups
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
      across(
        all_of(dot_cols),
        ~ ifelse(is_parent, . - sum(. [!is_parent], na.rm = TRUE), .)
      )
    ) %>%
    ungroup() %>%
    select(-is_parent)
  
  
  df_adjust_3d <- df_orcamento %>%
    filter(id_row_ced_3d %in% rows_3d) %>%
    group_by(id_row_ced_3d) %>%
    mutate(
      is_parent = str_ends(ced, "00"),
      across(
        all_of(dot_cols),
        ~ ifelse(is_parent, . - sum(. [!is_parent], na.rm = TRUE), .)
      )
    ) %>%
    ungroup() %>%
    select(-is_parent)
  
  
  df_leaf <- df_orcamento %>%
    filter(
      !(id_row_ced_4d %in% rows_4d),
      !(id_row_ced_3d %in% rows_3d)
    )
  

  df_final <- bind_rows(
    df_adjust_4d,
    df_adjust_3d,
    df_leaf
  )
  

  
  # checks
  df_final_check <- df_final %>% 
    group_by(id_row_ced_4d) %>%
    filter(n() > 1) %>% 
    relocate(id_row_dot, .before = ced)
  
  
  
# for test only
  # t <- df_orcamento %>%
  #   group_by(id_row_ced_4d) %>%
  #   filter(n() > 1) %>%
  #   ungroup() %>% 
  #   arrange(id_row_ced_4d)
  #   #filter(id_row_ced_4d == "50A000141 - MINISTÉRIO DA EDUCAÇÃO E DESENVOLVIMENTO HUMANO_09611 - SERVICOS AUXILIARES DE EDUCACAO_201MAE01060000000OF00  - DESPESAS GERAIS DE FUNCIONAMENTO_101000000000_1210") 
  # # search "50A000141 - MINISTÉRIO DA EDUCAÇÃO E DESENVOLVIMENTO HUMANO_09611 - SERVICOS AUXILIARES DE EDUCACAO_201MAE01060000000OF00  - DESPESAS GERAIS DE FUNCIONAMENTO_101000000000_1210"
  
  

  
  # subset of budget rows where there are duplicate id_row_ced_4d. Child values are subtracted from parent values by grouped id_row_ced_4d
  df_orcamento_adjust_000 <- df_orcamento %>%
    group_by(id_row_ced_4d) %>%
    filter(n() > 1) %>%
    mutate(
      is_parent = str_ends(ced, "000"),
      across(
        all_of(dot_cols),
        ~ ifelse(
          is_parent,
          . - sum(. [!is_parent], na.rm = TRUE),
          .
        )
      )
    ) %>%
    ungroup() %>% 
    select(-is_parent)
  
  
  # subset of budget rows where there are duplicate id_row_ced_3d. Child values are subtracted from parent values by grouped id_row_ced_4d
  df_orcamento_adjust_00 <- df_orcamento %>%
    group_by(id_row_ced_3d) %>%
    filter(n() > 1) %>%
    mutate(
      is_parent = str_ends(ced, "00"),
      across(
        all_of(dot_cols),
        ~ ifelse(
          is_parent,
          . - sum(. [!is_parent], na.rm = TRUE),
          .
        )
      )
    ) %>%
    ungroup() %>% 
    select(-is_parent)
  

 
 

  # need to add in lines that are not duplicated at all
  
  df_orcamento_not_dup <- df_orcamento %>%
    group_by(id_row_ced_3d) %>%
    filter(n() == 1)
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  # create a subset orcamento dataframe for all lines with ced not ending in '00'
  df_orcamento_XX <- df_orcamento %>% 
    filter(!str_detect(ced, "00$")) # keep all!
  
  # vector of 4 digit ced from above
  vec_orca_XX_ced_4d <- df_orcamento_XX %>% 
    distinct(id_row_ced_4d) %>% 
    pull()
  
  # create a subset orcamento dataframe for all lines with ced ending in '00'
  df_orcamento_00 <- df_orcamento %>% 
    filter(str_detect(ced, "[1-9]00$")) # keep only those that whose ced_base_3 does not appear in the 
  
  # vector of 3 digit ced from above USE THE ID_ROW_CED_3D TO FILTER FROM 000 DATAFRAME
  vec_orca_00_ced_3d <- df_orcamento_00 %>% 
    distinct(id_row_ced_3d) %>% 
    pull()
  
  

  
  # create a subset orcamento dataframe for all lines with ced ending in '000'
  df_orcamento_000 <- df_orcamento %>% 
    filter(str_detect(ced, "[1-9]000$"))
  
  # create a subset orcamento dataframe for all lines with ced ending in '0000'
  df_orcamento_0000 <- df_orcamento %>% 
    filter(str_detect(ced, "[1-9]0000$"))
  
  
  
  

  

  

  
  

  vec_orca_XX_ced_3d <- df_orcamento_XX %>% 
    distinct(ced_base_3) %>% 
    pull()
  
  vec_orca_00_ced_3d <- df_orcamento_00 %>% 
    distinct(ced_base_3) %>% 
    pull()

  vec_orca_000_ced_3d <- df_orcamento_000 %>% 
    distinct(ced_base_3) %>% 
    pull()
  
  vec_orca_0000_ced_3d <- df_orcamento_0000 %>% 
    distinct(ced_base_3) %>% 
    pull()
  
 



df_orcamento_final <- bind_rows(df_orcamento_00, df_orcamento_XX)

  
  df_despesa <- df1 %>% 
    select(ugb_id:ced,
           liq_ad_fundos_via_directa_lafvd) %>% 
    filter(!is.na(ced),
           !str_ends(ced, "00")) %>% 
    mutate(metrica = "liq_ad_fundos_via_directa_lafvd",
           ced_area = str_sub(ced, 1, 4))
    

# MUNGE -------------------------------------------------------------------
  
  df_despesa_join <- df_despesa %>% 
    left_join(df_map_join, join_by(ugb_id == codigo_ugb)) %>% 
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
      ced_area,
      metrica,
      valor = liq_ad_fundos_via_directa_lafvd
    )
  
  
  
  
  
  
  # t <- df_orcamento %>% 
  #   filter(id_row_ced_4d == "50A001641 - INSTITUTO DE LINGUAS_09811 - ENSINO N.E._201MAE01060000000OF00  - DESPESAS GERAIS DE FUNCIONAMENTO_101000000000_1111") %>% 
  #   group_by(parent = str_sub(ced, 1, 3)) %>%   # 111xxx all share prefix "111"
  #   mutate(
  #     is_parent = ced == paste0(parent, "100"),  # parent ends with "100"
  #     parent_value = dotacao_inicial[is_parent],
  #     children_sum = sum(dotacao_inicial[!is_parent], na.rm = TRUE),
  #     net_value = parent_value - children_sum
  #   ) %>%
  #   ungroup()
  